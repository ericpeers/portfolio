#!/usr/bin/python3

"""
Lint and upsert EODHD ticker JSON files into the portfolio database.

Usage:
    python3 utils/eodhd_import.py lint  utils/eodhd_tickers/eodhd_tickers_*.json
    python3 utils/eodhd_import.py upsert utils/eodhd_tickers/eodhd_tickers_*.json
"""

import gzip
import json
import os
import shutil
import sys

import psycopg2
from dotenv import load_dotenv

# GBOND maps to the existing exchange name in the DB
EXCHANGE_MAP = {
    "GBOND": "BONDS/CASH/TREASURIES",
}

BATCH_SIZE = 1000


def load_json_file(filepath):
    """Load JSON from filepath. If it ends in .gz, decompress to disk first, then remove the decompressed file."""
    if filepath.endswith(".gz"):
        out_path = filepath[:-3]
        with gzip.open(filepath, "rb") as f_in, open(out_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        try:
            with open(out_path) as f:
                return json.load(f)
        finally:
            os.remove(out_path)
    with open(filepath) as f:
        return json.load(f)


def connect_db():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(project_root, ".env"))
    pg_url = os.environ.get("PG_URL")
    if not pg_url:
        print("Error: PG_URL not set in environment or .env", file=sys.stderr)
        sys.exit(1)
    return psycopg2.connect(pg_url)


def fetch_exchanges(cur):
    """Returns dict of UPPER(name) -> id."""
    cur.execute("SELECT id, name FROM dim_exchanges")
    return {row[1].upper(): row[0] for row in cur.fetchall()}


def fetch_types(cur):
    """Returns set of uppercased enum values for ds_type."""
    cur.execute("SELECT unnest(enum_range(NULL::ds_type))")
    return {row[0].upper() for row in cur.fetchall()}


def map_exchange(raw):
    mapped = EXCHANGE_MAP.get(raw.upper(), raw.upper())
    return mapped


def map_type(raw):
    return raw.upper()


def run_lint(files, cur):
    exchanges = fetch_exchanges(cur)
    valid_types = fetch_types(cur)

    seen_exchanges = set()
    seen_types = set()

    for filepath in files:
        entries = load_json_file(filepath)

        for entry in entries:
            ex = map_exchange(entry.get("Exchange", ""))
            if ex not in exchanges and ex not in seen_exchanges:
                country = entry.get("Country", "Unknown")
                print(f"[{filepath}] Missing exchange: {ex} (country: {country})")
                seen_exchanges.add(ex)

            t = map_type(entry.get("Type", ""))
            if t == "MUTUAL FUND":
                t = "FUND"

            if t not in valid_types and t not in seen_types:
                print(f"[{filepath}] Missing type: {t}")
                seen_types.add(t)


def run_upsert(files, conn, cur):
    exchanges = fetch_exchanges(cur)
    valid_types = fetch_types(cur)

    total_inserted = 0
    total_skipped_existing = 0
    total_skipped_bad_type = 0
    total_skipped_long_ticker = 0
    total_skipped_dup = 0

    for filepath in files:
        entries = load_json_file(filepath)

        batch = []
        seen_in_file = set()
        file_skipped = 0
        file_skipped_dup = 0
        file_errors = 0

        for entry in entries:
            raw_exchange = entry.get("Exchange", "")
            ex = map_exchange(raw_exchange)
            t = map_type(entry.get("Type", ""))
            ticker = entry.get("Code", "")
            name = entry.get("Name", "")
            currency = entry.get("Currency", "")
            isin = entry.get("Isin")
            country = entry.get("Country", "Unknown")

            if t == "MUTUAL FUND":
                t = "FUND"

            # Auto-create missing exchange
            if ex not in exchanges:
                cur.execute(
                    "INSERT INTO dim_exchanges (name, country) VALUES (%s, %s) RETURNING id",
                    (ex, country),
                )
                new_id = cur.fetchone()[0]
                conn.commit()
                exchanges[ex] = new_id
                print(f"Created exchange: {ex} (country: {country}, id: {new_id})")

            # Skip bad type
            if t not in valid_types:
                print(f"  Warning: skipping {ticker} — unknown type: {t}")
                total_skipped_bad_type += 1
                file_errors += 1
                continue

            # Skip long ticker
            if len(ticker) > 30:
                print(f"  Skipping long ticker: {ticker} ({len(ticker)} chars)")
                total_skipped_long_ticker += 1
                file_skipped += 1
                continue

            # Truncate name
            name = name[:200]
            # Truncate currency
            currency = currency[:3] if currency else None

            exchange_id = exchanges[ex]
            key = (ticker, exchange_id)
            if key in seen_in_file:
                print(f"  Skipping duplicate in file: {ticker} (exchange: {ex})")
                file_skipped_dup += 1
                continue
            seen_in_file.add(key)

            batch.append((ticker, name, exchange_id, t, currency, isin))

        # Batch insert
        file_inserted = 0
        for i in range(0, len(batch), BATCH_SIZE):
            chunk = batch[i : i + BATCH_SIZE]
            args = ",".join(
                cur.mogrify("(%s,%s,%s,%s::ds_type,%s,%s)", row).decode()
                for row in chunk
            )
            if not args:
                continue
            cur.execute(
                f"INSERT INTO dim_security (ticker, name, exchange, type, currency, isin) "
                f"VALUES {args} "
                f"ON CONFLICT (ticker, exchange) DO NOTHING"
            )
            file_inserted += cur.rowcount
            conn.commit()

        file_total = len(batch)
        file_skipped_existing = file_total - file_inserted
        total_inserted += file_inserted
        total_skipped_existing += file_skipped_existing
        total_skipped_dup += file_skipped_dup

        if file_skipped_existing > 0:
            # Identify which tickers were already in the DB
            batch_tickers = [(row[0], row[2]) for row in batch]  # (ticker, exchange_id)
            cur.execute(
                "SELECT ticker, exchange FROM dim_security WHERE (ticker, exchange) IN %s",
                (tuple(batch_tickers),),
            )
            existing = {(r[0], r[1]) for r in cur.fetchall()}
            for ticker, _, exchange_id, *_ in batch:
                if (ticker, exchange_id) in existing:
                    ex_name = next(
                        (n for n, eid in exchanges.items() if eid == exchange_id), "?"
                    )
#                    print(f"  Already in DB: {ticker} (exchange: {ex_name})")

        print(
            f"[{filepath}] inserted={file_inserted} "
            f"skipped_existing={file_skipped_existing} "
            f"skipped_dup_in_file={file_skipped_dup} "
            f"skipped_long_ticker={file_skipped} "
            f"skipped_bad_type={file_errors}"
        )

    print(f"\nSummary:")
    print(f"  Inserted:              {total_inserted}")
    print(f"  Skipped (existing):    {total_skipped_existing}")
    print(f"  Skipped (dup in file): {total_skipped_dup}")
    print(f"  Skipped (bad type):    {total_skipped_bad_type}")
    print(f"  Skipped (long ticker): {total_skipped_long_ticker}")


def main():
    if len(sys.argv) < 3:
        print("Usage: python3 eodhd_import.py <lint|upsert> <file1.json> [file2.json ...]")
        sys.exit(1)

    mode = sys.argv[1]
    files = sys.argv[2:]

    if mode not in ("lint", "upsert"):
        print(f"Unknown mode: {mode}. Use 'lint' or 'upsert'.", file=sys.stderr)
        sys.exit(1)

    conn = connect_db()
    cur = conn.cursor()

    try:
        if mode == "lint":
            run_lint(files, cur)
        elif mode == "upsert":
            run_upsert(files, conn, cur)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

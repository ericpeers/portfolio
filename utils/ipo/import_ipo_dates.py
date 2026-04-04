#!/usr/bin/env python3
"""
Backfill dim_security.inception from IPO-age.csv for US securities where
inception is currently NULL. Safe to re-run: only updates NULL rows.

Usage:
    python3 import_ipo_dates.py [--dry-run] [--csv PATH] [--fuzzy-threshold F]

Output files (always written, even on dry-run):
  not_found.txt         — all tickers not matched in DB
  not_found_recent.txt  — not_found subset with IPO date in last 10 years
  fuzzy_candidates.txt  — name-based fuzzy match suggestions for not_found rows
"""

import argparse
import csv
import os
import re
import sys
from datetime import date, datetime
from difflib import SequenceMatcher

try:
    import psycopg2
except ImportError:
    sys.exit("psycopg2 not found — run: pip install psycopg2-binary")


SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV = os.path.join(SCRIPT_DIR, "IPO-age.csv")
ENV_FILE    = os.path.join(SCRIPT_DIR, "..", "..", ".env")

NOT_FOUND_FILE        = os.path.join(SCRIPT_DIR, "not_found.txt")
NOT_FOUND_RECENT_FILE = os.path.join(SCRIPT_DIR, "not_found_recent.txt")
FUZZY_FILE            = os.path.join(SCRIPT_DIR, "fuzzy_candidates.txt")

RECENT_YEARS  = 10
DEFAULT_FUZZY = 0.78

# Suffixes stripped during name normalization
_CORP_SUFFIXES = re.compile(
    r'\b(INC|CORP|LTD|CO|LLC|LP|PLC|GROUP|HOLDINGS|HOLDING|COMPANY|'
    r'INTERNATIONAL|INTERNATL|INTL|BANCSHARES|BANCORP|FINANCIAL|'
    r'ACQUISITION|ACQUISITIONS|TECHNOLOGIES|TECHNOLOGY|TECH|SERVICES|'
    r'SOLUTIONS|SYSTEMS|ENTERPRISES|PARTNERS|CAPITAL|INDUSTRIES|INDUSTRY|'
    r'RESOURCES|COMMUNICATIONS|THE)\b\.?',
    re.I,
)


def load_pg_url(env_path: str) -> str:
    with open(env_path) as f:
        for line in f:
            m = re.match(r'^PG_URL=(.+)', line.strip())
            if m:
                return m.group(1).strip("\"'")
    raise RuntimeError(f"PG_URL not found in {env_path}")


def normalize_name(name: str) -> str:
    """Uppercase, strip corp suffixes and punctuation, collapse whitespace."""
    name = name.upper()
    name = _CORP_SUFFIXES.sub(' ', name)
    name = re.sub(r'[^A-Z0-9\s]', ' ', name)
    return ' '.join(name.split())


# Named tuple-like: store (ipo_date, csv_name) per ticker
class IpoRow:
    __slots__ = ('ipo_date', 'name')
    def __init__(self, ipo_date: date, name: str):
        self.ipo_date = ipo_date
        self.name = name


def parse_ipo_csv(csv_path: str) -> tuple[dict[str, IpoRow], int]:
    """
    Returns ({ticker: IpoRow}, skipped_no_ticker_count).
    When a ticker appears more than once, keeps the row with the earliest date.
    """
    results: dict[str, IpoRow] = {}
    skipped_no_ticker = 0
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get('Ticker', '').strip().replace('.', '')
            if not ticker:
                skipped_no_ticker += 1
                continue
            raw_date = row.get('offer date', '').strip()
            if len(raw_date) != 8 or not raw_date.isdigit():
                continue
            try:
                ipo_date = datetime.strptime(raw_date, '%Y%m%d').date()
            except ValueError:
                continue
            name = row.get('IPO name', '').strip()
            if ticker not in results or ipo_date < results[ticker].ipo_date:
                results[ticker] = IpoRow(ipo_date, name)
    return results, skipped_no_ticker


def do_update(cur, dry_run: bool, ticker: str, ipo_date: date) -> None:
    if not dry_run:
        cur.execute("""
            UPDATE dim_security ds
            SET inception = %s
            FROM dim_exchanges de
            WHERE ds.exchange = de.id
              AND de.country = 'USA'
              AND ds.ticker = %s
              AND ds.inception IS NULL
        """, (ipo_date, ticker))


def find_fuzzy_matches(
    not_found: list[tuple[str, str, date]],
    db_names: dict[str, str],
    threshold: float,
) -> list[tuple[float, str, str, date, str, str]]:
    """
    For each not-found (ticker, name, ipo_date), find the best-scoring DB
    security by normalized name similarity.

    Returns list of (score, csv_ticker, csv_name, ipo_date, db_ticker, db_name)
    sorted by score desc, filtered to score >= threshold.
    """
    # Pre-normalise DB names once
    db_norm: list[tuple[str, str, str]] = [
        (db_ticker, db_name, normalize_name(db_name))
        for db_ticker, db_name in db_names.items()
    ]

    results = []
    total = len(not_found)
    for i, (csv_ticker, csv_name, ipo_date) in enumerate(not_found):
        if i % 500 == 0:
            print(f'\r  fuzzy matching {i}/{total}...', end='', flush=True)
        norm_csv = normalize_name(csv_name)
        if not norm_csv:
            continue
        best_score = 0.0
        best_db_ticker = best_db_name = ''
        sm = SequenceMatcher(None, norm_csv, '', autojunk=False)
        for db_ticker, db_name, norm_db in db_norm:
            sm.set_seq2(norm_db)
            score = sm.ratio()
            if score > best_score:
                best_score = score
                best_db_ticker = db_ticker
                best_db_name = db_name
        if best_score >= threshold:
            results.append(
                (best_score, csv_ticker, csv_name, ipo_date,
                 best_db_ticker, best_db_name)
            )
    print(f'\r  fuzzy matching {total}/{total}... done')
    results.sort(reverse=True)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true',
                        help='Print matches without writing to DB')
    parser.add_argument('--csv', default=DEFAULT_CSV, metavar='PATH',
                        help=f'Path to IPO CSV (default: {DEFAULT_CSV})')
    parser.add_argument('--fuzzy', action='store_true',
                        help='Run slow fuzzy name matching on not-found tickers')
    parser.add_argument('--fuzzy-threshold', type=float, default=DEFAULT_FUZZY,
                        metavar='F',
                        help=f'Name-similarity threshold 0–1 (default {DEFAULT_FUZZY})')
    args = parser.parse_args()

    today = date.today()
    recent_cutoff = date(today.year - RECENT_YEARS, today.month, today.day)

    print(f'Reading CSV: {args.csv}')
    ipo_data, skipped_no_ticker = parse_ipo_csv(args.csv)
    print(f'  {len(ipo_data)} unique tickers with valid dates in CSV')
    print(f'  {skipped_no_ticker} rows skipped (no ticker in CSV)')

    pg_url = load_pg_url(ENV_FILE)
    conn = psycopg2.connect(pg_url)
    conn.autocommit = False
    cur = conn.cursor()

    # All US tickers: whether any row has NULL inception, plus the name for fuzzy.
    # db_null is filtered to US exchanges only — sibling matches can't cross
    # to foreign exchanges.
    cur.execute("""
        SELECT ds.ticker,
               bool_or(ds.inception IS NULL) AS any_null,
               min(ds.name) AS name
        FROM dim_security ds
        JOIN dim_exchanges de ON de.id = ds.exchange
        WHERE de.country = 'USA'
        GROUP BY ds.ticker
    """)
    db_all: set[str] = set()
    db_null: set[str] = set()
    db_names: dict[str, str] = {}  # ticker → name (for fuzzy matching)
    for ticker, any_null, name in cur.fetchall():
        db_all.add(ticker)
        db_names[ticker] = name or ''
        if any_null:
            db_null.add(ticker)

    stats = {
        'csv_skipped_no_ticker': skipped_no_ticker,
        'updated':               0,
        'unit_sibling_updated':  0,
        'skipped_has_date':      0,
        'not_found_in_db':       0,
    }
    # (csv_ticker, csv_name, ipo_date) for all truly not-found rows
    not_found: list[tuple[str, str, date]] = []

    for ticker, row in sorted(ipo_data.items()):
        ipo_date = row.ipo_date

        # --- Direct match ---
        if ticker in db_null:
            #if args.dry_run:
            #    print(f'  [direct] {ticker} → {ipo_date}')
            do_update(cur, args.dry_run, ticker, ipo_date)
            stats['updated'] += 1
        elif ticker in db_all:
            stats['skipped_has_date'] += 1

        # --- Unit/warrant sibling expansion ---
        # IPO listings often use "U" (units) or "W" (warrants); strip suffix
        # to find the base company's shares and related securities in the DB.
        # db_null is already US-only, so no cross-exchange false positives.
        if ticker.endswith(('U', 'W')) and len(ticker) >= 4:
            base = ticker[:-1]
            siblings_null = sorted(
                t for t in db_null if t.startswith(base) and t != ticker
            )
            for sib in siblings_null:
                #if args.dry_run:
                #    print(f'  [unit-sibling] {ticker} → {sib} → {ipo_date}')
                do_update(cur, args.dry_run, sib, ipo_date)
                stats['unit_sibling_updated'] += 1

        # --- Not found ---
        if ticker not in db_all:
            has_unit_siblings = (
                ticker.endswith(('U', 'W')) and len(ticker) >= 4
                and any(t.startswith(ticker[:-1]) for t in db_all if t != ticker)
            )
            if not has_unit_siblings:
                stats['not_found_in_db'] += 1
                not_found.append((ticker, row.name, ipo_date))

    if not args.dry_run:
        conn.commit()
        print('Committed.')
    else:
        conn.rollback()
        print('Dry run — no changes written.')

    # --- Fuzzy name matching for not-found rows (opt-in: slow) ---
    if args.fuzzy:
        print(f'\nRunning fuzzy name match (threshold={args.fuzzy_threshold}) '
              f'on {len(not_found)} not-found tickers...')
        fuzzy_hits = find_fuzzy_matches(not_found, db_names, args.fuzzy_threshold)
    else:
        fuzzy_hits = []
        print('\nFuzzy matching skipped (pass --fuzzy to enable)')

    # --- Write output files ---
    # not_found.txt — all, ticker + name + date
    with open(NOT_FOUND_FILE, 'w') as f:
        f.write(f'{"TICKER":<12}  {"IPO DATE"}   IPO NAME\n')
        f.write('-' * 60 + '\n')
        for ticker, name, ipo_date in sorted(not_found, key=lambda r: r[2]):
            f.write(f'{ticker:<12}  {ipo_date}   {name}\n')

    # not_found_recent.txt — IPO date within last 10 years
    recent = [(t, n, d) for t, n, d in not_found if d >= recent_cutoff]
    with open(NOT_FOUND_RECENT_FILE, 'w') as f:
        f.write(f'Not found in DB, IPO date >= {recent_cutoff} '
                f'({len(recent)} tickers)\n')
        f.write(f'{"TICKER":<12}  {"IPO DATE"}   IPO NAME\n')
        f.write('-' * 60 + '\n')
        for ticker, name, ipo_date in sorted(recent, key=lambda r: r[2]):
            f.write(f'{ticker:<12}  {ipo_date}   {name}\n')

    # fuzzy_candidates.txt — name-similarity suggestions
    with open(FUZZY_FILE, 'w') as f:
        f.write(f'Fuzzy name match candidates (threshold={args.fuzzy_threshold}, '
                f'{len(fuzzy_hits)} matches)\n')
        f.write(f'{"SCORE":>6}  {"CSV TICKER":<12}  {"IPO DATE"}  '
                f'{"DB TICKER":<12}  CSV NAME  →  DB NAME\n')
        f.write('-' * 90 + '\n')
        for score, csv_t, csv_name, ipo_date, db_t, db_name in fuzzy_hits:
            f.write(
                f'{score:>6.3f}  {csv_t:<12}  {ipo_date}  '
                f'{db_t:<12}  {csv_name}  →  {db_name}\n'
            )

    print(f'  not_found.txt         → {len(not_found)} entries')
    print(f'  not_found_recent.txt  → {len(recent)} entries (>= {recent_cutoff})')
    print(f'  fuzzy_candidates.txt  → {len(fuzzy_hits)} suggestions')

    total_csv = len(ipo_data) + skipped_no_ticker
    print(f'\nResults (of {total_csv} CSV rows):')
    w = 26
    print(f"  {'csv_skipped_no_ticker':<{w}} {stats['csv_skipped_no_ticker']}")
    print(f"  {'updated (direct)':<{w}} {stats['updated']}")
    print(f"  {'updated (unit sibling)':<{w}} {stats['unit_sibling_updated']}")
    print(f"  {'skipped_has_date':<{w}} {stats['skipped_has_date']}")
    print(f"  {'not_found_in_db':<{w}} {stats['not_found_in_db']}")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()

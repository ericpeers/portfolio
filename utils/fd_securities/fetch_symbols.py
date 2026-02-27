#!/usr/bin/python3
# FD_KEY="your_api_key" python3 fetch_symbols.py [--first-page-only]
#
# Fetches security symbol lists from FinancialData.net and saves them to CSVs
# in the securities/ subdirectory.
#
# FD_KEY is read from the environment, or from the nearest .env file found by
# walking up from this script's directory. os.environ takes precedence.
#
# Output files:
#   securities/fd_stocks.csv
#   securities/fd_intl_stocks.csv
#   securities/fd_etfs.csv
#   securities/fd_otc.csv
#   securities/fd_indexes.csv
#
# CSV columns: Ticker,Name,Exchange,Type,Currency,Country,Isin
# (Currency, Country, Isin left empty; use enrich_company_info.py to populate)

import argparse
import csv
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

BASE_URL = "https://financialdata.net/api/v1"
PAGE_SIZE = 500
OUTPUT_DIR = Path(__file__).parent / "securities"

# Endpoints with pagination (offset-based, 500 records/page)
PAGINATED_ENDPOINTS = [
    {
        "path": "/stock-symbols",
        "name_field": "registrant_name",
        "security_type": "COMMON STOCK",
        "exchange": "FD_US",
        "output_file": "fd_stocks.csv",
    },
    {
        "path": "/international-stock-symbols",
        "name_field": "registrant_name",
        "security_type": "COMMON STOCK",
        "exchange": "FD_INTL",
        "output_file": "fd_intl_stocks.csv",
    },
    {
        "path": "/etf-symbols",
        "name_field": "description",
        "security_type": "ETF",
        "exchange": "FD_ETF",
        "output_file": "fd_etfs.csv",
    },
    {
        "path": "/otc-symbols",
        "name_field": "title_of_security",
        "security_type": "COMMON STOCK",
        "exchange": "FD_OTC",
        "output_file": "fd_otc.csv",
    },
]

# Endpoints that return all records in a single response (no pagination)
SINGLE_PAGE_ENDPOINTS = [
    {
        "path": "/index-symbols",
        "name_field": "index_name",
        "ticker_field": "trading_symbol",
        "security_type": "INDEX",
        "exchange": "FD_IDX",
        "output_file": "fd_indexes.csv",
    },
]


def load_dotenv():
    """Walk up from this script's directory looking for a .env file.
    Sets any key not already present in os.environ."""
    search = Path(__file__).resolve().parent
    while True:
        candidate = search / ".env"
        if candidate.exists():
            with open(candidate) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key and key not in os.environ:
                        os.environ[key] = value
            break
        parent = search.parent
        if parent == search:
            break
        search = parent


def fetch_page(api_key, path, offset):
    url = f"{BASE_URL}{path}?offset={offset}&limit={PAGE_SIZE}&key={api_key}"
    try:
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                print(f"HTTP {response.status} for {path} offset={offset}", file=sys.stderr)
                return []
            data = json.loads(response.read().decode("utf-8"))
            if isinstance(data, list):
                return data
            for key in ("data", "results", "securities", "symbols"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            print(f"Unexpected response structure for {path}: {list(data.keys())}", file=sys.stderr)
            return []
    except urllib.error.URLError as e:
        print(f"Error fetching {path} offset={offset}: {e}", file=sys.stderr)
        return []


def fetch_single_page(api_key, path):
    url = f"{BASE_URL}{path}?key={api_key}"
    try:
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                print(f"HTTP {response.status} for {path}", file=sys.stderr)
                return []
            data = json.loads(response.read().decode("utf-8"))
            if isinstance(data, list):
                return data
            for key in ("data", "results", "securities", "symbols"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            print(f"Unexpected response structure for {path}: {list(data.keys())}", file=sys.stderr)
            return []
    except urllib.error.URLError as e:
        print(f"Error fetching {path}: {e}", file=sys.stderr)
        return []


def fetch_all_records(api_key, path, first_page_only=False):
    all_records = []
    offset = 0
    while True:
        print(f"  Fetching {path} offset={offset}...", end=" ", flush=True)
        page = fetch_page(api_key, path, offset)
        print(f"{len(page)} records")
        all_records.extend(page)
        if len(page) < PAGE_SIZE or first_page_only:
            break
        offset += PAGE_SIZE
    return all_records


def extract_ticker(record, ticker_field=None):
    """Return the ticker value from a record, trying known field names."""
    if ticker_field:
        val = record.get(ticker_field)
        if val:
            return val.strip()
    return (
        record.get("ticker")
        or record.get("symbol")
        or record.get("trading_symbol")
        or record.get("Ticker")
        or ""
    ).strip()


def write_csv(output_path, records, name_field, security_type, exchange, ticker_field=None):
    fieldnames = ["Ticker", "Name", "Exchange", "Type", "Currency", "Country", "Isin"]
    written = 0
    skipped = 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            ticker = extract_ticker(record, ticker_field)
            if not ticker:
                skipped += 1
                continue
            name = record.get(name_field) or record.get("name") or ""
            name = name.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").strip()
            writer.writerow({
                "Ticker": ticker,
                "Name": name,
                "Exchange": exchange,
                "Type": security_type,
                "Currency": "",
                "Country": "",
                "Isin": "",
            })
            written += 1

    return written, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Fetch security symbol lists from FinancialData.net."
    )
    parser.add_argument(
        "--first-page-only",
        action="store_true",
        help="Fetch only the first page (≤500 records) per paginated endpoint. "
             "Useful for testing without a paid subscription.",
    )
    args = parser.parse_args()

    load_dotenv()

    api_key = os.environ.get("FD_KEY")
    if not api_key:
        print("Error: FD_KEY not set in environment or any .env file found upward from this script.", file=sys.stderr)
        sys.exit(1)

    OUTPUT_DIR.mkdir(exist_ok=True)

    if args.first_page_only:
        print("Note: --first-page-only is set; paginated endpoints will stop after the first page.\n")

    # Paginated endpoints
    for endpoint in PAGINATED_ENDPOINTS:
        print(f"Fetching {endpoint['path']} → {endpoint['output_file']}")
        records = fetch_all_records(api_key, endpoint["path"], first_page_only=args.first_page_only)
        print(f"  Total records fetched: {len(records)}")

        output_path = OUTPUT_DIR / endpoint["output_file"]
        written, skipped = write_csv(
            output_path,
            records,
            endpoint["name_field"],
            endpoint["security_type"],
            endpoint["exchange"],
        )
        print(f"  Written: {written}, Skipped (no ticker): {skipped}")
        print(f"  Saved to {output_path}\n")

    # Non-paginated endpoints
    for endpoint in SINGLE_PAGE_ENDPOINTS:
        print(f"Fetching {endpoint['path']} → {endpoint['output_file']} (single-page endpoint)")
        print(f"  Fetching {endpoint['path']}...", end=" ", flush=True)
        records = fetch_single_page(api_key, endpoint["path"])
        print(f"{len(records)} records")

        output_path = OUTPUT_DIR / endpoint["output_file"]
        written, skipped = write_csv(
            output_path,
            records,
            endpoint["name_field"],
            endpoint["security_type"],
            endpoint["exchange"],
            ticker_field=endpoint.get("ticker_field"),
        )
        print(f"  Written: {written}, Skipped (no ticker): {skipped}")
        print(f"  Saved to {output_path}\n")

    print("Done.")


if __name__ == "__main__":
    main()

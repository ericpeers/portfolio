#!/usr/bin/python3
# FD_KEY="your_api_key" python3 fetch_etf_holdings.py [--dry-run] TICKER [TICKER ...]
#
# Fetches ETF constituent holdings from FinancialData.net and saves them to CSVs
# in the etf_holdings/ subdirectory.
#
# Output: etf_holdings/fd_{TICKER}_{YYYYMMDD}.csv
# CSV columns: Symbol,Company,Weight
# Weight is percentage (e.g., 7.83), matching the format expected by
# POST /admin/load_etf_holdings
#
# --dry-run: print raw JSON for the first ETF and exit without writing files

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

BASE_URL = "https://financialdata.net/api/v1"


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
OUTPUT_DIR = Path(__file__).parent / "etf_holdings"
DELAY_SECONDS = 1.0


def fetch_holdings_raw(api_key, ticker):
    # URL-encode the identifier so tickers containing '&' don't corrupt the query string.
    url = f"{BASE_URL}/etf-holdings?identifier={urllib.parse.quote(ticker, safe='')}&key={api_key}"
    try:
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                print(f"HTTP {response.status} for {ticker}", file=sys.stderr)
                return None
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as e:
        print(f"Error fetching holdings for {ticker}: {e}", file=sys.stderr)
        return None


def extract_holdings(raw, ticker):
    """
    Extract a list of {Symbol, Company, Weight} dicts from raw API response.

    FD documentation does not specify field names; we try common variants.
    Weight is normalized to percentage (e.g., 7.83 not 0.0783).
    """
    records = raw
    # Unwrap if response is a dict with a list inside
    if isinstance(raw, dict):
        for key in ("holdings", "data", "results", "constituents"):
            if key in raw and isinstance(raw[key], list):
                records = raw[key]
                break
        else:
            # Single-item dict with unknown structure — warn and return empty
            print(f"  Warning: unexpected response structure for {ticker}: {list(raw.keys())}", file=sys.stderr)
            return []

    holdings = []
    for item in records:
        symbol = (
            item.get("ticker")
            or item.get("symbol")
            or item.get("constituent_ticker")
            or item.get("holding_ticker")
            or ""
        ).strip()

        company = (
            item.get("name")
            or item.get("company")
            or item.get("description")
            or item.get("registrant_name")
            or item.get("constituent_name")
            or ""
        ).strip()

        # Weight may be decimal (0.0783) or percent (7.83)
        weight_raw = (
            item.get("weight")
            or item.get("percentage")
            or item.get("allocation")
            or item.get("percent_of_net_assets")
            or 0
        )
        try:
            weight = float(weight_raw)
        except (TypeError, ValueError):
            weight = 0.0

        # Heuristic: if max weight across all items <= 1.0, assume decimal form
        # We'll handle this after collecting all items
        holdings.append({"Symbol": symbol, "Company": company, "_weight_raw": weight})

    if not holdings:
        return []

    # Detect decimal vs percent: if all weights <= 1.0, treat as decimal
    max_weight = max(h["_weight_raw"] for h in holdings)
    is_decimal = max_weight <= 1.0

    result = []
    for h in holdings:
        w = h["_weight_raw"] * 100.0 if is_decimal else h["_weight_raw"]
        result.append({"Symbol": h["Symbol"], "Company": h["Company"], "Weight": round(w, 4)})

    return result


def save_holdings_csv(ticker, holdings):
    OUTPUT_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    filename = OUTPUT_DIR / f"fd_{ticker}_{date_str}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Symbol", "Company", "Weight"])
        writer.writeheader()
        writer.writerows(holdings)

    print(f"  Saved {len(holdings)} holdings to {filename}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch ETF constituent holdings from FinancialData.net."
    )
    parser.add_argument("tickers", nargs="+", help="One or more ETF tickers.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print raw JSON for the first ETF and exit without writing files.",
    )
    args = parser.parse_args()

    load_dotenv()

    api_key = os.environ.get("FD_KEY")
    if not api_key:
        print("Error: FD_KEY not set in environment or any .env file found upward from this script.", file=sys.stderr)
        sys.exit(1)

    for i, ticker in enumerate(args.tickers):
        print(f"Fetching holdings for {ticker}...")
        raw = fetch_holdings_raw(api_key, ticker)

        if raw is None:
            print(f"  Failed to fetch {ticker}, skipping.")
            continue

        if args.dry_run:
            print(f"\n--- Raw JSON for {ticker} ---")
            print(json.dumps(raw, indent=2))
            print("--- End raw JSON ---")
            print("\n(dry-run mode: no files written)")
            break

        # On first ticker, also print raw JSON for inspection
        if i == 0:
            print(f"  Raw JSON sample (first record): {json.dumps(raw[:1] if isinstance(raw, list) else raw, indent=2)}")

        holdings = extract_holdings(raw, ticker)
        if not holdings:
            print(f"  No holdings extracted for {ticker}.")
            continue

        save_holdings_csv(ticker, holdings)

        if i < len(args.tickers) - 1:
            print(f"  Waiting {DELAY_SECONDS}s before next fetch...")
            time.sleep(DELAY_SECONDS)

    print("\nDone.")


if __name__ == "__main__":
    main()

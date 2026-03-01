#!/usr/bin/python3
# python3 validate_etf_prices.py [--limit N] [--workers N]
#
# For each ETF ticker in fd_etfs_enriched.csv, queries the FinancialData.net
# etf-prices endpoint and categorises the response:
#   - error:   HTTP error status or network failure
#   - empty:   HTTP 200 but body < 100 chars (no real data)
#   - data:    HTTP 200 with actual price data
#
# Reads FD_KEY from .env (walked up from this script's location).
# Prints a running tally and final summary to stdout.

import argparse
import csv
import os
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE_URL = "https://financialdata.net/api/v1"
CSV_PATH = Path(__file__).resolve().parent / "securities" / "fd_etfs_enriched.csv"


def load_dotenv():
    search = Path(__file__).resolve().parent
    while True:
        candidate = search / ".env"
        if candidate.exists():
            with open(candidate) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, val = line.partition("=")
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key and key not in os.environ:
                        os.environ[key] = val
            return
        parent = search.parent
        if parent == search:
            return
        search = parent


def read_tickers(limit=None):
    tickers = []
    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tickers.append(row["Ticker"].strip())
            if limit and len(tickers) >= limit:
                break
    return tickers


def check_ticker(ticker, api_key, retries=3, backoff=2.0):
    url = f"{BASE_URL}/etf-prices?identifier={ticker}&key={api_key}"
    last_err = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                body = resp.read()
                if len(body) < 100:
                    return ticker, "empty", len(body)
                else:
                    return ticker, "data", len(body)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                last_err = e.code
                time.sleep(backoff * (attempt + 1))
                continue
            return ticker, "error", e.code
        except Exception as e:
            last_err = str(e)
            time.sleep(backoff * (attempt + 1))
            continue
    return ticker, "error", last_err


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Check only first N tickers")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent requests (default 10)")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.environ.get("FD_KEY")
    if not api_key:
        print("ERROR: FD_KEY not found in environment or .env", file=sys.stderr)
        sys.exit(1)

    tickers = read_tickers(args.limit)
    total = len(tickers)
    print(f"Checking {total} ETF tickers against FinancialData.net etf-prices endpoint...")
    print(f"Workers: {args.workers}\n")

    counts = {"data": 0, "empty": 0, "error": 0}
    examples = {"data": [], "empty": [], "error": []}
    tickers_with_data = []
    MAX_EXAMPLES = 5
    done = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(check_ticker, t, api_key): t for t in tickers}
        for future in as_completed(futures):
            ticker, category, detail = future.result()
            counts[category] += 1
            if category == "data":
                tickers_with_data.append(ticker)
            if len(examples[category]) < MAX_EXAMPLES:
                examples[category].append((ticker, detail))
            done += 1
            if done % 100 == 0 or done == total:
                pct = done / total * 100
                print(
                    f"  [{done:4d}/{total}] {pct:5.1f}%  "
                    f"data={counts['data']}  empty={counts['empty']}  error={counts['error']}"
                )

    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"  Total checked : {total}")
    print(f"  Has data      : {counts['data']:5d}  ({counts['data']/total*100:.1f}%)")
    print(f"  Empty/no data : {counts['empty']:5d}  ({counts['empty']/total*100:.1f}%)")
    print(f"  Errors        : {counts['error']:5d}  ({counts['error']/total*100:.1f}%)")

    for category, label in [("data", "Has data"), ("empty", "Empty"), ("error", "Error")]:
        if examples[category]:
            print(f"\n  {label} examples:")
            for ticker, detail in examples[category]:
                print(f"    {ticker}: {detail}")

    if tickers_with_data:
        print("\n" + "=" * 60)
        print(f"ETFs WITH PRICE DATA ({len(tickers_with_data)}):")
        print(",".join(sorted(tickers_with_data)))


if __name__ == "__main__":
    main()

#!/usr/bin/python3
# FD_KEY="your_api_key" python3 enrich_company_info.py securities/fd_stocks.csv [--limit N] [--workers N]
#
# Reads a securities CSV from fetch_symbols.py and enriches each row with
# ISIN, exchange, country, and currency from FinancialData.net.
#
# Routing:
#   FD_INTL  → /international-company-information (isin_number, exchange)
#              Exchange resolved via INTL_EXCHANGE_MAP (full name, then ticker suffix).
#              Unrecognized exchange values are flagged as errors.
#   FD_US    → /company-information (isin, trading_exchange); sets Currency=USD, Country=USA
#   FD_OTC   → /company-information; sets Currency=USD, Country=USA, Exchange=OTCMKTS if not found
#
# Requests are issued concurrently (--workers, default 8). Output row order may
# differ from input order. The API allows up to 30 RPS; 8 workers with typical
# ~200-400ms latency stays comfortably within that limit.
#
# Output: same directory as input, with "_enriched" suffix before .csv extension
# Example: securities/fd_stocks.csv → securities/fd_stocks_enriched.csv

import argparse
import csv
import json
import os
import sys
import time
import threading
import urllib.parse
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE_URL = "https://financialdata.net/api/v1"

# Maps FD /international-company-information `exchange` field values
# (full names and ticker suffixes) to (dim_exchanges_name, country, currency).
# See exchange_map.md for the full reference table.
# Lookup order: full API name first, then ticker suffix extracted from Ticker column.
INTL_EXCHANGE_MAP = {
    # --- Full names (what the API returns) ---
    "London Stock Exchange":          ("LSE",  "UK",          "GBP"),
    "Toronto Stock Exchange":         ("TO",   "Canada",      "CAD"),
    "TSX Venture Exchange":           ("V",    "Canada",      "CAD"),
    "Frankfurt Stock Exchange":       ("F",    "Germany",     "EUR"),
    "XETRA":                          ("XETRA","Germany",     "EUR"),
    "Euronext Paris":                 ("PA",   "France",      "EUR"),
    "Euronext Amsterdam":             ("AS",   "Netherlands", "EUR"),
    "Tokyo Stock Exchange":           ("T",    "Japan",       "JPY"),
    "Hong Kong Stock Exchange":       ("HK",   "Hong Kong",   "HKD"),
    "Singapore Exchange":             ("SI",   "Singapore",   "SGD"),
    "Indonesia Stock Exchange":       ("JK",   "Indonesia",   "IDR"),
    "Bursa Malaysia":                 ("KLSE", "Malaysia",    "MYR"),
    "Korea Exchange":                 ("KO",   "Korea",       "KRW"),
    "Korea KOSDAQ":                   ("KQ",   "Korea",       "KRW"),
    "B3 Brasil Bolsa Balcao":         ("SA",   "Brazil",      "BRL"),
    "Bolsa Mexicana de Valores":      ("MX",   "Mexico",      "MXN"),
    "National Stock Exchange India":  ("NS",   "India",       "INR"),
    "Bombay Stock Exchange":          ("BO",   "India",       "INR"),
    "Shanghai Stock Exchange":        ("SHG",  "China",       "CNY"),
    "Shenzhen Stock Exchange":        ("SHE",  "China",       "CNY"),
    # --- Ticker suffixes (fallback) ---
    "L":   ("LSE",  "UK",          "GBP"),
    "TO":  ("TO",   "Canada",      "CAD"),
    "V":   ("V",    "Canada",      "CAD"),
    "F":   ("F",    "Germany",     "EUR"),
    "DE":  ("XETRA","Germany",     "EUR"),
    "PA":  ("PA",   "France",      "EUR"),
    "AS":  ("AS",   "Netherlands", "EUR"),
    "T":   ("T",    "Japan",       "JPY"),
    "HK":  ("HK",   "Hong Kong",   "HKD"),
    "SI":  ("SI",   "Singapore",   "SGD"),
    "JK":  ("JK",   "Indonesia",   "IDR"),
    "KL":  ("KLSE", "Malaysia",    "MYR"),
    "KS":  ("KO",   "Korea",       "KRW"),
    "KQ":  ("KQ",   "Korea",       "KRW"),
    "SA":  ("SA",   "Brazil",      "BRL"),
    "MX":  ("MX",   "Mexico",      "MXN"),
    "NS":  ("NS",   "India",       "INR"),
    "BO":  ("BO",   "India",       "INR"),
    "SS":  ("SHG",  "China",       "CNY"),
    "SZ":  ("SHE",  "China",       "CNY"),
}

# Maps exchange names returned by FD /company-information that don't match
# dim_exchanges entries directly. Applied after receiving api_exchange for
# non-international rows.
# CEP (Crestwood Equity Partners) is a known example that returns "NAS".
DOMESTIC_EXCHANGE_MAP = {
    "NAS": "NASDAQ",
    "OTC": "OTCMKTS",  # avoid creating a duplicate OTC exchange
}


# Adaptive concurrency control. Initialized in main() before workers start.
# _concurrency["max"] decreases by 1 on each 429, floor 1.
_concurrency = {"max": 0, "active": 0}
_concurrency_cond = threading.Condition()


def _acquire_slot():
    with _concurrency_cond:
        while _concurrency["active"] >= _concurrency["max"]:
            _concurrency_cond.wait()
        _concurrency["active"] += 1


def _release_slot():
    with _concurrency_cond:
        _concurrency["active"] -= 1
        _concurrency_cond.notify()


def _on_429(ticker):
    with _concurrency_cond:
        if _concurrency["max"] > 1:
            _concurrency["max"] -= 1
            print(
                f"  429 rate limit hit for {ticker} — reducing concurrency to "
                f"{_concurrency['max']} worker(s)",
                file=sys.stderr,
            )


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


def fetch_info(api_key, ticker, international=False):
    path = "/international-company-information" if international else "/company-information"
    # URL-encode the identifier so tickers like J&KBANK.NS don't corrupt the query string.
    url = f"{BASE_URL}{path}?identifier={urllib.parse.quote(ticker, safe='')}&key={api_key}"
    for attempt in range(4):
        _acquire_slot()
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                if response.status != 200:
                    return None
                data = json.loads(response.read().decode("utf-8"))
                # Handle possible list wrapper
                if isinstance(data, list):
                    return data[0] if data else None
                return data
        except urllib.error.HTTPError as e:
            if e.code == 429:
                _on_429(ticker)
                delay = 2 ** attempt
                print(f"  Retrying {ticker} in {delay}s (429, attempt {attempt + 1}/4)", file=sys.stderr)
                time.sleep(delay)
                continue
            print(f"  Error fetching {ticker}: HTTP {e.code}", file=sys.stderr)
            return None
        except urllib.error.URLError as e:
            print(f"  Error fetching {ticker}: {e}", file=sys.stderr)
            return None
        finally:
            _release_slot()
    print(f"  {ticker}: max retries reached after repeated 429s", file=sys.stderr)
    return None


def fetch_isin_fallback(api_key, ticker):
    """Try /securities-information as a fallback source for ISIN when the primary
    company-information call returns no isin_number.
    Returns the ISIN string, or "" if not found or the call fails."""
    # URL-encode the identifier so tickers like J&KBANK.NS don't corrupt the query string.
    url = f"{BASE_URL}/securities-information?identifier={urllib.parse.quote(ticker, safe='')}&key={api_key}"
    for attempt in range(4):
        _acquire_slot()
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                if response.status != 200:
                    return ""
                data = json.loads(response.read().decode("utf-8"))
                if isinstance(data, list):
                    data = data[0] if data else {}
                return (data.get("isin_number") or "").strip()
        except urllib.error.HTTPError as e:
            if e.code == 429:
                _on_429(ticker)
                time.sleep(2 ** attempt)
                continue
            return ""
        except urllib.error.URLError:
            return ""
        finally:
            _release_slot()
    return ""


def extract_isin_and_exchange(info, international=False):
    if international:
        isin = info.get("isin_number") or ""
        exchange = info.get("exchange") or ""
    else:
        isin = info.get("isin_number") or info.get("ISIN") or ""
        exchange = info.get("trading_exchange") or info.get("exchange") or ""
    return isin.strip(), exchange.strip()


def resolve_intl_exchange(api_exchange, ticker):
    """Resolve an FD_INTL exchange to (dim_exchange_name, country, currency).

    Tries the full API exchange name first, then falls back to the ticker
    suffix (e.g. 'L' from 'SHEL.L'). Returns None if unrecognized.
    """
    if api_exchange and api_exchange in INTL_EXCHANGE_MAP:
        return INTL_EXCHANGE_MAP[api_exchange]

    # Extract suffix from ticker (e.g. "SHEL.L" → "L")
    if "." in ticker:
        suffix = ticker.rsplit(".", 1)[-1].upper()
        if suffix in INTL_EXCHANGE_MAP:
            return INTL_EXCHANGE_MAP[suffix]

    return None


def enrich_row(row, api_key, index, total):
    """Enrich a single row with ISIN, exchange, country, and currency.

    Returns (row, status, unrecognized_exchanges) where status is one of
    'enriched', 'skipped', or 'no_data', and unrecognized_exchanges is a
    dict of {exchange_value: [ticker, ...]} for any unresolved intl exchanges.
    """
    ticker = row.get("Ticker", "").strip()
    if not ticker:
        return row, "skipped", {}

    original_exchange = row.get("Exchange", "").strip()
    intl = original_exchange == "FD_INTL"
    is_otc = original_exchange == "FD_OTC"
    is_fd_us = original_exchange == "FD_US"

    # For international tickers, the full "TICKER.EXCHANGE" form is used for API
    # requests (the API requires it), but the exchange suffix is stripped when
    # writing to the output CSV.
    api_ticker = ticker
    if intl and "." in ticker:
        ticker = ticker.rsplit(".", 1)[0]

    endpoint = "intl-company-info" if intl else "company-info"
    print(f"  [{index}/{total}] {ticker} ({endpoint})...", flush=True)
    info = fetch_info(api_key, api_ticker, international=intl)

    if info:
        isin, api_exchange = extract_isin_and_exchange(info, international=intl)

        if not isin:
            isin = fetch_isin_fallback(api_key, api_ticker)
            if isin:
                print(f"  [{index}/{total}] {ticker}: ISIN via securities-information fallback: {isin}")

        row["Isin"] = isin
        row["Ticker"] = ticker  # write stripped ticker to CSV

        if intl:
            resolved = resolve_intl_exchange(api_exchange, api_ticker)
            if resolved:
                exchange, country, currency = resolved
                row["Exchange"] = exchange
                row["Country"] = country
                row["Currency"] = currency
                print(f"  [{index}/{total}] {ticker}: ISIN={isin or '(none)'} exchange={exchange} ({country}, {currency})")
                return row, "enriched", {}
            else:
                raw = api_exchange or "(none)"
                print(f"  [{index}/{total}] {ticker}: ERROR unrecognized exchange {raw!r} — keeping FD_INTL", file=sys.stderr)
                return row, "enriched", {raw: [ticker]}
        else:
            api_exchange = DOMESTIC_EXCHANGE_MAP.get(api_exchange, api_exchange)
            exchange = api_exchange or ("OTCMKTS" if is_otc else ("US" if is_fd_us else original_exchange))
            row["Exchange"] = exchange
            row["Currency"] = "USD"
            row["Country"] = "USA"
            print(f"  [{index}/{total}] {ticker}: ISIN={isin or '(none)'} exchange={exchange or '(none)'}")
            return row, "enriched", {}
    else:
        if is_otc:
            row["Exchange"] = "OTCMKTS"
        elif is_fd_us:
            row["Exchange"] = "US"
        isin = fetch_isin_fallback(api_key, api_ticker)
        if isin:
            row["Isin"] = isin
            print(f"  [{index}/{total}] {ticker}: no primary data — ISIN={isin} via securities-information fallback")
        else:
            print(f"  [{index}/{total}] {ticker}: no data")
        row["Ticker"] = ticker  # write stripped ticker to CSV
        return row, "no_data", {}


def main():
    parser = argparse.ArgumentParser(
        description="Enrich a securities CSV with ISIN and exchange from FinancialData.net."
    )
    parser.add_argument("input_csv", help="Path to a securities CSV from fetch_symbols.py")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only enrich the first N rows (useful for sampling)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        metavar="N",
        help="Number of concurrent API requests (default: 8; API limit is 30 RPS)",
    )
    args = parser.parse_args()

    load_dotenv()

    api_key = os.environ.get("FD_KEY")
    if not api_key:
        print("Error: FD_KEY not set in environment or any .env file found upward from this script.", file=sys.stderr)
        sys.exit(1)

    input_path = Path(args.input_csv)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    output_path = input_path.parent / (input_path.stem + "_enriched" + input_path.suffix)

    fieldnames = ["Ticker", "Name", "Exchange", "Type", "Currency", "Country", "Isin"]

    with open(input_path, newline="", encoding="utf-8") as infile:
        rows = list(csv.DictReader(infile))
    if args.limit is not None:
        rows = rows[: args.limit]

    total = len(rows)
    print(f"Enriching {total} rows from {input_path}")
    print(f"Output: {output_path} (row order may differ from input)")
    print(f"Workers: {args.workers}")

    _concurrency["max"] = args.workers

    enriched = 0
    skipped = 0
    all_unrecognized = {}  # exchange_value → [ticker, ...]
    write_lock = threading.Lock()

    with open(output_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(enrich_row, row, api_key, i + 1, total): i
                for i, row in enumerate(rows)
            }
            for future in as_completed(futures):
                result_row, status, unrecognized = future.result()
                if status == "enriched":
                    enriched += 1
                else:
                    skipped += 1
                for exch, tickers in unrecognized.items():
                    all_unrecognized.setdefault(exch, []).extend(tickers)
                with write_lock:
                    writer.writerow(result_row)

    print(f"\nDone. Enriched: {enriched}, No data/skipped: {skipped}")
    print(f"Saved to {output_path}")

    if all_unrecognized:
        print(f"\nERROR: {sum(len(v) for v in all_unrecognized.values())} row(s) had unrecognized exchange values.", file=sys.stderr)
        print("Add these to INTL_EXCHANGE_MAP in this script and exchange_map.md:", file=sys.stderr)
        for exch, tickers in sorted(all_unrecognized.items()):
            print(f"  {exch!r}: {', '.join(tickers[:5])}{'...' if len(tickers) > 5 else ''}", file=sys.stderr)


if __name__ == "__main__":
    main()

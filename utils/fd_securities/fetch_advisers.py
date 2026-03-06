#!/usr/bin/python3
# python3 fetch_advisers.py [--limit N] [--workers N] [--first-page-only]
#
# Two-phase fetch of Financial Adviser data from FinancialData.net:
#
#   Phase 1 — Fetch the full list of investment adviser names (paginated).
#             Saves raw names to securities/fd_advisers.csv.
#
#   Phase 2 — For each name, query the investment-adviser-information endpoint
#             in parallel (URL-encoding the name).  Saves all detail records to
#             securities/fd_adviser_info.csv using whatever fields the API
#             returns (columns discovered dynamically from the first response).
#
# Reads FD_KEY from .env (walked up from this script's location).
# Prints running progress and a final summary to stdout.

import argparse
import csv
import json
import os
import socket
import sys
import time
import threading
import urllib.parse
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE_URL = "https://financialdata.net/api/v1"
PAGE_SIZE = 500
OUTPUT_DIR = Path(__file__).resolve().parent / "securities"
NAMES_CSV = OUTPUT_DIR / "fd_advisers.csv"
INFO_CSV = OUTPUT_DIR / "fd_adviser_info.csv"


# ---------------------------------------------------------------------------
# .env loader (same pattern as other scripts in this directory)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Phase 1 — fetch the adviser name list
# ---------------------------------------------------------------------------

def fetch_names_page(api_key, offset):
    url = f"{BASE_URL}/investment-adviser-names?offset={offset}&limit={PAGE_SIZE}&key={api_key}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            if resp.status != 200:
                print(f"  HTTP {resp.status} at offset={offset}", file=sys.stderr)
                return []
            data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, list):
                return data
            for key in ("data", "results", "advisers", "names"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            print(f"  Unexpected response shape at offset={offset}: {list(data.keys())}", file=sys.stderr)
            return []
    except urllib.error.URLError as e:
        print(f"  Network error at offset={offset}: {e}", file=sys.stderr)
        return []


def fetch_all_names(api_key, first_page_only=False):
    all_records = []
    offset = 0
    while True:
        print(f"  Fetching names offset={offset}...", end=" ", flush=True)
        page = fetch_names_page(api_key, offset)
        print(f"{len(page)} records")
        all_records.extend(page)
        if len(page) < PAGE_SIZE or first_page_only:
            break
        offset += PAGE_SIZE
    return all_records


def extract_name(record):
    """Pull the adviser name out of whatever shape the record has."""
    if isinstance(record, str):
        return record.strip()
    for field in ("name", "adviser_name", "investment_adviser_name", "registrant_name", "cik_name"):
        val = record.get(field)
        if val:
            return str(val).strip()
    # Fall back: first string-valued field
    for val in record.values():
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def extract_cik(record):
    if isinstance(record, dict):
        for field in ("cik", "cik_number", "central_index_key"):
            val = record.get(field)
            if val is not None:
                return str(val).strip()
    return ""


def save_names_csv(records):
    OUTPUT_DIR.mkdir(exist_ok=True)
    fieldnames = ["name", "cik"]
    written = 0
    with open(NAMES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            name = extract_name(rec)
            if not name:
                continue
            writer.writerow({"name": name, "cik": extract_cik(rec)})
            written += 1
    return written


# ---------------------------------------------------------------------------
# Phase 2 — fetch individual adviser info (parallelised)
# ---------------------------------------------------------------------------

def fetch_adviser_info(name, api_key, retries=3, backoff=2.0):
    """Return (name, status, data_or_error) where status is 'data'/'empty'/'error'."""
    encoded = urllib.parse.quote(name, safe="")
    url = f"{BASE_URL}/investment-adviser-information?identifier={encoded}&key={api_key}"
    last_err = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=20) as resp:
                body = resp.read()
                if len(body) < 10:
                    return name, "empty", {}
                data = json.loads(body.decode("utf-8"))
                # The API may return a single object or a list; normalise to dict
                if isinstance(data, list):
                    data = data[0] if data else {}
                return name, "data", data
        except urllib.error.HTTPError as e:
            if e.code == 429:
                last_err = "429"
                time.sleep(backoff * (attempt + 1))
                continue
            return name, "error", {"http_error": e.code}
        except Exception as e:
            last_err = str(e)
            time.sleep(backoff * (attempt + 1))
    return name, "error", {"error": last_err}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch investment adviser names and detail info from FinancialData.net."
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only the first N advisers (useful for testing).")
    parser.add_argument("--workers", type=int, default=10,
                        help="Concurrent requests for Phase 2 (default 10).")
    parser.add_argument("--first-page-only", action="store_true",
                        help="Fetch only the first page of names (~500 records).")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.environ.get("FD_KEY")
    if not api_key:
        print("ERROR: FD_KEY not found in environment or .env", file=sys.stderr)
        sys.exit(1)

    # Hard backstop: urllib timeout= covers the socket, but DNS resolution and
    # SSL handshakes can still stall indefinitely on some platforms.
    socket.setdefaulttimeout(35)

    OUTPUT_DIR.mkdir(exist_ok=True)

    # ------------------------------------------------------------------
    # Phase 1: adviser name list
    # ------------------------------------------------------------------
    print("=== Phase 1: Fetching investment adviser names ===")
    name_records = fetch_all_names(api_key, first_page_only=args.first_page_only)
    print(f"  Total names fetched: {len(name_records)}")

    written = save_names_csv(name_records)
    print(f"  Saved {written} names → {NAMES_CSV}\n")

    # Build the working list of names (apply --limit after dedup)
    names = []
    seen = set()
    for rec in name_records:
        n = extract_name(rec)
        if n and n not in seen:
            seen.add(n)
            names.append(n)
        if args.limit and len(names) >= args.limit:
            break

    total = len(names)
    if total == 0:
        print("No adviser names to look up. Exiting.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # Phase 2: parallel adviser info lookups — stream results to CSV
    # ------------------------------------------------------------------
    print(f"=== Phase 2: Fetching adviser info ({total} advisers, {args.workers} workers) ===")

    counts = {"data": 0, "empty": 0, "error": 0}
    error_examples = []
    done = 0

    # Fieldnames are discovered from the first data response; the header is
    # written lazily so we don't need to buffer all results before writing.
    csv_fieldnames = None
    writer = None
    write_lock = threading.Lock()

    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(INFO_CSV, "w", newline="", encoding="utf-8") as info_file:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(fetch_adviser_info, name, api_key): name for name in names}
            for future in as_completed(futures):
                name, status, detail = future.result()
                counts[status] += 1
                done += 1

                if status == "error" and len(error_examples) < 5:
                    error_examples.append((name, detail))

                if status == "data" and isinstance(detail, dict) and detail:
                    with write_lock:
                        if csv_fieldnames is None:
                            csv_fieldnames = ["adviser_name"] + list(detail.keys())
                            writer = csv.DictWriter(
                                info_file, fieldnames=csv_fieldnames, extrasaction="ignore"
                            )
                            writer.writeheader()
                        writer.writerow({"adviser_name": name, **detail})
                        info_file.flush()

                if done % 100 == 0 or done == total:
                    pct = done / total * 100
                    print(
                        f"  [{done:5d}/{total}] {pct:5.1f}%  "
                        f"data={counts['data']}  empty={counts['empty']}  error={counts['error']}"
                    )

    if csv_fieldnames is None:
        # No data results at all — write a minimal header so the file exists.
        with open(INFO_CSV, "w", newline="", encoding="utf-8") as f:
            f.write("adviser_name\n")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Advisers queried : {total}")
    print(f"  Has data         : {counts['data']:6d}  ({counts['data']/total*100:.1f}%)")
    print(f"  Empty/no data    : {counts['empty']:6d}  ({counts['empty']/total*100:.1f}%)")
    print(f"  Errors           : {counts['error']:6d}  ({counts['error']/total*100:.1f}%)")
    print(f"\n  Names CSV  → {NAMES_CSV}")
    print(f"  Info CSV   → {INFO_CSV}")
    if error_examples:
        print("\n  Error examples:")
        for n, d in error_examples:
            print(f"    {n!r}: {d}")


if __name__ == "__main__":
    main()

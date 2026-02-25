#!/usr/bin/env python3
"""Strip Fidelity CSV exports down to Symbol and Quantity columns.

For money market holdings (SPAXX, description "Cash" or "HELD IN MONEY MARKET"),
copies Current Value (minus $) into Quantity.
Removes the disclaimer block at the end of the file.
"""

import csv
import sys
import os

CASH_SYMBOLS = {"SPAXX", "SPAXX**"}
CASH_DESCRIPTIONS = {"CASH", "HELD IN MONEY MARKET"}
DISCLAIMER_PREFIX = "The data and information"


def clean_csv(input_path):
    base, ext = os.path.splitext(input_path)
    output_path = f"{base}_clean{ext}"

    with open(input_path, newline="", encoding="utf-8-sig") as infile:
        reader = csv.DictReader(infile)
        rows = []
        for row in reader:
            description = (row.get("Description") or "").strip().upper()
            symbol = (row.get("Symbol") or "").strip()

            # Stop at disclaimer
            if symbol.startswith('"') or description.startswith(DISCLAIMER_PREFIX.upper()):
                break
            # Skip empty rows
            if not symbol:
                break

            quantity = (row.get("Quantity") or "").strip()

            # Money market: use Current Value as quantity
            if (symbol.upper().rstrip("*") in {s.rstrip("*") for s in CASH_SYMBOLS}
                    or description in CASH_DESCRIPTIONS):
                current_value = (row.get("Current Value") or "").strip()
                quantity = current_value.replace("$", "").replace(",", "")
                symbol = symbol.rstrip("*")

            rows.append({"Symbol": symbol, "Quantity": quantity})

    with open(output_path, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["Symbol", "Quantity"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"{input_path} -> {output_path} ({len(rows)} rows)")


def merge_clean_csvs(clean_paths, output_path):
    """Merge multiple clean CSVs, summing quantities for duplicate symbols.

    Writes with columns 'ticker,percentage_or_shares' to match the Go CSV parser.
    """
    totals = {}
    order = []
    for path in clean_paths:
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row["Symbol"]
                qty = float(row["Quantity"])
                if symbol not in totals:
                    order.append(symbol)
                    totals[symbol] = 0.0
                totals[symbol] += qty

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "percentage_or_shares"])
        writer.writeheader()
        for symbol in order:
            writer.writerow({"ticker": symbol, "percentage_or_shares": round(totals[symbol], 6)})

    print(f"Merged {len(clean_paths)} files -> {output_path} ({len(totals)} symbols)")


if __name__ == "__main__":
    files = sys.argv[1:] if len(sys.argv) > 1 else ["ira.csv", "taxable.csv"]
    clean_paths = []
    for f in files:
        clean_csv(f)
        base, ext = os.path.splitext(f)
        clean_paths.append(f"{base}_clean{ext}")

    if len(clean_paths) > 1:
        merged_dir = os.path.dirname(clean_paths[0]) or "."
        merge_clean_csvs(clean_paths, os.path.join(merged_dir, "merged_clean.csv"))

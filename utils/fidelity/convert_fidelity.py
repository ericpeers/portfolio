import csv
import sys
import os
import glob
from datetime import datetime

def clean_value(val):
    if not val:
        return ""
    return val.replace('$', '').replace(',', '').strip()

ALLIE_ACCOUNT_NAMES = {'Allie Taxable Inherited (UTMA)', 'Allie IRA Inherited'}

PURGED_SYMBOLS = {'AJNMY', 'NHFSMKX98', 'NH7000902', 'NHFSTMX97'}

def parse_rows(input_file):
    """Parse the Fidelity CSV and return a list of dicts with account_number, account_name, ticker, quantity."""
    rows = []
    with open(input_file, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue

            symbol = row.get('Symbol') or ''
            symbol = symbol.strip()

            if not symbol or symbol.lower() == 'pending activity' or ' ' in symbol:
                continue

            if symbol in PURGED_SYMBOLS:
                continue

            if symbol.endswith('**'):
                symbol = symbol[:-2]

            account_number = (row.get('Account Number') or '').strip()
            account_name = (row.get('Account Name') or '').strip()
            quantity = (row.get('Quantity') or '').strip()
            current_value = (row.get('Current Value') or '').strip()

            final_quantity = clean_value(quantity) if quantity else clean_value(current_value)

            if symbol and final_quantity:
                rows.append({
                    'account_number': account_number,
                    'account_name': account_name,
                    'ticker': symbol,
                    'percentage_or_shares': final_quantity,
                })
    return rows

def write_csv(output_file, rows):
    with open(output_file, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['ticker', 'percentage_or_shares'])
        writer.writeheader()
        for row in rows:
            writer.writerow({'ticker': row['ticker'], 'percentage_or_shares': row['percentage_or_shares']})

def main(input_file, output_dir):
    rows = parse_rows(input_file)

    all_rows      = rows
    managed_rows  = [r for r in rows if r['account_number'].startswith('Y')]
    allie_rows    = [r for r in rows if r['account_name'] in ALLIE_ACCOUNT_NAMES]
    self_rows     = [r for r in rows if not r['account_number'].startswith('Y')]

    outputs = [
        ('fidelity_all.csv',     all_rows),
        ('fidelity_managed.csv', managed_rows),
        ('allie_actual.csv',     allie_rows),
        ('self_managed.csv',     self_rows),
    ]

    for filename, segment in outputs:
        path = os.path.join(output_dir, filename)
        write_csv(path, segment)
        print(f"Wrote {len(segment):>4} rows → {path}")

def find_latest_positions_csv(search_dir):
    """Return the most recent Portfolio_Positions_Mon-DD-YYYY.csv in search_dir."""
    pattern = os.path.join(search_dir, 'Portfolio_Positions_*.csv')
    candidates = []
    for path in glob.glob(pattern):
        name = os.path.basename(path)
        # e.g. Portfolio_Positions_Mar-26-2026.csv
        date_part = name[len('Portfolio_Positions_'):-len('.csv')]
        try:
            dt = datetime.strptime(date_part, '%b-%d-%Y')
            candidates.append((dt, path))
        except ValueError:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))

    if len(sys.argv) > 1:
        input_csv = sys.argv[1]
    else:
        input_csv = find_latest_positions_csv(script_dir)
        if not input_csv:
            print("Error: no Portfolio_Positions_*.csv found in", script_dir)
            sys.exit(1)
        print(f"Auto-detected input: {os.path.basename(input_csv)}")

    output_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(os.path.abspath(input_csv))

    if not os.path.exists(input_csv):
        print(f"Error: {input_csv} not found.")
        sys.exit(1)

    main(input_csv, output_dir)

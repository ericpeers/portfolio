#!/usr/bin/python3

# API_TOKEN="your_api_token" python3 fetch_exchange_symbols.py < eodhd_exchanges.json
import sys
import os
import json
import urllib.request



def main():
    """
    Fetches exchange symbol lists from eodhd.com and saves them to files.
    """
    api_token = os.environ.get("API_TOKEN")
    if not api_token:
        print("Error: API_TOKEN environment variable not set.", file=sys.stderr)
        sys.exit(1)

    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError:
        print("Error: Invalid JSON received on STDIN.", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists("results"):
        os.makedirs("results")

    for record in input_data:
        exchange_code = record.get("Code")
        if not exchange_code:
            continue

        url = f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}?api_token={api_token}&fmt=json"

        try:
            with urllib.request.urlopen(url) as response:
                if response.status == 200:
                    data = response.read()
                    output_filename = f"results/eodhd_tickers_{exchange_code}.json"
                    with open(output_filename, "wb") as f:
                        f.write(data)
                    print(f"Successfully fetched and saved symbols for {exchange_code}")
                else:
                    print(f"Error fetching data for {exchange_code}: HTTP {response.status}", file=sys.stderr)
        except urllib.error.URLError as e:
            print(f"Error fetching data for {exchange_code}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()

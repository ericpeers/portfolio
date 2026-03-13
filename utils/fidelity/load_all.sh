#!/usr/bin/env bash
# Submit every fidelity_TICKER_date.csv in results/ to /admin/load_etf_holdings

BASE_URL="${PORTFOLIO_URL:-http://localhost:8080}"
RESULTS_DIR="$(dirname "$0")/results"

success=0
failure=0
total_holdings=0

# Parse a load_etf_holdings JSON response body.
# Outputs the count of holdings loaded.
_parse_etf_stats() {
    echo "$1" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print(len(d.get('holdings') or []))
except Exception:
    print(0)
"
}

for csv_file in "$RESULTS_DIR"/fidelity_*.csv; do
    [[ -e "$csv_file" ]] || { echo "No CSV files found in $RESULTS_DIR"; exit 1; }

    filename="$(basename "$csv_file")"
    # Extract ticker from fidelity_{TICKER}_{date}.csv
    ticker="${filename#fidelity_}"
    ticker="${ticker%_*}"

    echo -n "Uploading $filename (ticker=$ticker)... "

    response=$(curl -s -w "\n%{http_code}" \
        -X POST "$BASE_URL/admin/load_etf_holdings" \
        -F "ticker=$ticker" \
        -F "file=@$csv_file")

    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | head -n -1)

    if [[ "$http_code" == "200" ]]; then
        count=$(_parse_etf_stats "$body")
        echo "OK  holdings=${count}"
        ((total_holdings += count))
        ((success++))
    else
        echo "FAILED (HTTP $http_code): $body"
        ((failure++))
    fi
done

echo ""
echo "Done: $success succeeded, $failure failed"
printf "  %-20s %d\n" "total holdings loaded:" "$total_holdings"

#!/usr/bin/env bash
# Submit every fidelity_TICKER_date.csv in results/ to /admin/load_etf_holdings

BASE_URL="${PORTFOLIO_URL:-http://localhost:8080}"
RESULTS_DIR="$(dirname "$0")/eodhd_tickers"

success=0
failure=0

for csv_file in "$RESULTS_DIR"/*.csv; do
    [[ -e "$csv_file" ]] || { echo "No CSV files found in $RESULTS_DIR"; exit 1; }

    echo -n "Uploading $csv_file ... "

    response=$(curl -s -w "\n%{http_code}" \
        -X POST "$BASE_URL/admin/load_securities" \
        -F "file=@$csv_file")

    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | head -n -1)

    if [[ "$http_code" == "200" ]]; then
        echo "OK"
        ((success++))
    else
        echo "FAILED (HTTP $http_code): $body"
        ((failure++))
    fi
done

echo ""
echo "Done: $success succeeded, $failure failed"

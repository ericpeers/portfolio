#!/usr/bin/env bash
# Submit every fd_*.csv in etf_holdings/ to /admin/load_etf_holdings

# Load .env from script dir or nearest parent dir; env vars take precedence
_load_dotenv() {
    local dir
    dir="$(cd "$(dirname "$0")" && pwd)"
    while [[ "$dir" != "/" ]]; do
        if [[ -f "$dir/.env" ]]; then
            while IFS='=' read -r key value; do
                [[ "$key" =~ ^[[:space:]]*# ]] && continue
                key="${key// /}"
                [[ -z "$key" ]] && continue
                value="${value#"${value%%[!\" ]*}"}"  # strip leading quotes/spaces
                value="${value%"${value##*[!\" ]}"}"  # strip trailing quotes/spaces
                [[ -z "${!key+x}" ]] && export "$key"="$value"
            done < "$dir/.env"
            break
        fi
        dir="$(dirname "$dir")"
    done
}
_load_dotenv

BASE_URL="${PORTFOLIO_URL:-http://localhost:8080}"
AUTH_TOKEN="${AUTH_TOKEN:?AUTH_TOKEN environment variable not set}"
ETF_DIR="$(dirname "$0")/etf_holdings"

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

for csv_file in "$ETF_DIR"/fd_*.csv; do
    [[ -e "$csv_file" ]] || { echo "No CSV files found in $ETF_DIR"; exit 1; }

    filename="$(basename "$csv_file")"
    # Extract ticker from fd_{TICKER}_{YYYYMMDD}.csv
    ticker="${filename#fd_}"
    ticker="${ticker%_*}"

    echo -n "Uploading $filename (ticker=$ticker)... "

    response=$(curl -s -w "\n%{http_code}" \
        -X POST "$BASE_URL/admin/load_etf_holdings" \
        -H "Authorization: Bearer $AUTH_TOKEN" \
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

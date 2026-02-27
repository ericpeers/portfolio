#!/usr/bin/env bash
# Submit every *.csv in eodhd_tickers/ to /admin/load_securities

BASE_URL="${PORTFOLIO_URL:-http://localhost:8080}"
RESULTS_DIR="$(dirname "$0")/eodhd_tickers"

success=0
failure=0
total_inserted=0
total_skipped_existing=0
total_skipped_bad_type=0
total_skipped_dup_in_file=0
total_skipped_long_ticker=0
all_new_exchanges=()

# Parse a load_securities JSON response body.
# Outputs one tab-separated line: inserted skipped_existing skipped_bad_type skipped_dup_in_file skipped_long_ticker new_exchanges(|-separated)
_parse_securities_stats() {
    echo "$1" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    exchanges = '|'.join(d.get('new_exchanges') or [])
    print('\t'.join([
        str(d.get('inserted', 0)),
        str(d.get('skipped_existing', 0)),
        str(d.get('skipped_bad_type', 0)),
        str(d.get('skipped_dup_in_file', 0)),
        str(d.get('skipped_long_ticker', 0)),
        exchanges,
    ]))
except Exception:
    print('0\t0\t0\t0\t0\t')
"
}

for csv_file in "$RESULTS_DIR"/*.csv; do
    [[ -e "$csv_file" ]] || { echo "No CSV files found in $RESULTS_DIR"; exit 1; }

    echo -n "Uploading $(basename "$csv_file") ... "

    response=$(curl -s -w "\n%{http_code}" \
        -X POST "$BASE_URL/admin/load_securities" \
        -F "file=@$csv_file")

    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | head -n -1)

    if [[ "$http_code" == "200" ]]; then
        IFS=$'\t' read -r ins sk_ex sk_bt sk_dup sk_long new_ex_str \
            <<< "$(_parse_securities_stats "$body")"
        echo "OK  inserted=${ins}  skipped_existing=${sk_ex}  skipped_bad_type=${sk_bt}  skipped_dup=${sk_dup}  skipped_long=${sk_long}"
        ((total_inserted        += ins))
        ((total_skipped_existing += sk_ex))
        ((total_skipped_bad_type += sk_bt))
        ((total_skipped_dup_in_file += sk_dup))
        ((total_skipped_long_ticker += sk_long))
        if [[ -n "$new_ex_str" ]]; then
            IFS='|' read -ra these_exchanges <<< "$new_ex_str"
            all_new_exchanges+=("${these_exchanges[@]}")
        fi
        ((success++))
    else
        echo "FAILED (HTTP $http_code): $body"
        ((failure++))
    fi
done

echo ""
echo "Done: $success succeeded, $failure failed"
echo ""
echo "Totals:"
printf "  %-26s %d\n" "inserted:"            "$total_inserted"
printf "  %-26s %d\n" "skipped_existing:"    "$total_skipped_existing"
printf "  %-26s %d\n" "skipped_bad_type:"    "$total_skipped_bad_type"
printf "  %-26s %d\n" "skipped_dup_in_file:" "$total_skipped_dup_in_file"
printf "  %-26s %d\n" "skipped_long_ticker:" "$total_skipped_long_ticker"
echo ""
echo "New exchanges (${#all_new_exchanges[@]}):"
for ex in "${all_new_exchanges[@]}"; do
    echo "  $ex"
done

#!/usr/bin/env bash
# fetch_historic.sh — pre-loads historical bulk price data by hitting the
# /admin/bulk-fetch-eodhd-prices endpoint once per trading day.
#
# Usage:
#   ./fetch_historic.sh                        # last 1 year → today
#   ./fetch_historic.sh 2023-01-01             # 2023-01-01 → today
#   ./fetch_historic.sh 2023-01-01 2023-12-31  # explicit range
#
# Weekend days (Sat/Sun) are skipped; NYSE holidays are not filtered
# here since the server handles them gracefully (returns fetched=0).
#
# Environment variables:
#   HOST    — server base URL          (default: http://localhost:8080)
#   USER_ID — value for X-User-ID header (default: 1)

set -euo pipefail

HOST="${HOST:-http://localhost:8080}"
USER_ID="${USER_ID:-1}"

# ── Date range ────────────────────────────────────────────────────────────────
END_DATE=$(date +%Y-%m-%d)
START_DATE=$(date -d "1 year ago" +%Y-%m-%d)

case $# in
  0) ;;
  1) START_DATE="$1" ;;
  2) START_DATE="$1"; END_DATE="$2" ;;
  *) echo "Usage: $0 [start_date [end_date]]" >&2; exit 1 ;;
esac

# Basic format validation
if ! date -d "$START_DATE" &>/dev/null; then
  echo "ERROR: invalid start_date '$START_DATE' (expected YYYY-MM-DD)" >&2; exit 1
fi
if ! date -d "$END_DATE" &>/dev/null; then
  echo "ERROR: invalid end_date '$END_DATE' (expected YYYY-MM-DD)" >&2; exit 1
fi
if [[ "$START_DATE" > "$END_DATE" ]]; then
  echo "ERROR: start_date ($START_DATE) is after end_date ($END_DATE)" >&2; exit 1
fi

# ── Count trading days for progress display ───────────────────────────────────
total=0
d="$START_DATE"
until [[ "$d" > "$END_DATE" ]]; do
  dow=$(date -d "$d" +%u)   # 1=Mon … 5=Fri, 6=Sat, 7=Sun
  [[ "$dow" -lt 6 ]] && (( total++ )) || true
  d=$(date -d "$d + 1 day" +%Y-%m-%d)
done

echo "Host:  $HOST"
echo "Range: $START_DATE → $END_DATE  ($total trading days)"
echo ""

# ── Fetch loop ────────────────────────────────────────────────────────────────
count=0
failed=0

d="$END_DATE"
until [[ "$d" < "$START_DATE" ]]; do
  dow=$(date -d "$d" +%u)

  if [[ "$dow" -lt 6 ]]; then
    (( count++ )) || true
    printf "[%d/%d] %s  " "$count" "$total" "$d"

    http_code=$(curl -s \
      -o /tmp/_fetch_historic.json \
      -w "%{http_code}" \
      -X GET \
      "${HOST}/admin/bulk-fetch-eodhd-prices?date=${d}" \
      -H "X-User-ID: ${USER_ID}")

    if [[ "$http_code" == "200" ]]; then
      if command -v jq &>/dev/null; then
        jq -r '"fetched=\(.fetched) stored=\(.stored) skipped=\(.skipped)"' \
          /tmp/_fetch_historic.json
      else
        cat /tmp/_fetch_historic.json; echo
      fi
    else
      echo "FAILED (HTTP $http_code)"
      cat /tmp/_fetch_historic.json >&2; echo >&2
      (( failed++ )) || true
    fi
  fi

  d=$(date -d "$d - 1 day" +%Y-%m-%d)
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
if [[ "$failed" -eq 0 ]]; then
  echo "Done. $count days fetched successfully."
else
  echo "Done. $count days processed, $failed failed." >&2
  exit 1
fi

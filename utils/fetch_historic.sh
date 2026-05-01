#!/usr/bin/env bash
# fetch_historic.sh — loads bulk price data by hitting the
# /admin/bulk-fetch-eodhd-prices endpoint once per trading day.
#
# Usage:
#   ./fetch_historic.sh                        # last market day (see below)
#   ./fetch_historic.sh 2023-06-01             # exactly 2023-06-01 only
#   ./fetch_historic.sh 2023-01-01 2023-12-31  # explicit range
#
# No-date behaviour:
#   Before 4:20 PM ET → fetches the preceding market day (data is settled)
#   After  4:20 PM ET → fetches today (partial; passes min_required=0 since
#                        EODHD data may not be fully published yet)
#
# Weekend days (Sat/Sun) are skipped; NYSE holidays are not filtered
# here since the server handles them gracefully (returns HTTP 422).
#
# Environment variables:
#   HOST          — server base URL  (default: http://localhost:8080)
#   ADMIN_EMAIL   — admin account email (can also be set in .env)
#   ADMIN_PASS    — admin account password (can also be set in .env)

set -euo pipefail

# Locate the project root (nearest ancestor containing bin/login).
_PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
while [[ "$_PROJECT_ROOT" != "/" ]] && [[ ! -x "$_PROJECT_ROOT/bin/login" ]]; do
    _PROJECT_ROOT="$(dirname "$_PROJECT_ROOT")"
done
[[ -x "$_PROJECT_ROOT/bin/login" ]] || { echo "ERROR: bin/login not found" >&2; exit 1; }

HOST="${HOST:-http://localhost:8080}"
TOKEN="$("$_PROJECT_ROOT/bin/login")"

# ── Date range ────────────────────────────────────────────────────────────────
PARTIAL_FETCH=false

case $# in
  0)
    # No date: determine the last market day in ET.
    # After 4:20 PM ET on a weekday → today (partial, data may be mid-publish).
    # Otherwise → most recent weekday before today (data is settled).
    ET_HOUR=$(TZ="America/New_York" date +%H)
    ET_MIN=$(TZ="America/New_York" date +%M)
    TODAY_ET=$(TZ="America/New_York" date +%Y-%m-%d)
    TODAY_DOW=$(TZ="America/New_York" date +%u)  # 1=Mon…5=Fri, 6=Sat, 7=Sun

    AFTER_420=false
    if [[ "$TODAY_DOW" -lt 6 ]] && \
       { [[ "$ET_HOUR" -gt 16 ]] || [[ "$ET_HOUR" -eq 16 && "$ET_MIN" -ge 20 ]]; }; then
      AFTER_420=true
    fi

    if [[ "$AFTER_420" == "true" ]]; then
      START_DATE="$TODAY_ET"
      PARTIAL_FETCH=true
    else
      d=$(TZ="America/New_York" date -d "$TODAY_ET - 1 day" +%Y-%m-%d)
      while [[ $(date -d "$d" +%u) -ge 6 ]]; do
        d=$(date -d "$d - 1 day" +%Y-%m-%d)
      done
      START_DATE="$d"
    fi
    END_DATE="$START_DATE"
    ;;

  1)
    START_DATE="$1"
    END_DATE="$1"
    ;;

  2)
    START_DATE="$1"
    END_DATE="$2"
    ;;

  *)
    echo "Usage: $0 [start_date [end_date]]" >&2
    exit 1
    ;;
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
  dow=$(date -d "$d" +%u)
  [[ "$dow" -lt 6 ]] && (( total++ )) || true
  d=$(date -d "$d + 1 day" +%Y-%m-%d)
done

echo "Host:  $HOST"
if [[ "$START_DATE" == "$END_DATE" ]]; then
  if [[ "$PARTIAL_FETCH" == "true" ]]; then
    echo "Date:  $START_DATE  (partial — EODHD data may not be fully published)"
  else
    echo "Date:  $START_DATE"
  fi
else
  echo "Range: $START_DATE → $END_DATE  ($total trading days)"
fi
echo ""

# ── Fetch loop ────────────────────────────────────────────────────────────────
count=0
failed=0

# Partial fetches bypass the min_required gate (data is still mid-publish).
MIN_REQUIRED_PARAM=""
[[ "$PARTIAL_FETCH" == "true" ]] && MIN_REQUIRED_PARAM="&min_required=0"

d="$END_DATE"
until [[ "$d" < "$START_DATE" ]]; do
  dow=$(date -d "$d" +%u)

  if [[ "$dow" -lt 6 ]]; then
    (( count++ )) || true
    printf "[%d/%d] %s  " "$count" "$total" "$d"

    http_code=$(curl -s \
      --connect-timeout 5 \
      -o /tmp/_fetch_historic.json \
      -w "%{http_code}" \
      -X GET \
      "${HOST}/admin/bulk-fetch-eodhd-prices?date=${d}${MIN_REQUIRED_PARAM}" \
      -H "Authorization: Bearer $TOKEN") || http_code=""

    if [[ -z "$http_code" || "$http_code" == "000" ]]; then
      echo "FAILED (no response — is the server running at $HOST?)"
      (( failed++ )) || true
    elif [[ "$http_code" == "200" ]]; then
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
  echo "Done. $count day(s) fetched successfully."
else
  echo "Done. $count day(s) processed, $failed failed." >&2
  exit 1
fi

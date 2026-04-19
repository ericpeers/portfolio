#!/usr/bin/env bash
# purge_fundamentals.sh — removes all fundamentals data for a list of tickers.
#
# Clears fact_fundamentals, fact_financials_history, dim_security_listings, and
# the fundamentals columns on dim_security (cik, isin, inception, gic, etc.).
# The dim_security row itself is preserved.
#
# Usage:
#   utils/purge_fundamentals.sh TICKER1 TICKER2 ...
#
# Example:
#   utils/purge_fundamentals.sh BITO TQQQ UVXY

set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "usage: $(basename "$0") TICKER [TICKER ...]" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

if [[ -f "$ENV_FILE" ]]; then
    PG_URL=$(grep -E '^PG_URL=' "$ENV_FILE" | head -1 | cut -d= -f2-)
fi

if [[ -z "${PG_URL:-}" ]]; then
    echo "$(basename "$0"): PG_URL not set in $ENV_FILE" >&2
    exit 1
fi

# Build a SQL array literal from the positional args: ('BITO','TQQQ','UVXY')
TICKER_LIST=$(printf "'%s'," "$@")
TICKER_LIST="(${TICKER_LIST%,})"

SQL=$(cat <<EOF
BEGIN;

WITH targets AS (SELECT id FROM dim_security WHERE ticker IN $TICKER_LIST)
DELETE FROM fact_fundamentals       WHERE security_id IN (SELECT id FROM targets);

WITH targets AS (SELECT id FROM dim_security WHERE ticker IN $TICKER_LIST)
DELETE FROM fact_financials_history WHERE security_id IN (SELECT id FROM targets);

WITH targets AS (SELECT id FROM dim_security WHERE ticker IN $TICKER_LIST)
DELETE FROM dim_security_listings   WHERE security_id IN (SELECT id FROM targets);

UPDATE dim_security SET
    cik               = NULL,
    cusip             = NULL,
    lei               = NULL,
    description       = NULL,
    employees         = NULL,
    country_iso       = NULL,
    fiscal_year_end   = NULL,
    gic_sector        = NULL,
    gic_group         = NULL,
    gic_industry      = NULL,
    gic_sub_industry  = NULL,
    isin              = NULL,
    inception         = NULL,
    url               = NULL,
    etf_url           = NULL,
    net_expense_ratio = NULL,
    total_assets      = NULL,
    etf_yield         = NULL,
    nav               = NULL
WHERE ticker IN $TICKER_LIST;

COMMIT;
EOF
)

echo "Purging fundamentals for: $*"
psql "$PG_URL" -c "$SQL"
echo "Done."

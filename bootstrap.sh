#!/usr/bin/env bash
# bootstrap.sh — One-shot setup for a fresh Portfolio server instance.
#
# Run order:
#   1. Load EODHD securities      (utils/eodhd_securities/load_all.sh)
#   2. Load FD.net securities     (utils/fd_securities/load_securities.sh)
#   3. Prefetch US10Y treasury    (GET /admin/get_daily_prices)
#   4. Create sample portfolios   (POST /portfolios)
#   5. Load Fidelity ETF holdings (utils/fidelity/load_all.sh)
#   6. Compare Allie's portfolios (POST /portfolios/compare)  [--compare only]
#
# Usage:
#   ./bootstrap.sh [options]
#
# Options:
#   --skip-eodhd      Skip step 1 (EODHD securities)
#   --skip-fd         Skip step 2 (FD.net securities)
#   --skip-fidelity   Skip step 5 (Fidelity ETF holdings)
#   --compare         Run step 6  (compare Allie Ideal vs Allie Actual)
#   --owner-id N      User ID that owns created portfolios (default: 1)
#   --help            Show this help text and exit
#
# Environment variables:
#   PORTFOLIO_URL   Server base URL (default: http://localhost:8080)
#
# Re-run safety:
#   Portfolio creation is idempotent — a 409 response means the portfolio
#   already exists and is skipped.  All other steps are idempotent too.
#   If both Allie portfolios were already created (409) and --compare is
#   passed, the script prints a ready-to-run curl command instead.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ──────────────────────── Defaults ─────────────────────────
BASE_URL="${PORTFOLIO_URL:-http://localhost:8080}"
OWNER_ID=1
DO_COMPARE=false
SKIP_EODHD=false
SKIP_FD=false
SKIP_FIDELITY=false

# ─────────────────────── Parse args ────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-eodhd)    SKIP_EODHD=true ;;
        --skip-fd)       SKIP_FD=true ;;
        --skip-fidelity) SKIP_FIDELITY=true ;;
        --compare)       DO_COMPARE=true ;;
        --owner-id)      OWNER_ID="$2"; shift ;;
        --help)
            tail -n +2 "$0" | sed '/^set /q' | grep '^#' | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
    shift
done

# ──────────────────────── .env loader ──────────────────────
# Walks up from SCRIPT_DIR to find the nearest .env; env vars take precedence.
_load_dotenv() {
    local dir="$SCRIPT_DIR"
    while [[ "$dir" != "/" ]]; do
        if [[ -f "$dir/.env" ]]; then
            while IFS='=' read -r key value; do
                [[ "$key" =~ ^[[:space:]]*# ]] && continue
                key="${key// /}"
                [[ -z "$key" ]] && continue
                value="${value#"${value%%[!\" ]*}"}"
                value="${value%"${value##*[!\" ]}"}"
                [[ -z "${!key+x}" ]] && export "$key"="$value"
            done < "$dir/.env"
            break
        fi
        dir="$(dirname "$dir")"
    done
}
_load_dotenv

# ──────────────────────── Print helpers ────────────────────
_banner() { echo; echo "══════════════════════════════════════════════"; echo "  $*"; echo "══════════════════════════════════════════════"; }
_info()   { echo "  ▶ $*"; }
_ok()     { echo "  ✓ $*"; }
_warn()   { echo "  ⚠ $*"; }
_fail()   { echo "  ✗ $*"; }

# ──────────────────────── Server health check ──────────────
_check_server() {
    if ! curl -sf "$BASE_URL/health" -o /dev/null; then
        echo ""
        echo "ERROR: Portfolio server is not reachable at $BASE_URL"
        echo "Start it with:  go run main.go"
        exit 1
    fi
    _ok "Server is up at $BASE_URL"
}

# ─────────────── Portfolio creation helpers ─────────────────

# Parse portfolio ID from a 201 response body (JSON with .portfolio.id)
_parse_id() {
    python3 -c "import json,sys; print(json.load(sys.stdin)['portfolio']['id'])" \
        2>/dev/null <<< "$1" || echo ""
}

# POST a JSON portfolio; outputs: numeric-ID | "conflict" | "error:CODE"
_create_json() {
    local name="$1" type="$2" objective="$3" memberships="$4"

    local body
    body=$(python3 - "$name" "$type" "$objective" "$OWNER_ID" "$memberships" <<'PY'
import json, sys
name, ptype, objective, owner_id, members_json = sys.argv[1:]
print(json.dumps({
    "name": name,
    "portfolio_type": ptype,
    "objective": objective,
    "owner_id": int(owner_id),
    "memberships": json.loads(members_json)
}))
PY
)

    local response http_code body_out
    response=$(curl -s -w "\n%{http_code}" \
        -X POST "$BASE_URL/portfolios" \
        -H "Content-Type: application/json" \
        -H "X-User-ID: $OWNER_ID" \
        -d "$body")
    http_code=$(echo "$response" | tail -1)
    body_out=$(echo "$response" | head -n -1)

    case "$http_code" in
        201) _parse_id "$body_out" ;;
        409) echo "conflict" ;;
        *)   echo "error:$http_code" ;;
    esac
}

# POST a multipart portfolio from a CSV file; outputs: numeric-ID | "conflict" | "error:CODE"
_create_csv() {
    local name="$1" type="$2" csv_path="$3"

    local metadata
    metadata=$(python3 - "$name" "$type" "$OWNER_ID" <<'PY'
import json, sys
name, ptype, owner_id = sys.argv[1:]
print(json.dumps({"name": name, "portfolio_type": ptype,
                  "objective": "Growth", "owner_id": int(owner_id)}))
PY
)

    local response http_code body_out
    response=$(curl -s -w "\n%{http_code}" \
        -X POST "$BASE_URL/portfolios" \
        -H "X-User-ID: $OWNER_ID" \
        -F "metadata=$metadata" \
        -F "memberships=@$csv_path")
    http_code=$(echo "$response" | tail -1)
    body_out=$(echo "$response" | head -n -1)

    case "$http_code" in
        201) _parse_id "$body_out" ;;
        409) echo "conflict" ;;
        *)   echo "error:$http_code" ;;
    esac
}

# Log the result of a portfolio creation call
_log_result() {
    local result="$1" name="$2"
    case "$result" in
        conflict)  _warn "'$name' already exists — skipped (idempotent)" ;;
        error:*)   _fail "'$name' failed: $result" ;;
        "")        _fail "'$name' returned empty ID" ;;
        *)         _ok   "'$name' created (ID=$result)" ;;
    esac
}

# ─────────────────────── Header ────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║         Portfolio Server Bootstrap           ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "  Server   : $BASE_URL"
echo "  Owner ID : $OWNER_ID"

_check_server

# ════════════════════════════════════════════════════════════
# Step 1 — EODHD Securities
# ════════════════════════════════════════════════════════════
_banner "Step 1/6 — EODHD Securities"
if [[ "$SKIP_EODHD" == "true" ]]; then
    _info "Skipped (--skip-eodhd)"
else
    bash "$SCRIPT_DIR/utils/eodhd_securities/load_all.sh"
fi

# ════════════════════════════════════════════════════════════
# Step 2 — FinancialData.net Securities
# ════════════════════════════════════════════════════════════
_banner "Step 2/6 — FinancialData.net Securities"
if [[ "$SKIP_FD" == "true" ]]; then
    _info "Skipped (--skip-fd)"
else
    bash "$SCRIPT_DIR/utils/fd_securities/load_securities.sh"
fi

# ════════════════════════════════════════════════════════════
# Step 3 — Prefetch US10Y Treasury Data
# ════════════════════════════════════════════════════════════
_banner "Step 3/6 — Prefetch US10Y Treasury Data"
TREASURY_START=$(date -d '3 years ago' '+%Y-%m-%d')
TREASURY_END=$(date '+%Y-%m-%d')
_info "Fetching US10Y from $TREASURY_START to $TREASURY_END ..."

treasury_response=$(curl -s -w "\n%{http_code}" \
    "$BASE_URL/admin/get_daily_prices?ticker=US10Y&start_date=${TREASURY_START}&end_date=${TREASURY_END}")
treasury_code=$(echo "$treasury_response" | tail -1)
treasury_body=$(echo "$treasury_response" | head -n -1)

if [[ "$treasury_code" == "200" ]]; then
    pts=$(python3 -c \
        "import json,sys; print(json.load(sys.stdin).get('data_points','?'))" \
        <<< "$treasury_body" 2>/dev/null || echo "?")
    _ok "US10Y fetched: $pts data points cached"
else
    _fail "US10Y fetch failed (HTTP $treasury_code): $treasury_body"
fi

# ════════════════════════════════════════════════════════════
# Step 4 — Sample Portfolios
# ════════════════════════════════════════════════════════════
_banner "Step 4/6 — Sample Portfolios (owner=$OWNER_ID)"

# These two IDs are captured on first creation; compare step uses them.
allie_ideal_id=""
allie_actual_id=""

_info "Ideal Allocation..."
r=$(_create_json "Ideal Allocation" "Ideal" "Growth" \
    '[{"ticker":"SPY","percentage_or_shares":0.40},{"ticker":"JPRE","percentage_or_shares":0.10},{"ticker":"HYGH","percentage_or_shares":0.10},{"ticker":"SPEM","percentage_or_shares":0.10},{"ticker":"SPDW","percentage_or_shares":0.10},{"ticker":"SPMD","percentage_or_shares":0.20}]')
_log_result "$r" "Ideal Allocation"

_info "Active Holdings..."
r=$(_create_json "Active Holdings" "Active" "Growth" \
    '[{"ticker":"SPY","percentage_or_shares":1000},{"ticker":"SPEM","percentage_or_shares":200},{"ticker":"NVDA","percentage_or_shares":20},{"ticker":"SPDW","percentage_or_shares":100}]')
_log_result "$r" "Active Holdings"

_info "Tech Heavy..."
r=$(_create_json "Tech Heavy" "Active" "Growth" \
    '[{"ticker":"NVDA","percentage_or_shares":50},{"ticker":"AAPL","percentage_or_shares":100},{"ticker":"MSFT","percentage_or_shares":75},{"ticker":"GOOGL","percentage_or_shares":30}]')
_log_result "$r" "Tech Heavy"

_info "Mag 7 (via MAGS)..."
r=$(_create_json "Mag 7 (via MAGS)" "Ideal" "Growth" \
    '[{"ticker":"MAGS","percentage_or_shares":1.0}]')
_log_result "$r" "Mag 7 (via MAGS)"

_info "Mag 7 (via direct)..."
r=$(_create_json "Mag 7 (via direct)" "Ideal" "Growth" \
    '[{"ticker":"AAPL","percentage_or_shares":0.142857},{"ticker":"AMZN","percentage_or_shares":0.142857},{"ticker":"GOOGL","percentage_or_shares":0.142857},{"ticker":"META","percentage_or_shares":0.142857},{"ticker":"MSFT","percentage_or_shares":0.142857},{"ticker":"NVDA","percentage_or_shares":0.142857},{"ticker":"TSLA","percentage_or_shares":0.142857}]')
_log_result "$r" "Mag 7 (via direct)"

_info "FAANG And Microsoft..."
r=$(_create_json "FAANG And Microsoft" "Ideal" "Growth" \
    '[{"ticker":"META","percentage_or_shares":0.166},{"ticker":"AAPL","percentage_or_shares":0.166},{"ticker":"AMZN","percentage_or_shares":0.166},{"ticker":"NFLX","percentage_or_shares":0.166},{"ticker":"GOOGL","percentage_or_shares":0.166},{"ticker":"MSFT","percentage_or_shares":0.17}]')
_log_result "$r" "FAANG And Microsoft"

_info "Allie Ideal..."
r=$(_create_json "Allie Ideal" "Ideal" "Growth" \
    '[{"ticker":"SPY","percentage_or_shares":0.55},{"ticker":"SPMD","percentage_or_shares":0.10},{"ticker":"SPSM","percentage_or_shares":0.05},{"ticker":"SPEM","percentage_or_shares":0.05},{"ticker":"SPDW","percentage_or_shares":0.10},{"ticker":"HYGH","percentage_or_shares":0.025},{"ticker":"IGIB","percentage_or_shares":0.025},{"ticker":"REZ","percentage_or_shares":0.05},{"ticker":"JPRE","percentage_or_shares":0.05}]')
_log_result "$r" "Allie Ideal"
[[ "$r" =~ ^[0-9]+$ ]] && allie_ideal_id="$r"

_info "Allie Actual (from tests/merged_clean.csv)..."
ALLIE_CSV="$SCRIPT_DIR/tests/merged_clean.csv"
if [[ ! -f "$ALLIE_CSV" ]]; then
    _fail "CSV not found: $ALLIE_CSV — skipping Allie Actual"
else
    r=$(_create_csv "Allie Actual" "Active" "$ALLIE_CSV")
    _log_result "$r" "Allie Actual"
    [[ "$r" =~ ^[0-9]+$ ]] && allie_actual_id="$r"
fi

# ════════════════════════════════════════════════════════════
# Step 5 — Fidelity ETF Holdings
# ════════════════════════════════════════════════════════════
_banner "Step 5/6 — Fidelity ETF Holdings"
if [[ "$SKIP_FIDELITY" == "true" ]]; then
    _info "Skipped (--skip-fidelity)"
else
    bash "$SCRIPT_DIR/utils/fidelity/load_all.sh"
fi

# ════════════════════════════════════════════════════════════
# Step 6 — Compare Allie's Portfolios (optional)
# ════════════════════════════════════════════════════════════
_banner "Step 6/6 — Compare Allie's Portfolios"

COMPARE_START=$(date -d '1 year ago' '+%Y-%m-%d')
COMPARE_END=$(date '+%Y-%m-%d')

if [[ "$DO_COMPARE" != "true" ]]; then
    _info "Skipped (pass --compare to enable)"
elif [[ -z "$allie_ideal_id" || -z "$allie_actual_id" ]]; then
    _warn "Cannot auto-compare: Allie portfolio IDs were not captured this run"
    _info "(Portfolios already existed — IDs are only returned on first creation)"
    echo ""
    echo "  Run the compare manually once you know the IDs:"
    echo ""
    echo "    curl -X POST $BASE_URL/portfolios/compare \\"
    echo "      -H \"Content-Type: application/json\" \\"
    echo "      -H \"X-User-ID: $OWNER_ID\" \\"
    echo "      -d '{\"portfolio_a\": ALLIE_IDEAL_ID, \"portfolio_b\": ALLIE_ACTUAL_ID,'"
    echo "           '\"start_period\": \"$COMPARE_START\", \"end_period\": \"$COMPARE_END\"}'"
    echo ""
else
    _info "Comparing 'Allie Ideal' (#$allie_ideal_id) vs 'Allie Actual' (#$allie_actual_id)"
    _info "Period: $COMPARE_START → $COMPARE_END"

    compare_body_json=$(python3 - \
        "$allie_ideal_id" "$allie_actual_id" "$COMPARE_START" "$COMPARE_END" <<'PY'
import json, sys
a, b, start, end = sys.argv[1:]
print(json.dumps({"portfolio_a": int(a), "portfolio_b": int(b),
                  "start_period": start, "end_period": end}))
PY
)

    compare_response=$(curl -s -w "\n%{http_code}" \
        -X POST "$BASE_URL/portfolios/compare" \
        -H "Content-Type: application/json" \
        -H "X-User-ID: $OWNER_ID" \
        -d "$compare_body_json")
    compare_code=$(echo "$compare_response" | tail -1)
    compare_body=$(echo "$compare_response" | head -n -1)

    if [[ "$compare_code" == "200" ]]; then
        python3 - "$compare_body" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
pm  = d.get("performance_metrics", {})
am  = pm.get("portfolio_a_metrics", {})
bm  = pm.get("portfolio_b_metrics", {})
a_name = d.get("portfolio_a", {}).get("name", "Allie Ideal")
b_name = d.get("portfolio_b", {}).get("name", "Allie Actual")
a_gain = am.get("gain_percent", 0)
b_gain = bm.get("gain_percent", 0)
print(f"    {a_name:<30}  gain: {a_gain*100:+.2f}%")
print(f"    {b_name:<30}  gain: {b_gain*100:+.2f}%")
PY
        _ok "Compare complete — price range data primed"
    else
        _fail "Compare failed (HTTP $compare_code): $compare_body"
    fi
fi

# ════════════════════════════════════════════════════════════
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║          Bootstrap complete!                 ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

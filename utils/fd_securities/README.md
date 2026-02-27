# FinancialData.net Bootstrap (`utils/fd_securities/`)

Fetches security symbols and ETF holdings from [FinancialData.net](https://financialdata.net)
for evaluation and bulk import into the portfolio server.

Follows the same pattern as `utils/eodhd_securities/` (symbol lists) and
`utils/fidelity/` (ETF holdings).

---

## Prerequisites

- Python 3.9+, no third-party packages required
- `FD_KEY` — your FinancialData.net API key
- `AUTH_TOKEN` — portfolio server bearer token (for upload scripts)
- `PORTFOLIO_URL` — server base URL (default: `http://localhost:8080`)

**Key resolution order**: environment variable → nearest `.env` file found by
walking up from the script's directory. The project root `.env` is picked up
automatically; `os.environ` always takes precedence.

---

## Step 1 — Fetch symbol lists

```bash
python3 fetch_symbols.py
```

Paginates all symbol endpoints and writes 5 CSVs to `securities/`:

| Output file           | Endpoint                        | Exchange placeholder |
|-----------------------|---------------------------------|----------------------|
| `fd_stocks.csv`       | `/stock-symbols`                | `FD_US`              |
| `fd_intl_stocks.csv`  | `/international-stock-symbols`  | `FD_INTL`            |
| `fd_etfs.csv`         | `/etf-symbols`                  | `FD_ETF`             |
| `fd_otc.csv`          | `/otc-symbols`                  | `FD_OTC`             |
| `fd_indexes.csv`      | `/index-symbols`                | `FD_IDX`             |

CSV columns: `Ticker,Name,Exchange,Type,Currency,Country,Isin`
(Currency, Country, Isin left empty — populate optionally with Step 2)

**Test without a paid subscription** — fetch only the first page (≤500 records)
per paginated endpoint:

```bash
python3 fetch_symbols.py --first-page-only
```

The index endpoint (`/index-symbols`) is not paginated and always fetches all records.

---

## Step 2 — Enrich with ISIN and exchange (optional)

```bash
python3 enrich_company_info.py securities/fd_stocks.csv [--limit N] [--delay SECONDS]
```

Calls `/company-information` per ticker to populate `Isin` and `Exchange`.
Rows with `Exchange=FD_INTL` automatically use `/international-company-information`
instead (different ISIN and exchange field names).
Writes output to `securities/fd_stocks_enriched.csv`.

- `--limit N` — enrich only the first N rows (useful for sampling before a full run)
- `--delay SECONDS` — pause between API calls (default: 0.2s)

```bash
# Sample first 5 rows to verify ISINs
python3 enrich_company_info.py securities/fd_stocks.csv --limit 5
```

---

## Step 3 — Fetch ETF holdings

```bash
python3 fetch_etf_holdings.py TICKER [TICKER ...]
```

Calls `/etf-holdings` per ticker and writes `etf_holdings/fd_{TICKER}_{YYYYMMDD}.csv`.

CSV columns: `Symbol,Company,Weight` — Weight is percentage (e.g., `7.83`),
matching the format expected by `POST /admin/load_etf_holdings`.
If the API returns decimal weights (≤1.0), the script multiplies by 100 automatically.

**Inspect raw JSON before committing** — FD documentation does not fully specify
the ETF holdings response fields:

```bash
# Print raw JSON for SPY, write nothing
python3 fetch_etf_holdings.py --dry-run SPY

# Fetch for real after confirming field names look right
python3 fetch_etf_holdings.py SPY QQQ IVV
```

---

## Step 4 — Upload securities to the server

```bash
AUTH_TOKEN=your_token ./load_securities.sh
```

Posts every `fd_*.csv` in `securities/` to `POST /admin/load_securities`.
Set `PORTFOLIO_URL` to target a non-local server.

---

## Step 5 — Upload ETF holdings to the server

```bash
AUTH_TOKEN=your_token ./load_etf_holdings.sh
```

Posts every `fd_*.csv` in `etf_holdings/` to `POST /admin/load_etf_holdings`,
extracting the ticker from the filename (`fd_{TICKER}_{YYYYMMDD}.csv`).

---

## Verification checklist

1. **Symbol fetch**: spot-check that `AAPL` appears in `fd_stocks.csv`, `SPY` in
   `fd_etfs.csv`, and `^GSPC` in `fd_indexes.csv`.
2. **ISIN enrichment**: confirm `Isin` column is populated after `--limit 5` run.
3. **ETF holdings dry-run**: inspect raw JSON to confirm field names and weight format.
4. **Load securities**: response JSON should show non-zero `inserted` and zero
   `skipped_bad_type` (FD_US/FD_INTL/FD_ETF/FD_OTC/FD_IDX exchanges are auto-created).
5. **Load ETF holdings**: query `dim_etf_membership` to confirm weights sum to ~1.0.

---

## Notes on exchange placeholders

FD-specific exchanges (`FD_US`, `FD_INTL`, `FD_ETF`, `FD_OTC`, `FD_IDX`) keep
evaluation data isolated from EODHD imports and avoid false deduplication.
The `load_securities` handler auto-creates unknown exchange codes.
Proper exchange mapping is out of scope for this bootstrap.

# Fundamentals Feature

Fundamentals are company/fund financial statistics fetched from EODHD and cached
in Postgres. They augment the price history already stored in `fact_price` with
point-in-time financial ratios, earnings history, and identity metadata.

---

## Tables

### `fact_fundamentals`

One row per security. Overwritten on every fetch — this is a snapshot, not a
time series. `last_update` tracks when the row was last refreshed.

Price-derived fields are **not stored here** — see Computed Fields below.

**Highlights** (quarterly cadence — income statement / per-share):
`peg_ratio`, `eps_ttm`, `revenue_ttm`, `ebitda`,
`profit_margin`, `operating_margin_ttm`, `return_on_assets_ttm`,
`return_on_equity_ttm`, `revenue_per_share_ttm`, `dividend_per_share`,
`quarterly_earnings_growth`, `quarterly_revenue_growth`,
`eps_estimate_current_year`, `eps_estimate_next_year`, `most_recent_quarter`

**Valuation** (requires balance sheet data not in `fact_price`):
`enterprise_value`, `price_book_mrq`, `ev_ebitda`, `ev_revenue`

**Short interest** (bi-monthly FINRA cadence):
`shares_short`, `short_percent`, `short_ratio`

**SharesStats** (slow-moving — buybacks / issuances):
`shares_outstanding`, `shares_float`, `percent_insiders`, `percent_institutions`

**Analyst Ratings** (raw vote counts):
`analyst_target_price`, `analyst_strong_buy`, `analyst_buy`, `analyst_hold`,
`analyst_sell`, `analyst_strong_sell`

### `fact_financials_history`

Time series of EPS results per security. PK is `(security_id, period_end, period_type)`.

- `period_type = 'Q'` — quarterly: `eps_actual`, `eps_estimate`, `eps_difference`,
  `surprise_percent`, `report_date`, `before_after_market`
- `period_type = 'A'` — annual: `eps_actual` only (all other columns are null by
  construction; EODHD does not return annual estimates or report metadata)

Annual rows go back to 1971 for some securities. Quarterly rows typically cover
10–15 years (median 50 quarters).

### `dim_security_listings`

Cross-exchange listing records for securities that trade on multiple exchanges.
PK is `(security_id, exchange_code)`. Used by the ETF holdings resolver to map a
foreign ticker (e.g. `0R2V` on `LSE`) to a canonical `dim_security` row.

### `dim_security` — new columns added by migration 002

Identity and classification fields populated from EODHD `General`:
`cik`, `cusip`, `lei`, `description`, `employees`, `country_iso`,
`fiscal_year_end`, `gic_sector`, `gic_group`, `gic_industry`, `gic_sub_industry`

ETF/fund-specific fields:
`etf_url`, `net_expense_ratio`, `total_assets`, `etf_yield`, `nav`

(`isin` and `url` already existed; `inception` is populated from `General.IPODate`.)

### Computed fields (not stored — derived from `fact_price` on demand)

These fields change with every price tick. Storing them produces a snapshot that
is stale by the next market open. All inputs are already in the DB.

| Field | Formula | Inputs |
|---|---|---|
| `week_52_high` | `MAX(fact_price.high) WHERE date > today−366` | fact_price |
| `week_52_low` | `MIN(fact_price.low) WHERE date > today−366` | fact_price |
| `ma_50` | `AVG(close)` last 50 trading days | fact_price |
| `ma_200` | `AVG(close)` last 200 trading days | fact_price |
| `beta` | `REGR_SLOPE(stock_returns, spy_returns)` trailing 252 days | fact_price + SPY (in DB) |
| `market_cap` | latest close × `shares_outstanding` | fact_price + snapshot |
| `pe_ratio` | latest close / `eps_ttm` (nil if eps_ttm ≤ 0) | fact_price + snapshot |
| `forward_pe` | latest close / `eps_estimate_current_year` (nil if estimate ≤ 0) | fact_price + snapshot |
| `price_sales_ttm` | `market_cap` / `revenue_ttm` | fact_price + snapshot |
| `dividend_yield` | `dividend_per_share` / latest close | fact_price + snapshot |

**Guard required for ratio fields:** `eps_ttm = 0` and `eps_estimate_current_year = 0`
are EODHD sentinels for "no value", not real zeros. Dividing by them produces
infinite PE. Treat any zero denominator as nil.

---

## Data Source and Flow

All fundamentals come from the EODHD `/api/fundamentals/{ticker}.{exchange}`
endpoint. The response is a large JSON object with named sections:
`General`, `Highlights`, `Valuation`, `Technicals`, `SharesStats`,
`AnalystRatings`, `Earnings`, `ETF_Data`, `MutualFund_Data`, `outstandingShares`.

**Parse flow:**

```
EODHD JSON
  → eodhd.rawFundamentals (internal struct, unexported)
  → providers.ParsedFundamentals (provider-neutral struct)
      ├─ dim_security update     via SecurityRepository.UpdateFundamentalsMeta
      ├─ fact_fundamentals        via FundamentalsRepository.UpsertFundamentals
      ├─ fact_financials_history  via FundamentalsRepository.UpsertFinancialsHistory
      └─ dim_security_listings    via FundamentalsRepository.UpsertSecurityListings
```

`ParsedFundamentals` is the handoff between the EODHD package and the repository
layer. The repository has no import of the EODHD package.

---

## Backfill Scheduling

`AdminService.SelectBackfillCandidates(ctx, n)` selects the top `n` securities
to fetch next. It does not call EODHD — it queries the DB and sorts in Go.

**Priority buckets** (lower = higher priority):

| Bucket | Condition |
|---|---|
| 0 — post-earnings stale | `next_earnings_announce` is in the past AND `last_update` predates it |
| 1 — never fetched | `last_update IS NULL` |
| 2 — oldest first | all others, sorted by `last_update ASC` |

**Within each bucket:** US securities first, then ETFs, then highest recent volume.
Volume is the last close volume from `fact_price`, fetched in a single batch query.

INDEX-type securities are excluded from candidates at the SQL level
(`WHERE ds.type != 'INDEX'`). EODHD returns 404 for index tickers like `^DJI`.

`RunBackfillFundamentals` runs the actual fetches concurrently, controlled by
`syncWorkers` (default 10, configurable via `CONCURRENCY` env var). Errors for
individual securities are non-fatal — the run logs them and continues.

---

## Design Decisions

### Analyst rating: raw counts only, no composite score

EODHD provides a pre-computed `Rating` field, but uses a **reversed scale**:
StrongBuy=5, StrongSell=1. The industry standard is StrongBuy=1, lower = more
bullish. Storing EODHD's Rating would silently produce wrong results for any
caller assuming the standard convention. We store the five raw vote counts only;
callers compute a consensus score at query time using the scale they require.

### Price-derived fields are computed, not stored

`market_cap`, `pe_ratio`, `forward_pe`, `price_sales_ttm`, `dividend_yield`,
`beta`, `week_52_high`, `week_52_low`, `ma_50`, `ma_200` were removed from
`fact_fundamentals`. These change with every price tick; storing them produces
a snapshot that is stale by the next market open. All can be computed on demand
from `fact_price` (see Computed Fields above).

`dividend_yield` is computed as `dividend_per_share / latest_close` rather than
from `fact_event` dividend history, because `fact_event` is only populated from
the date a security started being tracked — summing it would silently under-report
yield for recently-added securities.

**Zero-denominator guard:** `eps_ttm = 0` and `eps_estimate_current_year = 0`
are EODHD sentinels, not real zeros. Any code computing `pe_ratio` or
`forward_pe` must treat a zero denominator as nil.

### `percent_insiders` and `percent_institutions` are 0–100 scale

Despite the name "percent", EODHD returns these on a 0–100 scale (e.g. 8.5
means 8.5%), not the 0–1 fractional scale used by most other ratio fields in
this table. Do not divide by 100. Confirmed across 16K+ rows; no values exceed
100 for insiders, one pathological outlier (`AHFD`, `TCCPY`) for institutions
exceeds 200 and should be treated as NULL.

### `book_value_per_share` and `wall_street_target_price` were dropped

Both fields were defined in the original schema, but EODHD never populates them.
After confirming 100% null across 16K fetched securities they were removed from
`fact_fundamentals`, `migrations/002_fundamentals.sql`, `create_tables.sql`, and
all Go struct / repository code. `analyst_target_price` is the correct field for
price targets (81% populated for US stocks with analyst coverage).

### `fact_financials_history.shares_outstanding` was dropped

The `outstandingShares` EODHD section was included in the original design but
produces a sparse, duplicative dataset: 100% null across 631K rows. Outstanding
shares are already captured via `fact_fundamentals.shares_outstanding` (snapshot)
and are derivable from split-adjusted price history. The column was removed from
the table, migration, and Go code.

### Annual history rows carry only `eps_actual`

The `Earnings.Annual` EODHD section provides annual EPS figures going back
decades, but none of the other `fact_financials_history` columns (`eps_estimate`,
`eps_difference`, `surprise_percent`, `report_date`, `before_after_market`) are
populated for annual rows. This is a property of the data source, not a schema
gap. Annual rows are effectively a `(security_id, year, eps_actual)` time series.
A future migration could extract them into a dedicated `fact_annual_eps` table to
make the intent explicit.

### ETFs produce no stored data in `fact_fundamentals`

EODHD returns no income statement, valuation, or analyst data for ETFs.
Previously the five technicals (`beta`, `week_52_high/low`, `ma_50/ma_200`) were
the only populated fields for ETFs; those are now computed from `fact_price`
rather than stored. As a result, ETF rows in `fact_fundamentals` will have all
data columns null — the row exists only to record `last_update` and
`next_earnings_announce`. Do not query `fact_fundamentals` for ETF financial
ratios; use `dim_security` ETF-specific columns (`net_expense_ratio`,
`total_assets`, `etf_yield`) for ETF-level data.

### GIC classification is stock-only

`gic_sector`, `gic_group`, `gic_industry`, `gic_sub_industry` are 0% populated
for ETFs and funds. They are well-populated for common stocks (~87% after two
batches, with the gap mostly non-US securities that EODHD hasn't classified).
Queries spanning all security types must filter by type to avoid misleading null
rates.

### `dim_security_listings` is for cross-exchange resolution, not general use

This table is populated from the `General.Listings` section of each EODHD
fundamentals response. At 16K securities fetched, only 885 have any listing
records (5.5%). It is not a comprehensive cross-listing database — its purpose
is narrow: the ETF holdings resolver uses it to map foreign exchange tickers
back to a canonical `dim_security` row when a holding is listed under a
non-primary exchange code.

---

## Known Data Quality Issues (as of 2026-04-23)

See `notes/data_quality_results.md` for the full analysis. Summary of issues
requiring runtime guards:

| Issue | Scale | Guard |
|---|---|---|
| `eps_ttm = 0` sentinel | affects pe_ratio computation | Treat as nil denominator |
| `eps_estimate_current_year = 0` sentinel | affects forward_pe computation | Treat as nil denominator |
| `percent_institutions > 200` | 5 rows (e.g. TCCPY=75,630) | Treat as NULL |
| `short_percent > 1.0` | 7 rows; PMAX=119.8 | Cap at 1.0 or NULL |
| `profit_margin` outside [-1, 1] | 529 rows — legitimate | Document; do not clamp |

### Field reliability tiers for common stocks (stored fields only)

**Tier 1 — reliable (~99.7% populated):**
`revenue_ttm`, `profit_margin`, `operating_margin_ttm`, `ROA/ROE`,
`revenue_per_share_ttm`, `EV/EBITDA`, `EV/revenue`, `price_book_mrq`,
`shares_outstanding`, `shares_float`, `eps_estimate_current_year/next_year`,
`quarterly_earnings_growth/revenue_growth`.

**Tier 2 — reliable for US/large-cap, sparse for non-US/small-cap:**
`eps_ttm`, `ebitda`, GIC classification fields, `cik`, `cusip`.

**Tier 3 — use with caution:** `analyst_target_price` (54% null),
`analyst_*` counts (56% null), `short_percent` (42% null, some bad values),
`dividend_per_share` (65% null — correct for non-payers),
`peg_ratio` (50% null).

# Price Fetching

This document describes how the portfolio server fetches, caches, and serves historical price data for equities, ETFs, and US treasury rates.

---

## Overview

Price data is stored in two tables:

- **`fact_price`** — daily OHLCV rows, one per `(security_id, date)`.
- **`fact_event`** — daily corporate action rows (dividends, splits), one per `(security_id, date)`.
- **`fact_price_range`** — one row per security recording the cached date range and when the cache expires. This is the primary gate that prevents redundant provider calls.

There are two fetch paths: **singleton fetch** (one security at a time, on demand) and **bulk fetch** (the entire US exchange in one API call, triggered manually via an admin endpoint).

---

## Security Snapshot Cache

`SecurityRepository` holds an in-process snapshot of all `dim_security` rows in
two lookup maps (by ID and by ticker). The snapshot is an immutable `securitySnapshot`
value stored in an `atomic.Pointer`, so reads are lock-free and impose no mutex
contention on the hot path.

### Population

The snapshot is populated lazily on the first call to `GetAllSecurities`, but is
also warmed proactively at startup by `PrefetchService.runCatchup` before any
request is served (ahead of all price warm-up guards).

### TTL

The snapshot expires 1 hour after it was built (`securityCacheTTL`). On the next
`GetAllSecurities` call after expiry the maps are rebuilt from the database and a
fresh snapshot is stored.

### Invalidation

Write operations that mutate `dim_security` (`BulkCreateDimSecurities`,
`UpdateISINsForExisting`) call `ClearCache()` via `defer` before returning.
`ClearCache` stores `nil`, so the next `GetAllSecurities` call always rebuilds from
the database after a write — regardless of whether the TTL has elapsed.

### Fast-path coverage

When the snapshot is valid, these functions skip the database entirely:

| Function | Snapshot use |
|---|---|
| `GetAllSecurities` | returns `snap.byID`, `snap.byTicker` directly |
| `GetByID` | `snap.byID[id]` lookup |
| `GetByTicker` | `snap.byTicker[ticker]` → `PreferUSListing` |
| `GetMultipleByTickers` | per-ticker slice lookup from `snap.byTicker` |

---

## Singleton Fetch (`GetDailyPrices`)

`PricingService.GetDailyPrices(ctx, securityID, startDate, endDate)` is the standard entry point used by performance calculations, comparison, and glance views.

### Cache check

Before touching any provider, the service reads `fact_price_range` for the security:

```
GetPriceRange(securityID) → PriceRange{StartDate, EndDate, NextUpdate}
                          or nil (never fetched)
```

It then calls `DetermineFetch(priceRange, now, startDate, endDate)` which returns `(needsFetch bool, adjustedStart, adjustedEnd)`. The logic:

1. **No row in `fact_price_range`** → fetch the full requested range.
2. **Requested start is before `StartDate`** → fetch the historical gap (from `startDate` to the existing `StartDate`).
3. **Requested end is after `EndDate`** AND **`now` is after `NextUpdate`** → fetch the forward gap (from `EndDate` to `endDate`). Both conditions must be true: the data must be stale *and* the calendar day must have actually advanced past the cached end.
4. **Otherwise** → return cached data without any provider call.

The "both conditions" rule on point 3 prevents re-fetching on the same calendar day if `NextUpdate` has technically elapsed but no new market data exists yet.

The adjusted start/end returned by `DetermineFetch` close any gap between the incoming request and the cached range edges, preventing holes in `fact_price` from forming.

### Provider fetch (`fetchAndStore`)

`fetchAndStore` runs under a semaphore (`fetchSem`, default capacity 10) that caps concurrent provider connections globally. It calls the appropriate provider based on security type:

| Security | Provider | Notes |
|---|---|---|
| Money market fund | Synthetic | Generates `$1.00` close prices for each trading day; no network call |
| `US10Y` (10-year treasury) | FRED | `GetTreasuryRate(ctx, startDate, endDate)` — see [FRED section](#fred-treasury-data) |
| All other equities / ETFs | EODHD (or AV) | `GetDailyPrices(ctx, security, startDate, endDate)` |

After storing prices in `fact_price` and events in `fact_event`, it writes one row to `fact_price_range` via `UpsertPriceRange`, setting `NextUpdate` to the next expected data availability time (see [NextUpdate](#nextupdate-and-staleness)).

### Reading from cache

After any required fetch, the service queries `fact_price` and `fact_event` for the requested range and returns the results. The caller always sees data read from the database, never directly from the provider response.

---

## `fact_price_range`: Preventing Re-fetches

`fact_price_range` has one row per security with three meaningful columns:

| Column | Meaning |
|---|---|
| `start_date` | Earliest date stored in `fact_price` for this security |
| `end_date` | Latest date stored in `fact_price` for this security |
| `next_update` | Absolute earliest wall-clock time to speculatively re-fetch |

### Upsert semantics

Both `UpsertPriceRange` (single) and `BatchUpsertPriceRange` (bulk) use `LEAST`/`GREATEST` to expand rather than overwrite the range:

```sql
ON CONFLICT (security_id) DO UPDATE
SET start_date  = LEAST(fact_price_range.start_date, EXCLUDED.start_date),
    end_date    = GREATEST(fact_price_range.end_date, EXCLUDED.end_date),
    next_update = EXCLUDED.next_update
```

This means fetching an old historical range never shrinks the known `end_date`, and fetching recent data never forgets the historical `start_date`.

### `NextUpdate` and staleness

`NextUpdate` is set to the next time market data is expected to be available, computed at fetch time:

- **Equities/ETFs**: `NextMarketDate(fetchEndDate)` — returns 4:30 PM ET on the next trading day (skipping weekends and NYSE holidays).
- **US10Y**: `NextTreasuryUpdateDate(fetchEndDate)` — returns 4:30 PM ET on the next Monday–Thursday that is a trading day. Fridays are always skipped because FRED does not publish DGS10 on Fridays; that data appears on the following Monday.

Until `now >= NextUpdate`, `DetermineFetch` will not speculatively refetch the forward edge, even if the requested `endDate` is today. This prevents hammering the provider on every request during trading hours.

---

## Bulk Fetch (`BulkFetchPrices`)

`PricingService.BulkFetchPrices(ctx, exchange, date, secsByTicker)` fetches end-of-day data for the entire US exchange in three parallel API calls, then stores results for all known securities in two batch database operations.

This is only cost-effective for the `US` exchange. EODHD's bulk endpoint returns all US tickers in a single response; international exchanges would require individual per-ticker calls and offer no advantage over singleton fetch.

### Flow

```
Admin endpoint (GET /admin/bulk-fetch-eodhd-prices)
  │
  ├─ default date: LastMarketClose(now)   ← ET-aware, avoids UTC midnight rollover
  │
  ├─ secRepo.GetAllUS(ctx)               ← load all US securities into map[ticker]*Security
  │
  └─ PricingService.BulkFetchPrices(ctx, "US", date, secsByTicker)
       │
       ├─ [concurrent]
       │   ├─ GetBulkEOD(ctx, "US", date)       → []BulkEODRecord  (prices)
       │   └─ GetBulkEvents(ctx, "US", date)     → []BulkEventRecord (splits + dividends)
       │        ├─ [concurrent]
       │        │   ├─ GetBulkSplits(ctx, "US", date)
       │        │   └─ GetBulkDividends(ctx, "US", date)
       │        └─ merge on (ticker, date)
       │
       ├─ match EOD records against secsByTicker map (O(1) per ticker)
       │   skips tickers not in dim_security
       │
       ├─ StoreDailyPrices(ctx, prices)          ← pgx.Batch, fatal on error
       ├─ BatchUpsertPriceRange(ctx, ranges)     ← pgx.Batch, non-fatal on error
       └─ StoreDailyEvents(ctx, events)          ← pgx.Batch, non-fatal on error
```

`GetBulkEOD` and `GetBulkEvents` run concurrently via a `sync.WaitGroup`. Inside `GetBulkEvents`, `GetBulkSplits` and `GetBulkDividends` also run concurrently and are merged on `(ticker, date)` before being returned.

A failure in the EOD fetch is fatal (returns error). Event fetch failures are logged as warnings and do not abort the operation, since prices are more critical than same-day dividend/split data.

### Default date selection

When no `date` is supplied, the endpoint uses `LastMarketClose(time.Now())` rather than `time.Now().UTC()`. This matters in the evening: at 8 PM ET, UTC has already rolled to the next calendar day, so a naive UTC truncation would request a date with no data. `LastMarketClose` stays anchored to ET and returns the most recent 4:30 PM ET trading day close.

### Relation to singleton fetch

Bulk fetch writes to the same tables (`fact_price`, `fact_event`, `fact_price_range`) using the same upsert SQL as singleton fetch. After a successful bulk fetch, subsequent `GetDailyPrices` calls for any stored security will find a valid `fact_price_range` row with `EndDate = fetchDate` and `NextUpdate` set to the next trading day, and will return cached data without contacting any provider.

Bulk fetch does not use or check `fact_price_range` as a gate — it unconditionally writes whatever the provider returns. It is intended for nightly refresh of the entire exchange, not on-demand backfill.

---

## FRED Treasury Data

US 10-year treasury rates (`US10Y` security) are fetched from the [Federal Reserve Economic Data (FRED)](https://fred.stlouisfed.org/) API rather than EODHD or AlphaVantage.

### Endpoint

```
GET https://api.stlouisfed.org/fred/series/observations
    ?series_id=DGS10
    &api_key={key}
    &file_type=json
    &observation_start=2024-01-01
    &observation_end=2024-12-31
```

FRED returns one observation per calendar day in the range. Days with no published data (weekends, holidays) have `value: "."` and are skipped during parsing.

### Rate representation

FRED publishes DGS10 as a percentage (e.g., `4.52` means 4.52%). The value is stored as-is in `fact_price.close`. Callers that use this value as a decimal rate (e.g., performance calculations) divide by 100 at the point of use.

### Incremental fetching

Unlike AlphaVantage (which ignores date parameters and returns full history), FRED honours `observation_start` / `observation_end`. This means `fetchAndStore` can request only the missing date range when `DetermineFetch` identifies a gap or a stale forward edge, rather than re-downloading years of history on every update.

### Friday staleness

FRED publishes the DGS10 series Monday–Thursday. Friday's rate is not published until the following Monday at approximately 4:30 PM ET. `NextTreasuryUpdateDate` accounts for this by treating Friday as always-past-cutoff:

- Input on Thursday before 4:30 PM ET → `NextUpdate` = Thursday 4:30 PM ET (fetch tonight)
- Input on Thursday after 4:30 PM ET → `NextUpdate` = Monday 4:30 PM ET (skip Friday)
- Input on Friday (any time) → `NextUpdate` = Monday 4:30 PM ET

This means a Thursday-after-close fetch sets `NextUpdate` to Friday 4:30 PM ET, which is technically wrong (FRED won't publish Friday data). The practical consequence is that the Friday row will trigger one extra provider call on Friday evening that returns nothing new. This is accepted as a minor known limitation.

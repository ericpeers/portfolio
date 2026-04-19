-- Migration 002: Fundamentals tables
-- Run against live DB; safe to re-run (IF NOT EXISTS / IF NOT ALREADY EXISTS guards).
-- Prerequisites: dim_security table must already exist.

-- 1. Extend dim_security with identity and classification fields from EODHD General.
ALTER TABLE dim_security
    ADD COLUMN IF NOT EXISTS cik               varchar(20),
    ADD COLUMN IF NOT EXISTS cusip             varchar(12),
    ADD COLUMN IF NOT EXISTS lei               varchar(30),
    ADD COLUMN IF NOT EXISTS description       text,
    ADD COLUMN IF NOT EXISTS employees         integer,
    ADD COLUMN IF NOT EXISTS country_iso       char(2),
    ADD COLUMN IF NOT EXISTS fiscal_year_end   varchar(20),   -- e.g. "September", "December"
    ADD COLUMN IF NOT EXISTS gic_sector        varchar(100),
    ADD COLUMN IF NOT EXISTS gic_group         varchar(100),
    ADD COLUMN IF NOT EXISTS gic_industry      varchar(100),
    ADD COLUMN IF NOT EXISTS gic_sub_industry  varchar(100),
    ADD COLUMN IF NOT EXISTS etf_url           varchar,       -- ETF product page (ETF_Data.ETF_URL); NULL for stocks/funds
    ADD COLUMN IF NOT EXISTS net_expense_ratio float,         -- ETF_Data.NetExpenseRatio / MutualFund_Data.Expense_Ratio
    ADD COLUMN IF NOT EXISTS total_assets      bigint,        -- ETF_Data.TotalAssets / MutualFund_Data.Share_Class_Net_Assets in USD
    ADD COLUMN IF NOT EXISTS etf_yield         float,         -- ETF_Data.Yield / MutualFund_Data.Yield
    ADD COLUMN IF NOT EXISTS nav               float;         -- MutualFund_Data.Nav (mutual funds only; ETF NAV not in EODHD)

-- Notes:
--   inception already exists and will be populated from General.IPODate / ETF_Data.Inception_Date.
--   isin     already exists and will be populated from General.ISIN / ETF_Data.ISIN.
--   url      already exists and will be populated from General.WebURL (stocks) / ETF_Data.Company_URL (ETFs).

-- 2. Cross-exchange listing resolution.
-- When an ETF's holdings list returns a foreign ticker (e.g., 0R2V on LSE),
-- look it up here to resolve the canonical US listing (AAPL on NASDAQ).
CREATE TABLE IF NOT EXISTS dim_security_listings (
    security_id     bigint REFERENCES dim_security(id),
    exchange_code   varchar(20),   -- EODHD exchange code: "LSE", "NASDAQ", "SA", etc.
    ticker_code     varchar(30),   -- ticker on that exchange
    security_name   varchar(200),
    PRIMARY KEY (security_id, exchange_code)
);
CREATE INDEX IF NOT EXISTS idx_dsl_ticker_exchange ON dim_security_listings (ticker_code, exchange_code);

-- 3. Fundamentals snapshot — one row per security, overwritten on each fetch.
-- Scheduler uses next_update to drive backfill ordering.
-- next_update IS NULL means never fetched; worker picks these first via LEFT JOIN.
CREATE TABLE IF NOT EXISTS fact_fundamentals (
    security_id                 bigint PRIMARY KEY REFERENCES dim_security(id),
    last_update                 timestamptz,           -- NULL = never fetched; scheduling logic lives in Go
    next_earnings_announce      date,                  -- populated by earnings calendar job; NULL = unknown

    -- Highlights
    market_cap                  bigint,
    pe_ratio                    float,
    peg_ratio                   float,
    eps_ttm                     float,
    revenue_ttm                 bigint,
    ebitda                      bigint,
    profit_margin               float,
    operating_margin_ttm        float,
    return_on_assets_ttm        float,
    return_on_equity_ttm        float,
    revenue_per_share_ttm       float,
    book_value_per_share        float,
    dividend_yield              float,
    dividend_per_share          float,
    quarterly_earnings_growth   float,
    quarterly_revenue_growth    float,
    eps_estimate_current_year   float,
    eps_estimate_next_year      float,
    wall_street_target_price    float,
    most_recent_quarter         date,

    -- Valuation
    enterprise_value            bigint,
    forward_pe                  float,
    price_book_mrq              float,
    price_sales_ttm             float,
    ev_ebitda                   float,
    ev_revenue                  float,

    -- Technicals
    beta                        float,
    week_52_high                float,
    week_52_low                 float,
    ma_50                       float,
    ma_200                      float,
    shares_short                bigint,
    short_percent               float,
    short_ratio                 float,

    -- SharesStats
    shares_outstanding          bigint,
    shares_float                bigint,
    percent_insiders            float,
    percent_institutions        float,

    -- Analyst Ratings
    -- Raw vote counts only; compute the consensus rating at query time using the
    -- industry-standard scale (StrongBuy=1 … StrongSell=5, lower = more bullish).
    -- EODHD provides a pre-computed Rating field but uses the opposite scale
    -- (StrongBuy=5), so we drop it to avoid ambiguity.
    analyst_target_price        float,
    analyst_strong_buy          int,
    analyst_buy                 int,
    analyst_hold                int,
    analyst_sell                int,
    analyst_strong_sell         int
);

-- 4. Financial history — time series combining outstandingShares, Earnings.History/Annual,
-- and future Financials.* data into one table.
-- Quarterly rows align across sources (same calendar quarter-end dates).
-- Annual rows for non-December fiscal year companies (e.g. AAPL: Sep 30 vs Dec 31)
-- land at different period_end values — these become separate rows, which is correct.
CREATE TABLE IF NOT EXISTS fact_financials_history (
    security_id         bigint REFERENCES dim_security(id),
    period_end          date,
    period_type         char(1) NOT NULL,   -- 'A' annual, 'Q' quarterly

    -- From outstandingShares
    shares_outstanding  bigint,

    -- From Earnings.History / Earnings.Annual
    eps_actual          float,
    eps_estimate        float,
    eps_difference      float,
    surprise_percent    float,
    report_date         date,
    before_after_market varchar(15),        -- "BeforeMarket", "AfterMarket", or NULL

    PRIMARY KEY (security_id, period_end, period_type)
);

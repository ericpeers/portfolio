-- Copyright (C) 2025-2026, Eric Peers
-- All Rights Reserved
-- to lint this, use sqlfluff
-- myshell> sqlfluff lint create_tables.sql --dialect postgres
-- myshell> sqlfluff fix create_tables.sql --dialect postgres

-- assumption is this will go into a postgres DB called securities
-- createdb securities
drop index if exists idx_ds_ticker;
drop index if exists idx_dsl_ticker_exchange;
drop table if exists dim_exchanges cascade;
drop table if exists dim_security cascade;
drop table if exists dim_security_listings cascade;
drop table if exists dim_etf_membership cascade;
drop table if exists dim_etf_pull_range cascade;
drop table if exists dim_organization cascade;
drop table if exists dim_user cascade;

drop table if exists portfolio_membership cascade;
drop table if exists portfolio_glance;
drop table if exists portfolio cascade;
drop type if exists pf_type;
drop type if exists pf_objective;
drop type if exists ds_type;
drop type if exists user_role;

drop type if exists ffl_type;
drop table if exists fact_fetch_log;

drop table if exists fact_fundamentals;
drop table if exists fact_financials_history;
drop table if exists fact_price_range cascade;
drop table if exists fact_price;
drop table if exists fact_event;

drop table if exists app_hints;

create table dim_exchanges (
    id serial primary key,
    name varchar(80),
    country varchar(80)
);

insert into dim_exchanges (name, country) values
('BONDS/CASH/TREASURIES', 'Unkown'), -- generic holder for government bonds and treasuries
('NASDAQ', 'USA'),
('NYSE', 'USA');

create type ds_type as enum (
    'COMMON STOCK',    -- normal stock with voting rights
    'PREFERRED STOCK', -- better dividends stock, but no vote. Callable by company
    'BOND',  -- normally a loan to the US/other gvmt like US10Y which is the US 10 year treasury or "risk free rate"
    'ETC',    -- Exchange traded commodity like silver or gold
    'ETF',    -- group of stocks that don't incur taxes on redemption when buy/sell for individual - goes just to the person who sold. Tax advantaged
    'FUND',  -- Money markets, Mutual Funds : mixture of securities, where the fund buys/sells on redemption from larger pool. Tax disadvantaged
    'INDEX', -- Top X stocks in a certain exchange or country like the DOW
    'NOTES',   -- debt, traded as a grouping like an ETF. Also called ETN
    'UNIT',    -- mixture of stock and warrants
    'WARRANT', -- right to buy shares at a price, l ike an option
    'CURRENCY', -- like USD
    'COMMODITY', -- pigs and gold
    'OPTION' -- right to buy/sell in the future at a certain price.
);

create table dim_security (
    id bigserial primary key,
    ticker varchar(30), --NXT(EXP20091224) is potentially a bad security. Some securities are long like ASRV 8.45 06-30-28. Morocco has 30 character tickers. Sigh.
    name varchar(200),
    isin varchar(12), -- worldwide code to identify the stock.
    exchange serial references dim_exchanges (id),
    currency varchar(3),
    inception date,
    url varchar, --useful for holdings on mutual funds, etf, reit, index.
    type ds_type not null,
    delisted boolean not null default false,

    -- identity and classification fields populated from EODHD General (migration 004)
    cik varchar(20),
    cusip varchar(12),
    lei varchar(30),
    description text,
    employees integer,
    country_iso char(2),
    fiscal_year_end varchar(20),   -- e.g. "September", "December"
    gic_sector varchar(100),
    gic_group varchar(100),
    gic_industry varchar(100),
    gic_sub_industry varchar(100),

    -- ETF/fund fields populated from EODHD ETF_Data / MutualFund_Data (migration 004)
    etf_url varchar,       -- ETF product page (ETF_Data.ETF_URL); NULL for stocks/funds
    net_expense_ratio float,         -- ETF_Data.NetExpenseRatio / MutualFund_Data.Expense_Ratio
    total_assets bigint,        -- ETF_Data.TotalAssets / MutualFund_Data.Share_Class_Net_Assets in USD
    etf_yield float,         -- ETF_Data.Yield / MutualFund_Data.Yield
    nav float,         -- MutualFund_Data.Nav; ETF NAV not provided by EODHD

    constraint only_one_ticker_per_exchange unique (ticker, exchange)
);
create index idx_ds_ticker on dim_security (ticker);

-- FIXME. HACK XXX. Remove
-- This is a manually inserted test security for bootstrapping.
insert into dim_security (
    ticker, name, exchange, inception, url, type
) values

(
    'US DOLLAR',
    'US Dollar - aka cash',
    1,
    '1776-07-04',
    '',
    'CURRENCY'
),
(
    'US10Y',
    'US 10 Year Treasury',
    1,
    '1962-01-02',
    '',
    'BOND'
);

-- ETF's and indices are often very, very similar, thus I collapsed it.
-- we don't use percentage or shares because portfolios can have either one. These are always percentages
-- see also portfolio_membership

-- tracks when ETF membership data was fetched (one row per ETF, not per member)
create table dim_etf_pull_range (
    composite_id bigserial references dim_security (id),
    pull_date date,
    next_update timestamptz,
    primary key (composite_id)
);

create table dim_etf_membership (
    dim_security_id bigserial references dim_security (id), -- this is the member like NVDA
    dim_composite_id bigserial references dim_security (id), -- this is the parent like SPY
    percentage float,

    primary key (dim_security_id, dim_composite_id)
);

-- Cross-exchange listing resolution (migration 004).
-- When an ETF's holdings list returns a foreign ticker (e.g., 0R2V on LSE),
-- look it up here to resolve the canonical US listing (AAPL on NASDAQ).
create table dim_security_listings (
    security_id bigint references dim_security (id),
    exchange_code varchar(20),   -- EODHD exchange code: "LSE", "NASDAQ", "SA", etc.
    ticker_code varchar(30),   -- ticker on that exchange
    security_name varchar(200),
    primary key (security_id, exchange_code)
);
create index idx_dsl_ticker_exchange on dim_security_listings (ticker_code, exchange_code);

create table dim_organization (
    id serial primary key,
    name varchar(256) not null,
    created_at timestamptz default now()
);

-- USER     — standard account; can manage their own portfolios only
-- ORG_ADMIN — organization administrator; can move/reassign portfolios among members of their org
-- ADMIN     — system administrator; full access, approves new accounts
create type user_role as enum ('USER', 'ORG_ADMIN', 'ADMIN');

create table dim_user (
    id bigserial primary key,
    name varchar(256),
    email varchar(256) unique, -- https://www.rfc-editor.org/errata_search.php?rfc=3696
    passwd varchar(256), -- bcrypt hash
    join_date date,
    organization_id bigint references dim_organization (id),
    is_approved boolean not null default false,
    role user_role not null default 'USER',
    updated_at timestamptz default now()
);

-- Create a test user to bootstrap operations, and for testing.
insert into dim_user (name, email, join_date) values
('Eric Peers ADMIN', 'peers@mtnboy.net', now());

create type pf_objective as enum
('Aggressive Growth', 'Growth', 'Income Generation', 'Capital Preservation', 'Mixed Growth/Income');

-- Ideal portfolios use Percentages adding up to 100%
-- Active and Historic portfolios assume a share count per security.
create type pf_type as enum ('Ideal', 'Active', 'Historic');

-- these are like FACT tables since they may be edited. It's not a one time add.
-- they are also like DIM tables since they don't have a bunch of data.
create table portfolio (
    id bigserial primary key,
    portfolio_type pf_type,
    objective pf_objective not null,
    name varchar(80),
    comment text,   -- additional comments about this portfolio
    created_at date, -- date the portfolio was conceived/initially purchased. Used for returns
    snapshotted_at date, -- when were the portfolio holdings captured? Used for splits forward/backward.
    ended_at date, -- used for historic portfolios - where you might change them
    updated_at date,
    owner bigserial references dim_user (id)
);

create table portfolio_membership (
    portfolio_id bigserial references portfolio (id),
    security_id bigserial references dim_security (id),
    percentage_or_shares float,

    primary key (portfolio_id, security_id)
);

-- Simple list of portfolios that a user wants to see when on their home page
create table portfolio_glance (
    user_id bigserial references dim_user (id),
    portfolio_id bigserial references portfolio (id),

    primary key (user_id, portfolio_id)
);



-- Fundamentals snapshot — one row per security, overwritten on each fetch (migration 004).
-- next_update IS NULL means never fetched; worker picks these first via LEFT JOIN on dim_security.
create table fact_fundamentals (
    security_id bigint primary key references dim_security (id),
    last_update timestamptz,           -- NULL = never fetched; scheduling logic lives in Go
    next_earnings_announce date,                  -- populated by earnings calendar job; NULL = unknown

    -- Highlights
    market_cap bigint,
    pe_ratio float,
    peg_ratio float,
    eps_ttm float,
    revenue_ttm bigint,
    ebitda bigint,
    profit_margin float,
    operating_margin_ttm float,
    return_on_assets_ttm float,
    return_on_equity_ttm float,
    revenue_per_share_ttm float,
    book_value_per_share float,
    dividend_yield float,
    dividend_per_share float,
    quarterly_earnings_growth float,
    quarterly_revenue_growth float,
    eps_estimate_current_year float,
    eps_estimate_next_year float,
    wall_street_target_price float,
    most_recent_quarter date,

    -- Valuation
    enterprise_value bigint,
    forward_pe float,
    price_book_mrq float,
    price_sales_ttm float,
    ev_ebitda float,
    ev_revenue float,

    -- Technicals
    beta float,
    week_52_high float,
    week_52_low float,
    ma_50 float,
    ma_200 float,
    shares_short bigint,
    short_percent float,
    short_ratio float,

    -- SharesStats
    shares_outstanding bigint,
    shares_float bigint,
    percent_insiders float,
    percent_institutions float,

    -- Analyst Ratings
    -- Raw vote counts only; compute the consensus rating at query time using the
    -- industry-standard scale (StrongBuy=1 … StrongSell=5, lower = more bullish).
    -- EODHD provides a pre-computed Rating field but uses the opposite scale
    -- (StrongBuy=5), so we drop it to avoid ambiguity.
    analyst_target_price float,
    analyst_strong_buy int,
    analyst_buy int,
    analyst_hold int,
    analyst_sell int,
    analyst_strong_sell int
);

-- Financial history — time series combining outstandingShares, Earnings.History/Annual,
-- and future Financials.* data into one table (migration 004).
-- Quarterly rows align across sources (same calendar quarter-end dates).
-- Annual rows for non-December fiscal year companies land at different period_end values
-- (e.g. AAPL outstandingShares annual Dec 31 vs. EPS annual Sep 30) — separate rows, correct.
create table fact_financials_history (
    security_id bigint references dim_security (id),
    period_end date,
    period_type char(1) not null,   -- 'A' annual, 'Q' quarterly

    -- From outstandingShares
    shares_outstanding bigint,

    -- From Earnings.History / Earnings.Annual
    eps_actual float,
    eps_estimate float,
    eps_difference float,
    surprise_percent float,
    report_date date,
    before_after_market varchar(15),        -- "BeforeMarket", "AfterMarket", or NULL

    primary key (security_id, period_end, period_type)
);

-- cache table that tracks what pricing data we have in the bigger fact_price table
-- it is also used for fact_event table. Pricing data and event data is bundled in Alphavantage.
-- we might want to split this apart for other data providers.
--
-- it is possible that we have a startd/end that is bigger in range than the
-- fact_price table : this happens on holidays or weekends. It is also possible that
-- during the weekend you won't see any more updates. (or holiday weekend). 
-- E.g. ask for data from 1/1/2026 to 2/1/2026, on 2/1/2026. Stock data will have data
-- up to 1/30. It will never have data for 1/31/26 (Saturday) 2/1/26 (sunday).
-- Fed data will have data up to 1/29. 
-- Fed data will be released on 2/2/2026 (Monday) at 3:15pm, for 1/30/26. 
-- So for US stock markets, the next update is:
--    Closed, Business day, pre market: Current Day, 4:15PM
--    Open, Business day: Current Day, 4:15pm
--    Closed, Business day, 4-4:15pm: Current Day, 4:15pm
--    Closed, business day, 4:15pm onward: Next business day, 4:15pm
--    Closed, non business day: Next business day, 4:15pm
--    it is possible to simplify this (at the cost of holiday refetch-es) to if before 4:15pm on a business day, then wait. If after 4:15 or non business day, use next day.
--


create table fact_price_range (
    security_id bigserial references dim_security (id),
    start_date date,
    end_date date,
    next_update timestamptz,
    primary key (security_id)
);

create table fact_price (
    security_id bigserial references dim_security (id),
    date date,
    open float,
    high float,
    low float,
    close float,
    volume float,
    primary key (security_id, date)
);

-- this is a separate table to show Splits and Dividends
-- since that data is generally sparse (0 for dividend, and 1.0 for split), put it in another table
-- for quick lookup
create table fact_event (
    security_id bigserial references dim_security (id),
    date date,

    dividend float,
    split_coefficient float,
    primary key (security_id, date)
);

-- Application-level key/value hint store.
-- Used for persisting operational watermarks across restarts (last bulk fetch date,
-- last securities sync date, etc.). Values are stored as text; date hints use YYYY-MM-DD.
create table app_hints (
    key text primary key,
    value text,
    updated_at timestamptz not null default now()
);

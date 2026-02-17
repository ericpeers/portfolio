-- Copyright (C) 2025-2026, Eric Peers
-- All Rights Reserved
-- to lint this, use sqlfluff
-- myshell> sqlfluff lint create_tables.sql --dialect postgres
-- myshell> sqlfluff fix create_tables.sql --dialect postgres

-- assumption is this will go into a postgres DB called securities
-- createdb securities
drop table if exists dim_exchanges cascade;
drop table if exists dim_security cascade;
drop table if exists dim_stock_index cascade;
drop table if exists dim_stock_index_membership cascade;
drop table if exists dim_etf cascade;
drop table if exists dim_etf_membership cascade;
drop table if exists dim_etf_pull_range cascade;
drop table if exists dim_user cascade;

drop table if exists portfolio cascade;
drop table if exists portfolio_membership cascade;
drop type if exists PF_TYPE;
drop type if exists PF_OBJECTIVE;
drop type if exists DS_TYPE;

drop table if exists fact_price_range cascade;
drop table if exists fact_price;
drop table if exists fact_event;

create table dim_exchanges (
    id SERIAL primary key,
    name VARCHAR(80),
    country VARCHAR(80)
);
insert into dim_exchanges (name, country) values
('NASDAQ', 'USA'),
('NYSE', 'USA'),
('NYSE ARCA', 'USA'),
('NYSE MKT', 'USA'),
('AMEX', 'USA'),
('BATS', 'USA'),
('BONDS/CASH/TREASURIES', 'USA');
-- US cash markets, US 10Y treasury, money markets don't have a strict public exchange, so USBONDS is a synthetic substitute.
-- Add OTC LINK Markets? OTCPK (pink sheets), OTCQX OTCQB  


create type ds_type as enum (
    'stock',
    'mutual fund',
    'etf',
    'reit',
    'index',
    'bond',
    'money market',
    'currency',
    'commodity',
    'option'
);

create table dim_security (
    id BIGSERIAL primary key,
    ticker VARCHAR(10), --NXT(EXP20091224) is potentially a bad security. Some securities are long like ASRV 8.45 06-30-28
    name VARCHAR(80),
    exchange SERIAL references dim_exchanges (id),
    -- TODO FIXME. Consider adding sector
    -- sector VARCHAR(30),
    inception DATE,
    url VARCHAR, --useful for holdings on mutual funds, etf, reit, index.
    type DS_TYPE not null,
    constraint only_one_ticker_per_exchange unique (ticker, exchange)
);
-- FIXME. HACK XXX. Remove
-- This is a manually inserted test security for bootstrapping.
insert into dim_security (
    ticker, name, exchange, inception, url, type
) values
(
    'ZVZZT',
    'Nasdaq Test V',
    1,
    NOW(),
    'https://www.nasdaqtrader.com/micronews.aspx?id=era2016-3',
    'stock'
),
(
    'ZWZZT',
    'Nasdaq Test W',
    1,
    NOW(),
    'https://www.nasdaqtrader.com/micronews.aspx?id=era2016-3',
    'stock'
),
(
    'US10Y',
    'US 10 Year Treasury',
    7,
    '1962-01-02',
    'https://alphavantage.co/query?FUNCTION=TREASURY_YIELD&interval=daily&maturity=10year&datatype=csv&apikey=XXX',
    'money market'
),
(
    'US DOLLAR',
    'US Dollar - aka cash',
    7,
    '1776-07-04',
    '',
    'money market'
);

-- ETF's and indices are often very, very similar, thus I collapsed it.
-- we don't use percentage or shares because portfolios can have either one. These are always percentages
-- see also portfolio_membership

-- tracks when ETF membership data was fetched (one row per ETF, not per member)
create table dim_etf_pull_range (
    composite_id BIGSERIAL references dim_security (id),
    pull_date DATE,
    next_update TIMESTAMPTZ,
    primary key (composite_id)
);

create table dim_etf_membership (
    dim_security_id BIGSERIAL references dim_security (id), -- this is the member like NVDA
    dim_composite_id BIGSERIAL references dim_security (id), -- this is the parent like SPY
    percentage FLOAT,

    primary key (dim_security_id, dim_composite_id)
);

create table dim_user (
    id BIGSERIAL primary key,
    name VARCHAR(256),
    email VARCHAR(256), -- https://www.rfc-editor.org/errata_search.php?rfc=3696
    passwd VARCHAR(256), -- one way hash this.
    join_date DATE
);

-- FIXME - we probably want this inserted via API call, not manually injected here.
-- Also have a NULL passwd on purpose
-- Create a test user to bootstrap operations with testing.
insert into dim_user (name, email, join_date) values
('Test User', 'peers@mtnboy.net', NOW());

create type pf_objective as enum
('Aggressive Growth', 'Growth', 'Income Generation', 'Capital Preservation', 'Mixed Growth/Income');

-- Ideal portfolios use Percentages adding up to 100%
-- Active and Historic portfolios assume a share count per security.
create type pf_type as enum ('Ideal', 'Active', 'Historic');

-- these are like FACT tables since they may be edited. It's not a one time add.
-- they are also like DIM tables since they don't have a bunch of data.
create table portfolio (
    id BIGSERIAL primary key,
    portfolio_type PF_TYPE,
    objective PF_OBJECTIVE not null,
    name VARCHAR(80),
    comment TEXT,   -- additional comments about this portfolio
    created DATE,
    -- used for historic portfolios - where you might change them
    ended DATE,
    updated DATE,
    owner BIGSERIAL references dim_user (id)
);

create table portfolio_membership (
    portfolio_id BIGSERIAL references portfolio (id),
    security_id BIGSERIAL references dim_security (id),
    percentage_or_shares FLOAT,

    primary key (portfolio_id, security_id)
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
    security_id BIGSERIAL references dim_security (id),
    start_date DATE,
    end_date DATE,
    next_update TIMESTAMPTZ,
    primary key (security_id)
);

create table fact_price (
    security_id BIGSERIAL references dim_security (id),
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    primary key (security_id, date)
);

-- this is a separate table to show Splits and Dividends
-- since that data is generally sparse (0 for dividend, and 1.0 for split), put it in another table
-- for quick lookup
create table fact_event (
    security_id BIGSERIAL references dim_security (id),
    date DATE,

    dividend FLOAT,
    split_coefficient FLOAT,
    primary key (security_id, date)
);

-- Copyright (C) 2025, Eric Peers
-- All Rights Reserved

-- assumption is this will go into a postgres DB called securities
-- createdb securities
drop table if exists dim_exchanges cascade;
drop table if exists dim_security cascade;
drop table if exists dim_security_types cascade;
drop table if exists dim_stock_index cascade;
drop table if exists dim_stock_index_membership cascade;
drop table if exists dim_etf cascade;
drop table if exists dim_etf_membership cascade;
drop table if exists dim_user cascade;
drop table if exists dim_objective cascade;

drop table if exists portfolio cascade;
drop table if exists portfolio_membership cascade;
drop type if exists PF_TYPE;

drop table if exists fact_price_range cascade;
drop table if exists fact_price;

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
('BATS', 'USA');


create table dim_security_types (
    id SERIAL primary key,
    name VARCHAR(80)
);

insert into dim_security_types (name) values
('stock'),
('mutual fund'), --split to active/passive?
('etf'), --split to active/passive?
('reit'),
('index'),
('bond'),
('money market'),
('currency'),
('commodity'),
('option');

create table dim_security (
    id BIGSERIAL primary key,
    ticker VARCHAR(10),
    name VARCHAR(80),
    exchange SERIAL references dim_exchanges (id),
    -- TODO FIXME. Consider adding sector
    -- sector VARCHAR(30),
    inception DATE,
    url VARCHAR, --useful for holdings on mutual funds, etf, reit, index.
    type SERIAL references dim_security_types (id)
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
    1
),
(
    'ZWZZT',
    'Nasdaq Test W',
    1,
    NOW(),
    'https://www.nasdaqtrader.com/micronews.aspx?id=era2016-3',
    1
);

-- ETF's and indices are often very, very similar, thus I collapsed it.
-- we don't use percentage or shares because portfolios can have either one. These are always percentages
-- see also portfolio_membership
create table dim_etf_membership (
    dim_security_id BIGSERIAL references dim_security (id),
    --ETF, Mutual Fund, Index
    dim_composite_id BIGSERIAL references dim_security (id),
    percentage FLOAT,
    pull_date DATE,

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

create table dim_objective (
    id BIGSERIAL primary key,
    name VARCHAR(256)
);

insert into dim_objective (name) values
('Aggressive Growth'),
('Growth'),
('Income Generation'),
('Capital Preservation'),
('Mixed Growth/Income');


-- these are like FACT tables since they may be edited. It's not a one time add.
-- they are also like DIM tables since they don't have a bunch of data.

-- Ideal portfolios use Percentages adding up to 100%
-- Active and Historic portfolios assume a share count per security.
create type pf_type as enum ('Ideal', 'Active', 'Historic');

create table portfolio (
    id BIGSERIAL primary key,
    portfolio_type PF_TYPE,
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

-- cache table that tracks what pricing data we have in the bigger table
-- it is possible that we have a startd/end that is bigger in range than the
-- fact_price table : this happens on holidays or weekends.
create table fact_price_range (
    security_id BIGSERIAL references dim_security (id),
    start_date DATE,
    end_date DATE,
    primary key (security_id)
);

create table fact_price (
    security_id BIGSERIAL references dim_security (id),
    date DATE,
    open FLOAT,
    close FLOAT,
    primary key (security_id, date)
);

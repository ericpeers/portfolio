## Configuring Claude
 * Add //CLAUDE_DO_NOT_TOUCH and //CLAUDE_DO_NOT_TOUCH_END
 * Track subcommands being issued. Find out what it's doing under the covers - e.g. analyze my database schema

## More elegant error handling
* Better function headers. Describe what portion of instructions attempting to implement.
* Better error messages in failures

## Tests
* Move database to a clean copy - so that we don't end up polluting a prod db (also ensures we get clean ID #'s we can trusqt
* Prepopulate db with user data
* Explore adding unit tests
* Consider moving tests to sit next to the source code itself
* Add a test to prevent refactors from touching DO_NOT_TOUCH sections. If git diff shows the section to be touched, fail the test.
* Stress test to see how many RPS can be sustained.

## AV
* DONE: Prepopulate interesting stocks
  * can dl a list of stocks for nasdaq from: https://www.nasdaq.com/market-activity/stocks/screener
  * https://www.sec.gov/file/company-tickers
  * https://github.com/LondonMarket/Global-Stock-Symbols
  * https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo



## Bugs / Features

### P1 Bugs/Features
* Try additional screens/workflow for login, portfolio listings, comparison with Lovable
* Pull investor sentiment data on portfolio holdings. 
* Missing Dollar amounts in the React app for holdings breakdown. 
* Big Moved feature: Selecting a day in the performance graph could replace holdings breakdown and show big movers for that day (or week) inside of the portfolio including stock level and direct holdings. The idea is if you see a sharp decline,
or a sharp increase, get the attribution for that decline, and make it obvious.
* Implement dividends card
* Add a new card for downside volatility measurement like sharpe
* Implement Warning popover handling in the React App: models.Warning, models.WarningCode
* Simplify warning logic to just take a ctx and a string so that the metadata isn't passed. Is it really useful? It makes the code look dirty.

### P2 Bugs/Features
* ETF holdings fetches have lots of singletons and should (if in postgres) have all the relevant ID's already. Even if we persist to postgres, we should have all the id's. Should clean up getETFHoldings to return a the symbol + ID's. 
* Bad ETF's that get fetched will try to clean up as best as possible. E.g. 'MAGS' can normalize the SWAPS but can't handle several other weird non-securities. New normalized-the-best-we-can ETF is persisted to the db. First user gets error message. Second user never gets an error message. We need to persist this "error" to the db to indicate this is not a normal ETF. 

* Add the ability to backtest with substitution.
  * Needs a classifier which means we need lots of the fundamental data, and then scoring on multiple axis. Then we build a similarity table. How do we handle historic similarities? Do we pick it by year? 
  * Probably need dynamic portfolio adjustment. I'm thinking of this as a portfolio chain.
* Selectable cards in portfolio analyzer
* Swagger should be auto built ideally when building the app. Need to check if we have undocumented endpoints somehow.
* Comments are not handled for portfolio. Also need a test that checks for those fields. (scan the table, try all the fields for every CRUD endpoint?)
* OAuth2 implementation
  * Fix the README.md bug in Keycloak
  * Build a simple login + gin server that checks AUTH as a howto. User 1 cannot login to User 2's list of fruits.
  * Wire up OAUTH2 to Portfolio
    * User 1 cannot view User 2 session
    * Non admin users cannot access admin endpoints.
* 
* Imports have github.com/epeers. It seems like it's local. Read up why it is ok or not ok.
* This is wrong: 			newID, err := s.exchangeRepo.CreateExchange(ctx, entry.Exchange, "USA") - new exchanges are not always USA. Probably need to drop country? Maybe not? Maybe just retain as USA and fix if we add new countries later?
* integration_test.go defines getTestPool and admin_sync_test.go uses it without abstracting to a separate helper file. This means '''go test admin_sync_test.go''' fails.
* How do we handle a portfolio comparison with a security that IPO's in the middle of a time range?
  * We should either convert to cash
  * Equivalent security / replace it. 
  * Adjust the timeline? 

* Dim_objective is unused. Consider dumping it. Or change to a type and link into the dim_portfolio type
* Change dim_security_type to a type not a dim table.
* Add portfolio changes. Might be a snapshot? Might be a buy-sell? Maybe a ptr to current portfolio? Or do I link back?
* Create a daily cron job that pulls recent stock changes, possibly refresh ETF composition?
* add retry/backoff logic to AV if we are declined due to too many requests per minute.
  * Handle timeouts gracefully from the gin server. If I need to fetch more than 10 things from alphavantage, what do I send back?

* Accept CSV for portfolio creation
* Create a similarity table of stocks/ETF's to other stocks/ETF's. 
  * Cache major statistics so they don't need to be recomputed
  * Score based on Sector, Sharpe, Downmarket Sharpe, Volatility, 1/3/5Y gain, P/E, Market Cap. Then find 10 similar equities: 5 of the closest friends, and 5 long lived friends. 
  * This is a 12,000 x 12,000 matrix problem. 

### P3 Bugs/Features
* Add cacheing layer in memory. There is code, but it needs to be thought out.
* Add a symbols endpoint to fetch known symbols to SymbolID.  Consider bundling this data as part of the react app itself so it doesn't have to hit the API
* secure endpoints: 
  add event logging to capture interesting features/events
  * AV backoff failures
  * API calls

### Won't fix
* P1: Truncate dollar values to nearest cent. Truncate percentages to nearest 1000th of a percent.
  * Tried this. Seems risky in round trip which doesn't happen now. Concern I had was in portfolio allocation roundtrips.
  * Claude said don't bother after I did it. Minor savings in number of bytes. 
  * If I want to save bytes, enable gzip on the http response.
* P1: Add sharpe via mean of Rt-Rf or end case Rt-Rf. Consider computing both and returning both? Just use mean.


 ### Completed
* Ideal portfolio should have a start value of real dollars to compare dollars to dollars.
* Change size of exchange to 4 characters in @create_tables.sql:dim_security:exchange 
* Add .env file reading. @config.go ?
* add pricing table to create_tables.sql refactor existing claude generated logic in repository and alphavantage to utilize it. 
* needs to fetch historic data to present. Capture start data to end data. Need additional table to capture how much data we have?
* fetch list of stocks from Alphavantage and list stocks not present in db sync-securities, get_etf_holdings, get_daily_prices
* Add go-swagger to document the API. 
* Fix the sharpe calculation logic : correct the daily interest compounding formula
* Refactor portfolio composition to include attribution based on ETF or direct holding
* Accept symbols or ID's for portfolio creation in JSON.
* Accept CSV for portfolio creation. Use a multipart approach (JSON for portfolio record, CSV for membership)
* Compare portfolio holdings is incorrect for A% values. 
* All percentages seem to be multiplied by 100 by react. Serverside should take only values less than or equal to 1.0 for ideal portfolios. 
* Use Portfolio name as label in performance cards. 
* Stock level holdings do not show direct holdings in the app.
* Why am I missing risk free data for when I have stock data? 11/11/25 and 10/13/25. Veteran's and Columbus day. Bond closed. Stock open. Previously used average value. Should I instead average before/after and use that?
* Select area is inconsistent for zooming in. 
* Pulling MAGS ETF holdings has a bunch of symbols not supported including FGXXX and SWAP. Should I re-round to 100% after this? Maybe not, because cash holdings are not the same as Leveraged securities like total return swap.
  * Maybe pass through a message to the user as a warning and surface that in the UI? 


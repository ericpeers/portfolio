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

## Bugs / Features

### P1 Bugs/Features
* HEIA in allie portfolio merged_clean.csv maps to HEI-A in db. Do I have a good SPAXX datafeed?

* Try different data sources outside of Alphavantage
  * Vanguard has ETF data: https://investor.vanguard.com/vmf/api/0964/portfolio-holding/pcf.json

* -F stocks probably are referring to overseas stocks, not in US exchanges. It's an OTC code for "Foreign". We are dropping the -F and it sometimes resolves incorrectly: BH-F resolves to BHF but is actually TH0168A10Z19 / Bumrungrad Hospital PCL in thailand. SPEM and VWO both have this problem.


* Mutual funds are treated like ETF's in many areas, but we don't have data for them  
  * May need to purge mutual fund treament unless we can decompose. Search for: string(models.SecurityTypeMutualFund)

* Missing India, HK and Colombia stock exchanges. Causes issues for AVEM. 

* improve performance of compare endpoint
  * performance_plan.md
    *  Added annotation for GetPortfolio. It's plenty fast enough
  * bulk fetch is taking 250ms of all securities with country. 100-125ms to fetch from DB. Can we drop URL from the fetch? ISIN? Where is the other 100ms? Go marshalling?
  * Compute Membership took 1067ms for Allie's portfolio comparison on the actual.
    * Now 439ms on 2/16. Previously 250ms. Still can be improved.
  * Reduce the response size itself by using shortening json identifier fields
  * purge inception date check in GetDailyPrices?. Or pass the map of securities by ID into this and not look it up again?
  * comparison_service.go:ComputeDailyValues calls GetDailyPrices for each security. Why not fetch all of them all at once?
    * Check all the ranges. Whatever ranges I don't have, go fetch from AV. Then grab the data from postgres.

* on creating portfolios, if there is a collision, we should prompt the user, and then also allow for specifying the exchange somehow. 
  * need to handle on server side, esp for CSV
  * need to handle on client side for receiving errors
  * ideally, have a list of securities on clientside as well. 
  * check out portfolio_service.go:90 - this only allows US holdings?

* need to resolve tickers since we can have multiples tickers across various exchanges.
  * pick ticker.exchange?

* We still are missing daily price data in some cases. VOO fetched to 2-13, but not on 2-17. Portfolio value returns 0 for the day.
  * Some updates happen at 5:30pm EST. Adjust fetch date to move out to there. There may also be sharding problems with data providers. 
  * Need to check size of arrays/end date in React and limit window if it is missing a day.
  * Pass a Warning message that we are missing data. If we are close to the EOB (within 2 hours) and the ETF hasn't updated, consider setting next update to 30 minutes from now?
  * JPRE, HYGH failed later in the evening. Persistently failed to return today's data at 9-9:30pm. There was an outage prior to this. 

* Don't allow an end date of TODAY if we don't have data for TODAY. Both in UI and in Service
  * If the data is incomplete for TODAY (E.g. late market update), WARN and truncate both datasets.
  * Set the client to look for 0'd out data as well. 

* Support ADR (American depository receipt) for foreign stocks ending in "Y" for OTC trading. Also support same named company for NYSE listed stocks like TSM.ARCA => 2330.TW
* Is this useful to anybody else? 

* Finish UI in mock mode. 
   * Clean up table colors
   * Add nice rings to mimic Lovable UI

* Add add-portfolio to UX.
* Add Edit-portfolio to UX
* Add Delete portfolio to UX

* At-A-Glance implementation
  * Determine where to store the portfolios of interest. 
  * Generate endpoint to compute performance of portfolios

* Try additional screens/workflow for login, portfolio listings, comparison with Lovable
  * A porfolio specific reporting screen would be useful to show stats on individual holdings in a table format. 

* Pull investor sentiment data on portfolio holdings. 

* Add Dollar amounts in the React app for holdings breakdown.
  * Tie it to the day in question - move slider on graph, show holdings values on that day. 

* Big Moved feature: Selecting a day in the performance graph could replace holdings breakdown and show big movers for that day (or week) inside of the portfolio including stock level and direct holdings. 
The idea is if you see a sharp decline, or a sharp increase, get the attribution for that decline, and make it obvious.

* Implement dividends card

* Add a new card for downside volatility measurement like sharpe

  

### P2 Bugs/Features
* Dialog description sits at the top of the pages, but it is not terribly useful to people with sight and takes up room. This is for auditory screen readers. Can we make it invisible somehow? deleting causes typescript errors and other problems in the test suite. 

* pricing_service.go:getdailyprices stores in SQL and then fetches right after the store. Why not return what I just stored?
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
* This is wrong: 			newID, err := s.exchangeRepo.CreateExchange(ctx, entry.Exchange, "USA") - new exchanges are not always USA. Probably need to drop country? Maybe not? Maybe just retain as USA and fix if we add new countries later?
* integration_test.go defines getTestPool and admin_sync_test.go uses it without abstracting to a separate helper file. This means '''go test admin_sync_test.go''' fails.
* How do we handle a portfolio comparison with a security that IPO's in the middle of a time range?
  * We should either convert to cash
  * Equivalent security / replace it. 
  * Adjust the timeline? 

* Add portfolio changes. Might be a snapshot? Might be a buy-sell? Maybe a ptr to current portfolio? Or do I link back?
* Create a daily cron job that pulls recent stock changes, possibly refresh ETF composition?
* add retry/backoff logic to AV if we are declined due to too many requests per minute.
  * Handle timeouts gracefully from the gin server. If I need to fetch more than 10 things from alphavantage, what do I send back?

* Create a similarity table of stocks/ETF's to other stocks/ETF's. 
  * Cache major statistics so they don't need to be recomputed
  * Score based on Sector, Sharpe, Downmarket Sharpe, Volatility, 1/3/5Y gain, P/E, Market Cap. Then find 10 similar equities: 5 of the closest friends, and 5 long lived friends. 
  * This is a 12,000 x 12,000 matrix problem. 

* Alphavantage: Why are we skipping multiple securities on insertion? No errors for them. Count the ones I skip too and add to the list.

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
* P1: Renamed to sync-securities-from-av to deprecate. admin_service fetches list of securities and overwrites/inserts irrespective if it exists already. Not desireable from Alphavantage. 
  * Should I formalize the insertion logic in utils from eodhd? (DONE)
  * Should we have a linting mode instead? Look for what's different, and then print that out? And then just do fixup on fields if it exists and AV has supplemental data.


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
* Fix up interesting_sandbox_test fixes I (not AI) made
* Data popover for chart needs year if the range is greater than a year. The bubbles also occasionally pick a different year even though the cursor is to the right. 
* Simplify warning logic to just take a ctx and a string so that the metadata isn't passed. Is it really useful? It makes the code look dirty.
* Add year to the bottom X axis on the compare chart
* Imports have github.com/epeers. It seems like it's local. Read up why it is ok or not ok.
   As you can see, the very first line of your go.mod file is:
```
   1 module github.com/epeers/portfolio
```
  This line is the key. It declares the module path for your entire project.

  When you import a package, Go's tooling checks if the import path starts with this module path. In your case, the import github.com/epeers/portfolio/internal/alphavantage does.

  Because it's a match, Go knows this isn't a third-party package to be downloaded from the internet. Instead, it resolves that path to your local file system, looking for a directory at
  ./internal/alphavantage relative to your go.mod file.

  So, even though the path looks like a URL, it's used as an identifier for your local code. This is the standard way Go handles intra-project imports, and it means that if you do decide to git
  push and make your project public, all your imports will work without any changes.
* Lots of fails in code review from Claude reviewing Gemini. 8/10 legit. How does Gemini pay attention to CLAUDE.md? ~/.gemini/settings.json : "contextFileName": "CLAUDE.md"
* We are fetching the same TIME_SERIES_DAILY for a symbol we just fetched it for. When we go forward a day....
* We are also fetching the same TREASURY_YIELD twice in a single compare. : pricingservice.go:150 - always fetch it. This may be right for Securities, but we don't have new data for Treasury data. I think the real fix is to check
  whether we are in the next day. The original fix was to prevent truncation on the second portfolio fetch. 
* Why does plan -> edit mode lose context in Claude? It clears screen too so we lose the plan. Item 10 was completely lost. Couldn't recover it. Wrote python scripts to try to recover it. INTENTIONAL - this is because you select "Clear context and accept edits"
* DONE: Prepopulate interesting stocks
  * can dl a list of stocks for nasdaq from: https://www.nasdaq.com/market-activity/stocks/screener
  * https://www.sec.gov/file/company-tickers
  * https://github.com/LondonMarket/Global-Stock-Symbols
  * https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo
* Change dim_security_type to a type not a dim table.
* Loadtest wrt to AV. What happens when I hit my API limit? Do I gracefully retry in my app? It issues a 200 and "Burst pattern detected"
* Dim_objective is unused. Consider dumping it. Or change to a type and link into the dim_portfolio type (Changed to TYPE)
* Graphing is dropping Jun 7, 2024 and showing data for Jun 9 2024 instead. 
* From June 6 to June 9, 2024, I suspect there is a stock split. Mags 7 vs Mags 7 Direct show a major drop. 
  * NVDA split on 06-07-2024. Need to handle splits. It was a 1:10 split. 
* Implement Warning popover handling in the React App: models.Warning, models.WarningCode
* Fix tests that don't have treasury data:
* Treasury Data ALWAYS returns CSV, and always returns the full set. Trim the option for COMPACT.
* pricing_service has needsFetch in GetDailyPrices. Move to a separate routine for readibility.
* Rename Price_cache_repo to Price_repo.go
* Fix the enumerated type check for ETF or Mutual Fund to be an enum not a hardcoded value.
* DailyPrices is choosing JSON for large time ranges. We ought to use CSV always instead. 
* Multi-ticker resolution across exchanges for ETF's: pick overseas for Developed/emerging markets vs. US. 
* isETForMutualFund should be rewritten to assume the db element is already fetched, and then simply look at the enums. Rather than re-fetching the item. 
* admin/loadFidelity/Holdings: 
   * Rewrite as admin/loadETFHoldingsCSV
   * fetch all Securities by ID/Symbol at the top, not at the bottom
* Need a solution to pull in ETF's from CSV. Possibly also pull in securities from CSV. 
* Can I purge GetQuote/CacheQuote? YES. Purged.
* FUND is probably the same as MUTUAL FUND (see eodhd discussion)
* Formalize insertion logic from EODHD - done. Now in load_securities, accepts CSV.
* Change ETF next-update logic to defer for a month. It doesn't update that frequently. 
* Add Allie portfolio : Deal with failures
  * HEIA: Heico Class A, follows HEI at a discount. Not on massive.
  * OTC Stocks on massive: SIEGY, HTHIY, RNMBY, BNPQY, UCBJY, RYCEY, ALIZY, DHLGY, UNCRY, CFRUY
  * Private fund: FZAEX (fidelity - closed)
  * Fidelity Money Market: SPAXX (hard to find. Maybe in NASDAQ feed for mutual funds?)

## P1 Bugs/Features

### gin-gonic 
* need versioning on the app. 
* Automigration to upgrade db: notes/automigration.md : Do we really want to balloon size of binary with embedded sql? 
* We fetch a bunch of price data that we don't use. Would memory and db memory be lower if we didn't fetch that? (open high low close volume)

* discontinuinty on June 1, 2023 on compare portfolios
* logging is DEBU or "ERRO" - can we get the full string? 
* FRED data should fetch in the prefetch loop if we don't have it for that day
* Fix Fred fetch times to a variable, not hardcoded number. trading_calendar.go:380
* explore index data - can we backfill that given the new EODHD subscription?

* glance is taking too long at 2.88s. Can it be faster? takes 45 seconds on production.
* symlink test/.env to ROOT/.env - this is not checked in. Should it be? Makes running tests easier.

* Substitution: Remove value and rebalance portfolio as if it didn't exist. Does overlay work for this? 
* Substitution: Like kind security - simplify to begin - just use SPY.   
  * Add these new substitutions to pre_ipo_compare_test.go

* Fundamental data: 
  * slow backfill. 
  * update logic for recent earnings to re-fetch day after earnings
  * import/export with CSV
  * what happens when earnings date gets adjusted? This seems to track weekly, but that is expensive across 150k earnings dates. We should run weekly and see who is about to update, and then go query them.
  * earnings calendar scheduler
  * fundamental data scheduler
  * handle NAV -   nav is defined in dim_security but never written
  * reevaluate (see notes) data quality and purge unneeded rows in tables.
  
* Ouath2
* MIGHT not be able to do this: inception dates not complete - revert the check code/refactor to use a data_coverage.go : it has to go run a bunch of min's for securities without inception dates. (after we have inceptions)
* profile concurrency changes on AWS - do lower thread counts result in lower latency? portfolio-infra needs to be CONCURRENCY, CONCURRENCY_HTTP, CONCURRENCY_DB. 

* lots of errors in comparing fidelity everything: Older data missing, some stocks missing. 
* Portfolio substitution - backtesting - cash sub
* Portfolio substitution - backtesting - like kind

* Add tax advising for selling
* Securities that are similar logic: to be used for substitution of securities
* Add Index data: scrape from fidelity? 
* minPricesForFullFetch is 30,000 (below the 40k–48k per-day bulk count, above any individual security fetch). Come up with a better heuristic or make the threshold dynamic.
  * Add a test back in for checking the fact_fetch_log table

* IPO Dates
  * https://finance.yahoo.com/calendar/ipo/?from=2025-03-08&to=2025-03-14&day=2025-03-12&err=1
  * https://site.warrington.ufl.edu/ritter/files/IPO-age.xlsx
  * https://stockanalysis.com/ipos/2019/
  * https://www.otcmarkets.com/stock/AAUKF/security   : This is interesting because it also shows symbol changes.
  * https://www.otcmarkets.com/stock/AAUKF/profile : Profile Data - joined OTCQX, also company notes: Spnsored by BNY Mellon for OTCQX on November 20, 2017
  * https://iposcoop.com
  * IPO/inception date data source. Pay more to get it for a month and add?
  * Don't update Treasury date of inception

* Pull investor sentiment data on portfolio holdings. 
* find JUNK stocks / bad stocks / bad ETF's: 
  * "non-diversified" or "non diversified" text suggests poor etf's that have high exposure to one core holding+derivatives
  * fulltime employees = 4 - this is bad. KKUR
  * missing ipo date
  * profit_margin < 1.0 in fundamentals - losing a bunch of money. >

### UI
* when I go to a page that has quiesced for a while, it re-submits the backend request. Seems unnecessary. 
* portfolio substitution - select what replacement strategy you want in UI
* test coverage in UI
* run impeccable style on UI: https://impeccable.style

* Mock an advisor workflow - to build a portfolio. This is the "interview" to find what the person wants, and then recommend portfolios to them.
  * Desired outcomes
    * Volatility
    * Gains
    * Spend: Now, and minimum spend
    * Cashflow
  * Assets
    * Tax free accounts (IRA/401k/Roth/HSA)
    * Real estate: primary, secondary, commercial
    * Alts
    * Other income, now and later
    * SSN expected benefits
    * Inheritance
    * Reverse Mortgage
  * Debts
    * Mortgage
    * College
    * Parent care
    * Your own care
  * Secondaries:
    * Willing to move?

* Add another report
* Implement actual reportgen behind the scenes - no mock. 
* Add discussion to reportgen for the last 3 pages
* Report: Jen feedback
  * use black and red instead of green and red. Colorblind problem
  * Use bigger text
  * Use smaller words. Fewer datapoints. Dumb it down.


* Try additional screens/workflow for login, individual portfolio report
  * A porfolio specific reporting screen would be useful to show stats on individual holdings in a table format. 


### Code Cleanup
  * Functions over 500 lines should be refactored for readability : ComputeDailyValues needs a cleanup
  * move to a fresh/test database rather than running on prod data. Deleting from fact_fetch_log was bad. 
  * prefetch_Service.go has StartNightly calling runNightly. How many other single layer calls do we have that are not necessary? I've seen this across service layers.
  * before creating a test security, check that it does not exist. We don't want to overwrite and then delete real security data. Instead, maybe we should make them a bit more unique?
  * Add AJNMY back into our mix from utils/fidelity/convert_fidelity.py
  * Improve code coverage again
  * pickETFSecurity has US preference to fix bug where we cached under mexico when using admin endpoints. Make sure we use the same path for preference. 
  * GetAllUS calls could use the new cache added to security_repo.go. Admin endpoints could too?
    * Name matching for etf resolution excludes PLC. (but I don't think it does) and 2 characters or less. Those might actually be useful. 
  * fixed on 3/27, but prompt gemini with this: Look for comments in the code that are stale or do not match the behavior of the function it comments. Also look for comments before code that might be stale. Create a list of these with            
   file:line:problem where problem is a 80 character or less description of why it mismatches. Put the list in "function_mismatch.txt"
  
* if I don't have historic data, the portfolio initial values diverge and should not. 
* forward filling securities on an "overachiever day" where there is only 1-2 pieces of data out of 100 securities should invert the algorithm. 
* do I need to fetch 5-7 days ahead for normal range fetches such that I always have extra data for filling no-volume days?
* STALE? I get double fetches (overlaid) when I have a new day past end date, and a new start date. E.g. cache is [1/1/25:3/3/26]. Now fetching [1/1/24:3/4/26]. It has to do a end portfolio computation and then a start date fill. 

  
### Other stuff 
* Do I have a good SPAXX datafeed?
  * Not really. It had limited data. Moved to a synthetic approach, but need rates like US10Y. 

* Handle ETF composition correctly
  * Vanguard has ETF data: 
    * List: https://investor.vanguard.com/investment-products/list/etfs?filters=open
    * Porfolio composition file data: https://investor.vanguard.com/vmf/api/0964/portfolio-holding/pcf.json
  * Full list? https://www.dtcc.com/charts/exchange-traded-funds
  * NYSE list: https://www.nyse.com/listings_directory/etf/
  * Nasdaq list: https://www.nasdaqtrader.com/trader.aspx?id=etf_definitions
  * Each ETF provider required by law to disclose daily holdings on their website: https://www.sec.gov/about/divisions-offices/division-investment-management/accounting-disclosure-information/adi-2025-15-website-posting-requirements#:~:text=Daily%20Holdings.,national%20best%20offer.%5B30%5D  
  * List of stocks on intl exchanges: https://www.reddit.com/r/algotrading/comments/1r9zzki/lists_of_all_companies_listed_on_a_few_exchanges/
  * Schwab: https://www.schwabassetmanagement.com/sites/g/files/eyrktu361/files/product_files/SCHF/SCHF_FundHoldings_2026-03-20.CSV

* Frontcast ETF % holdings. Adjust from last date of sample. 

* Mutual funds are treated like ETF's in many areas, but we don't have data for them  
  * May need to purge mutual fund treament unless we can decompose. Search for: string(models.SecurityTypeMutualFund)

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


* Support substitution of ADR (American depository receipt) for foreign stocks ending in "Y" for OTC trading. Also support same named company for NYSE listed stocks like TSM.ARCA => 2330.TW

* Is this useful to anybody else? 


* Add Dollar amounts in the React app for holdings breakdown.
  * Tie it to the day in question - move slider on graph, show holdings values on that day. 

* Big Moved feature: Selecting a day in the performance graph could replace holdings breakdown and show big movers for that day (or week) inside of the portfolio including stock level and direct holdings. 
The idea is if you see a sharp decline, or a sharp increase, get the attribution for that decline, and make it obvious.



  

### P2 Bugs/Features
* Support "Source" for fetching data, allowing a fallback quoting. E.g. India from FinancialData.net
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

* Stress test to see how many RPS can be sustained.

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
* Missing India, HK and Colombia stock exchanges. Causes issues for AVEM. 
  * FinancialData.net adds India, HK. EODHD does not have info for colombia.


* Check how many are available just from FD.net, also run with just FD.net data + holdings from fidelity.
     * How do FD.net ETF holdings compare to Fidelity?
     * What does the FD.net resolution rate look like as compared to EODHD data?
* Why did we succeed on AV requests for missing ETFS? It should have errored out. Probably because it 200's and says not authorized after.
* NOFIX: This is fixed by the bulk fill hints/thresholds. When we fetch /glance after close of business but before 4am next day with bulk fetch, we fetch inefficiently with a bunch of singletons. Enough singleton fetches could make it look like a bulk fetch completed.


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
* Interleave symbol ingest between EODHD and FD. Update/append data vs. upsert
* UX fixes: add/edit/delete
  * Add add-portfolio to UX.
  * Add Edit-portfolio to UX
  * Add Delete portfolio to UX
* CSV parsing of "US" exchange should look for same ticker, but other exchange within US. Really should just look at any US exchange because FD.net might be wrong with Nasdaq/NYSE instead of ARCA
  * Add mutual fund fetching for the listings to FD.net. No ISIN/Exch available. Infer US.
  * ETF fetching for FD.net fetches should go straight to an enriched format. No ISIN/Exch available. Infer US.
* Add US 10Y Treasury fetcher including most recent data from FRED.
* HEIA in allie portfolio merged_clean.csv maps to HEI-A in db. 
* Finishing up FD integration
  * Fetch pricing data from FD.net
     * Add stock splits and dividends fetch coincident with this from the miscellaneous data section.
* Don't allow an end date of TODAY if we don't have data for TODAY. Both in UI and in Service
  * If the data is incomplete for TODAY (E.g. late market update), WARN and truncate both datasets.
  * Set the client to look for 0'd out data as well.     
* slice fetching is broken - both for fetch at day and if someone fetched a time range that was before my previous fetch. E.g. I have fetched Feb 1, 2025 to Mar 3 2026 for VTI. Then I fetch Feb 1, 2024 to Jan 1, 2024. I will have a big gap in my stored data. end date for the fetch must go to the earliest fact_price_range date.
* Now I am overfetching for cached pricing data... Fixed with major refactor around DetermineFetch
    * original problem was setting a date, and then moving backwards
* FDRXX is filling on a non market day (4/18/25) which is then causing everybody else to try to forward fill. Stupid overachiever.
* code cleanup: fdClient inside of PricingService is a misnomer. Might be AV, FD, EOD.
* code cleanup: Lots of models have "Symbol" which means "Ticker". We should unify on one name style.
* code cleanup: prune test cases for compact fetches. Clean up old tests/stale/un-dry. Run coverage. Add tests for missing. 
* Implement dividends card
* At-A-Glance implementation
  * Determine where to store the portfolios of interest. : In a table silly!
  * Generate endpoint to compute performance of portfolios
* Report Generation - Mocks
  * Topics for convo with your advisor
  * Market Recap
  * Strategy Review: Recap, Goal Amount, Idealized Portfolio
  * Performance: Volatility, Sharpe, Gains, Dividends, Tax Loss Harvesting/Gains
  * Benchmark: Chart it!
  * Top 10 Best/Worst. Recommendation for each. 
* Parallel fetch from EODHD
* Cleanup printing view of reportgen
* fix good friday / market holiday logic: should we precompute the days it is closed, hardcode it, and put that in a map for quick lookup rather than dynamically constructing each year for a given date
    * January 9, 2025 markets were closed.
* Bulk Fetch from EODHD. Compute whether bulk is better. Does bulk include splits? NOPE. Do I need splits on top of this given the range I have? YUP.
* Add endpoints for price export/import
* optimize price/export import memory/runtimes. 
* Need consistent theming for buttons  foreground color, Background, Button disabled, Mouseover behavior
* deadcode in ~/go/bin found bad paths : add to install instructions? 
* Cleanup code: cleanup.txt are problems that Gemini found. Have Claude consider them.
* consolidated bulkDividends and BulkSplit to BulkEventFetcher. But bulk => to price_fetcher was declined since FD doesn't have a bulk fetcher. Still have 3 calls to NewPricingService with eohdClient 3 times.
      96 +  pricingSvc := services.NewPricingService(priceRepo, securityRepo, eohdClient, eohdClient, fredClient, eohdClient).
* cleanup: is membership_service the right home for GetAllSecurities?
* nextTradingDay is duplicated in prefetch_service.go and elsewhere. Make it common.
* staticcheck: do we want to run this for code quality too?
* Performance is bad for compare. 5 seconds. ComputeMembership on Actual took 4.4s.
* When we fetch for Mar 1 2025:Mar13 2026 we always go back and re-fetch data even though the Mar1 date is a saturday. Repeated requests result in repeated re-fetches.
* Code Cleanup: Don't return pointers to small, immutable structs. Return the struct on the stack.
* in ETF import, -F stocks probably are referring to overseas stocks, not in US exchanges. It's an OTC code for "Foreign". We are dropping the -F and it sometimes resolves incorrectly: BH-F resolves to BHF but is actually TH0168A10Z19 / Bumrungrad Hospital PCL in thailand. SPEM and VWO both have this problem. -R seems to point at foreign stocks but should be "rights". PAA.U "Pan American Silver Corp" should be PAAS from fidelity for SCHF.
* When bulk fetching, and there is no trade data for that day, we don't update our price range, and then go re-fetch it singleton later...
* Add "birthday" for portfolio - user controlled "created at". 
* Sortino: Add a new card for downside volatility measurement like sharpe
* Try different data sources outside of Alphavantage
* created at also needs a snapshot at so we can go backwards/forwards for portfolio creation.
* glance portfolio value did not match compare portfolio value. need to handle forward/reverse splits after snapshotted_at but before start_date of compare window.
* Bulk fetching can return out of order data, and perhaps die on a middle chunk that was missing. If that happens, price_range says the data is there when it is not.
  * do we need consistency checking for missing chunks? YES. Find_gaps would be useful. : check_price_gaps.py
  * why am I skipping 9000 records in 2023 for bulk fetch? : old securities merged or delisted or Warrants/Units that expired. 
* Daily fetch of daily data did not happen / catch up did not happen: now schedules every 5min to try to resume. 
* Code Cleanup: Tests are slow again. Make them faster.
* Add alpha and beta measurements.
* Code Cleanup: eliminate stale / fix wrong comments.    
* UI: performance cards in compare don't line up vertically. Sharpe/Sortino pill on newline is the cause.
* I deployed a new binary without a complete database present on the RDS server. That causes issues. We should check database consistency first. Maybe even db version?
* 11/27/25 is filling data when it doesn't need to. It's a market holiday. 
* consolidate log.Printf to log.Info, fmt.printf to log.error (main.go)
* golang version of EODHD Security refresh
  * Exchange worker count is a separate variable. It should use the variable from main.go, and these should be exposed to .env / environment variables so we can tune on server. 
  * don't use sym.code, is it sim.ticker?
  * Dry run logic is separate from actual logic. This means the dry-run might count differently. They should be coalesced.
  * Is new securities really new securities or is it what was read? - it was including non US securities
  * Do a comparison for "how many securities in the db" vs. new_securities. 
  * 124,520 securities in the db. But eodhd now only shows 68911. What gives? Do I have a bunch of excluded securities in my db? .US was mapping everything to .US
  * NOFIX: Logic in csv load is separate from the EODHD logic as well. Can we coalesce that too?
  * NOFIX: This logic is in admin_service.go - should it be in a separate service with a thin layer for admin? I want to be able to schedule this. Lots of EODHD specific code in this.
  * original refactor did not move admin/load_securities/ipo to admin/securities
    * Missed this: 1) move /admin/load_securities and /admin/load_securities/ipo and /admin/sync-securities-from-av to : /admin/securities/load_csv, /admin/securities/load_ipo_csv,                   
  /admin/securities/sync-from-provider. 
* Current logic fails on JPRE which has no inception date and no pricing info prior to a 3 year lookback. Should find last day of pricing data in the data_coverage.go and use that instead. Add a test! (DONE)
* UI: pages look bad on iphone and don't handle rotate sideways cleanly (not using full width). Menus are starting above the viewport (for portfolio selection) in landscape mode. How do we test this effectively?
* UI: Found this in portfolio-infra - npm audit — you have dependency scanning in the Go repo (via govulncheck) but nothing equivalent here for the CDK/Node packages. Easy one-liner to add to the test suite.      
* Add logic to refresh securities on a scheduled basis. 
* Bulk fill early and often
  * Excise fact_fetch_log: it couldn't accurately predict that we had sufficient data because it progressively fills in over a 24h period. (see docs/price_fetching.md)
  * ComputeDailyValues refactor: rather than GetDailyPrices for each security just-in-time, instead: 1) check for possible missing data. 2) Do a bulk fetch from EODHD+any minor individual fills 3) fetch the daily prices in aggregate rather than as singletons from postgres. Then process them
  * revert our prevent-glance-on-end-of-day change? 454506e9fa0c9c4d791c7f61f688665dd10e3a1b . Favor a fetch of missing data insteasd.
  * Find a bulk fetch at close that redoes the bulk fetch the next day "just in case". What if... we track replaced counts by fetching
  last 2-3 days up to current day on warmup, every day at 4am or after? 
    * Skip this: And keep stats? Check what has been dropped vs. retained by re-fetching some of our daily data to see how it overlays.
  * app_hints table as an authoritative KV store for the last day we fetched. We don't have to re-fetch 3 days of data if we missed a day or two - if it is now 3 market days away, we stop re-fetching it. 
  * automate a 3 day re-fetch to fill in any data progressively.
* Can I constrain postgres to only allow read-only commands via an alias or equivalent? YES - psql_ro
* /glance is dropping days because no data for a penny stock. Need to fetch last day before the start_date to seed the fill-forward
* fill forward is too spammy. Can we make it less so?
* refetching n-2 day on every server restart. Can we track via hint to avoid refetch?
* removed Alphavantage: commit 4d47572693e2dad2bd68a12952ab683a5738d35d (HEAD -> main, origin/main, origin/HEAD, feature/purge_av)
* branches introduce schema changes breaks main. Allow new tables in flight from this in sql_test.go
* Missing EODHD or Fred keys should be an error, not a warn.
* These need to be errors: Apr 19 18:51:35 ip-10-0-0-106.ec2.internal portfolio-api[1960]: WARN[2026-04-19 18:51:35] PrefetchService: failed to update N-2 correction hint: SetDateHint "last_n2_correction_fetch_date": ERROR: relation "app_hints" does not exist (SQLSTATE 42P01)

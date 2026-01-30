### Configuring Claude
 * Add //CLAUDE_DO_NOT_TOUCH and //CLAUDE_DO_NOT_TOUCH_END
 * Track subcommands being issued. Find out what it's doing under the covers - e.g. analyze my database schema

### More elegant error handling
* Better function headers. Describe what portion of instructions attempting to implement.
* Better error messages in failures

### Tests
* Move database to a clean copy - so that we don't end up polluting a prod db (also ensures we get clean ID #'s we can trusqt
* Prepopulate db with user data
* Explore adding unit tests
* Consider moving tests to sit next to the source code itself
* Add a test to prevent refactors from touching DO_NOT_TOUCH sections. If git diff shows the section to be touched, fail the test.
* Stress test to see how many RPS can be sustained.

### AV
* Prepopulate interesting stocks
  * can dl a list of stocks for nasdaq from: https://www.nasdaq.com/market-activity/stocks/screener
  * https://www.sec.gov/file/company-tickers
  * https://github.com/LondonMarket/Global-Stock-Symbols
  * https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo



### Bugs
* Comments are not handled for portfolio. Also need a test that checks for those fields. (scan the table, try all the fields for every CRUD endpoint?)
* Fix the sharpe calculation logic : correct the daily interest compounding formula
* Add sharpe via mean of Rt-Rf or end case Rt-Rf. Consider computing both and returning both?
* Imports have github.com/epeers. It seems like it's local. Read up why it is ok or not ok.
* Change size of exchange to 4 characters in @create_tables.sql:dim_security:exchange


### Features
* Add cacheing layer in memory. There is code, but it needs to be thought out.
* Add a symbols endpoint to fetch known symbols to SymbolID.  Consider bundling this data as part of the react app itself so it doesn't have to hit the API
* Add a 
* Add .env file reading. @config.go ?
* add pricing table to create_tables.sql : refactor existing claude generated logic in repository and alphavantage to utilize it. 
  * needs to fetch historic data to present. Capture start data to end data. Need additional table to capture how much data we have?
* add retry/backoff logic to AV if we are declined due to too many requests per minute.
* add event logging to capture interesting features/events
  * AV backoff failures
  * API calls
  * 
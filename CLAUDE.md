### Project Overview
"Portfolio" is a CRUD server that analyzes financial portfolios comprises of stocks, bonds, and ETF's. It is written in go. Portfolio uses the gin library to route http requests. The portfolio server fetches recent and historical data from the Alphavantage API. To avoid multiple calls to Alphavantage (AV), and for speed, the program caches data in Postgres. 

### Code Style & Architecture
* Postgres support lives in internal/repository/
* business logic in internal/services/
* gin route handling in internal/handlers/
* tests live in tests/
* json representation of data for http, and fetched from postgres is in internal/models

### Repository Table Ownership

Each repository file in `internal/repository/` owns specific database tables.
A repository should ONLY query tables it owns. If you need data from a table
owned by another repository, call that repository's methods instead.

**Exception**: Read-only JOINs for lookup purposes are allowed.

See `TestRepositoryTableOwnership` in `tests/sql_test.go` for the authoritative
table-to-repository mapping.

### Testing
* Every new feature should have include a file in tests/
* Features should be tested for both error conditions and for correctness

### Environment variables
* 2 environment variables are necessary to connect to postgres (PG_URL), and to connect to the Alphavantage API (AV_KEY)
* These variables can be set by sourcing exports_no_commit.bash



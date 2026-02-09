### Project Overview
"Portfolio" is a CRUD server that analyzes financial portfolios comprises of stocks, bonds, and ETF's. It is written in go. Portfolio uses the gin library to route http requests. The portfolio server fetches recent and historical data from the Alphavantage API. To avoid multiple calls to Alphavantage (AV), and for speed, the program caches data in Postgres. 

### Code Style & Architecture
* `main.go` — entry point, dependency wiring, Gin router setup
* `config/` — config struct, loads PG_URL/AV_KEY from env
* `internal/alphavantage/` — AV API client, response types, listing status
* `internal/database/` — Postgres connection pool (pgxpool)
* `internal/middleware/` — auth middleware (user ID extraction)
* `internal/models/` — data models, API request/response DTOs
* `internal/handlers/` — Gin HTTP handlers (portfolio, user, compare, admin, csv)
* `internal/services/` — business logic (portfolio, pricing, membership, performance, comparison, admin)
* `internal/repository/` — database operations (portfolio, security, price_cache, exchange, security_type)
* `docs/` — auto-generated Swagger docs
* `tests/` — integration tests (shared setup in setup_test.go, requires live DB)

### Dependency Flow
handlers → services → repositories → pgxpool
                   → alphavantage client
Tests use real DB connections via shared setup in tests/setup_test.go.

### Function Design

When a function computes intermediate data that could be useful elsewhere:
* **Extract it** into a separate public function rather than returning it as a byproduct
* **Pass it in** to dependent functions rather than having them recompute it
* Example: `ComputeDailyValues()` is separate from `ComputeSharpe()` so daily values
  can be reused for other metrics (charts, volatility, max drawdown) without recomputation

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

### Swagger Docs                                                                                                                                                                                  
* Regenerate after model changes: `~/go/bin/swag init --parseDependency --parseInternal`   

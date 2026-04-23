package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epeers/portfolio/config"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	logrus "github.com/sirupsen/logrus"
)

func TestMain(m *testing.M) {
	var err error

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logrus.Fatalf("Failed to load configuration: %v", err)
	}

	// Get database URL from environment
	pgURL := cfg.PGURL
	/*
		os.Getenv("PG_URL")
	*/
	if pgURL == "" {
		logrus.Fatalf("PG_URL environment variable not set, skipping integration tests")
	}

	// Create connection pool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testPool, err = pgxpool.New(ctx, pgURL)
	if err != nil {
		logrus.Fatalf("Failed to connect to database: %v", err)
	}
	defer testPool.Close()

	// Verify connection
	if err := testPool.Ping(ctx); err != nil {
		logrus.Fatalf("Failed to ping database: %v", err)
	}

	// Suppress application logrus output during test execution so error/warn/info
	// messages from code under test don't pollute the terminal.
	logrus.SetOutput(io.Discard)

	// Run tests
	code := m.Run()

	os.Exit(code)
}

// testSeq is an atomic counter for generating unique tickers and portfolio names.
var testSeq int64

// nextTicker returns a unique TST-prefixed ticker for parallel-safe DB tests.
func nextTicker() string {
	id := atomic.AddInt64(&testSeq, 1)
	return fmt.Sprintf("T%06dT", id) // e.g. T000001T (8 chars)
}

// nextPortfolioName returns a unique portfolio name for parallel-safe DB tests.
func nextPortfolioName() string {
	id := atomic.AddInt64(&testSeq, 1)
	return fmt.Sprintf("test_ptf_%d", id)
}

// --- Consolidated test helpers ---

// createTestSecurity creates a test security with the given type.
// Cleans up any prior data for this ticker first.
func createTestSecurity(pool *pgxpool.Pool, ticker, name string, secType models.SecurityType, inception *time.Time) (int64, error) {
	ctx := context.Background()

	// Clean up any existing test security first
	cleanupTestSecurity(pool, ticker)

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO dim_security (ticker, name, exchange, type, inception)
		VALUES ($1, $2, 2, $3, $4)
		RETURNING id
	`, ticker, name, string(secType), inception).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert test security: %w", err)
	}

	return id, nil
}

// createTestStock creates a test stock (COMMON STOCK) security.
func createTestStock(pool *pgxpool.Pool, ticker, name string) (int64, error) {
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	return createTestSecurity(pool, ticker, name, models.SecurityTypeStock, &inception)
}

// createTestETF creates a test ETF security.
func createTestETF(pool *pgxpool.Pool, ticker, name string) (int64, error) {
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	return createTestSecurity(pool, ticker, name, models.SecurityTypeETF, &inception)
}

// cleanupTestSecurity removes a test security and ALL dependent data.
// Safe to call even if some tables have no data for this ticker.
func cleanupTestSecurity(pool *pgxpool.Pool, ticker string) {
	ctx := context.Background()

	var securityID int64
	err := pool.QueryRow(ctx, `SELECT id FROM dim_security WHERE ticker = $1`, ticker).Scan(&securityID)
	if err != nil {
		return // Security doesn't exist
	}

	// Delete from all dependent tables in FK order
	pool.Exec(ctx, `DELETE FROM portfolio_membership WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM dim_etf_membership WHERE dim_composite_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM dim_etf_membership WHERE dim_security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM dim_etf_pull_range WHERE composite_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM fact_price WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM fact_event WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
}

// insertPriceData inserts price data for a security using a bulk COPY for speed.
func insertPriceData(pool *pgxpool.Pool, securityID int64, startDate, endDate time.Time, basePrice float64) error {
	ctx := context.Background()

	var rows [][]any
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		rows = append(rows, []any{securityID, d, basePrice, basePrice + 5, basePrice - 1, basePrice + 2, int64(1000000)})
	}

	if len(rows) > 0 {
		_, err := pool.CopyFrom(ctx,
			pgx.Identifier{"fact_price"},
			[]string{"security_id", "date", "open", "high", "low", "close", "volume"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("failed to bulk insert price data: %w", err)
		}
	}

	// Set up price range with far-future next_update
	futureNextUpdate := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := pool.Exec(ctx, `
		INSERT INTO fact_price_range (security_id, start_date, end_date, next_update)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (security_id) DO UPDATE SET start_date = $2, end_date = $3, next_update = $4
	`, securityID, startDate, endDate, futureNextUpdate)
	if err != nil {
		return fmt.Errorf("failed to insert price range: %w", err)
	}

	return nil
}

// insertDividendEvent inserts a dividend event into fact_event.
func insertDividendEvent(pool *pgxpool.Pool, securityID int64, date time.Time, dividend float64) error {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO fact_event (security_id, date, dividend, split_coefficient)
		VALUES ($1, $2, $3, 1.0)
		ON CONFLICT (security_id, date) DO UPDATE
		SET dividend = EXCLUDED.dividend
	`, securityID, date, dividend)
	if err != nil {
		return fmt.Errorf("failed to insert dividend event: %w", err)
	}
	return nil
}

// insertSplitEvent inserts a split event into fact_event
func insertSplitEvent(pool *pgxpool.Pool, securityID int64, date time.Time, splitCoefficient float64) error {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO fact_event (security_id, date, dividend, split_coefficient)
		VALUES ($1, $2, 0, $3)
		ON CONFLICT (security_id, date) DO UPDATE
		SET split_coefficient = EXCLUDED.split_coefficient
	`, securityID, date, splitCoefficient)
	if err != nil {
		return fmt.Errorf("failed to insert split event: %w", err)
	}
	return nil
}

// insertETFHoldings directly inserts ETF holdings and pull range into the database
func insertETFHoldings(pool *pgxpool.Pool, etfID int64, holdings map[int64]float64) error {
	ctx := context.Background()

	for secID, percentage := range holdings {
		_, err := pool.Exec(ctx, `
			INSERT INTO dim_etf_membership (dim_security_id, dim_composite_id, percentage)
			VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING
		`, secID, etfID, percentage)
		if err != nil {
			return fmt.Errorf("failed to insert ETF holding: %w", err)
		}
	}

	// Set pull range with a month in the future
	futureUpdate := time.Now().AddDate(0, 1, 0)
	_, err := pool.Exec(ctx, `
		INSERT INTO dim_etf_pull_range (composite_id, pull_date, next_update)
		VALUES ($1, $2, $3)
		ON CONFLICT (composite_id) DO UPDATE SET pull_date = $2, next_update = $3
	`, etfID, time.Now(), futureUpdate)
	if err != nil {
		return fmt.Errorf("failed to insert ETF pull range: %w", err)
	}

	return nil
}

// createTestPortfolio creates a portfolio for testing
func createTestPortfolio(pool *pgxpool.Pool, name string, ownerID int64, portfolioType models.PortfolioType, memberships []models.MembershipRequest) (int64, error) {
	ctx := context.Background()

	var portfolioID int64
	now := time.Now()
	err := pool.QueryRow(ctx, `
		INSERT INTO portfolio (name, owner, portfolio_type, objective, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`, name, ownerID, portfolioType, models.ObjectiveGrowth, now, now).Scan(&portfolioID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert portfolio: %w", err)
	}

	for _, m := range memberships {
		_, err := pool.Exec(ctx, `
			INSERT INTO portfolio_membership (portfolio_id, security_id, percentage_or_shares)
			VALUES ($1, $2, $3)
		`, portfolioID, m.SecurityID, m.PercentageOrShares)
		if err != nil {
			return 0, fmt.Errorf("failed to insert membership: %w", err)
		}
	}

	return portfolioID, nil
}

// cleanupTestPortfolio removes test portfolio and its memberships
func cleanupTestPortfolio(pool *pgxpool.Pool, name string, ownerID int64) {
	ctx := context.Background()
	pool.Exec(ctx, `
		DELETE FROM portfolio_glance
		WHERE portfolio_id IN (
			SELECT id FROM portfolio WHERE name = $1 AND owner = $2
		)
	`, name, ownerID)
	pool.Exec(ctx, `
		DELETE FROM portfolio_membership
		WHERE portfolio_id IN (
			SELECT id FROM portfolio WHERE name = $1 AND owner = $2
		)
	`, name, ownerID)
	pool.Exec(ctx, `DELETE FROM portfolio WHERE name = $1 AND owner = $2`, name, ownerID)
}

// findMembership finds an ExpandedMembership by ticker in a slice
func findMembership(memberships []models.ExpandedMembership, ticker string) *models.ExpandedMembership {
	for i := range memberships {
		if memberships[i].Ticker == ticker {
			return &memberships[i]
		}
	}
	return nil
}

// findSource finds a MembershipSource by ticker in a slice
func findSource(sources []models.MembershipSource, ticker string) *models.MembershipSource {
	for i := range sources {
		if sources[i].Ticker == ticker {
			return &sources[i]
		}
	}
	return nil
}

// createMockEODHDPriceServer creates a mock EODHD-compatible price server.
// Pass nil for prices to return an empty JSON array (no data rows).
// Pass nil for callCounter if call tracking is not needed.
func createMockEODHDPriceServer(prices []providers.ParsedPriceData, callCounter *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCounter != nil {
			atomic.AddInt32(callCounter, 1)
		}
		w.Header().Set("Content-Type", "application/json")
		type eodhRecord struct {
			Date          string  `json:"date"`
			Open          float64 `json:"open"`
			High          float64 `json:"high"`
			Low           float64 `json:"low"`
			Close         float64 `json:"close"`
			AdjustedClose float64 `json:"adjusted_close"`
			Volume        int64   `json:"volume"`
		}
		records := make([]eodhRecord, 0, len(prices))
		for _, p := range prices {
			records = append(records, eodhRecord{
				Date:          p.Date.Format("2006-01-02"),
				Open:          p.Open,
				High:          p.High,
				Low:           p.Low,
				Close:         p.Close,
				AdjustedClose: p.Close,
				Volume:        p.Volume,
			})
		}
		json.NewEncoder(w).Encode(records)
	}))
}

// generatePriceData generates mock price data for a date range, skipping weekends.
func generatePriceData(startDate, endDate time.Time) []providers.ParsedPriceData {
	var prices []providers.ParsedPriceData
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		prices = append(prices, providers.ParsedPriceData{
			Date:             d,
			Open:             100.00,
			High:             105.00,
			Low:              99.00,
			Close:            102.00,
			Volume:           1000000,
			Dividend:         0,
			SplitCoefficient: 1.0,
		})
	}
	return prices
}

// insertPriceRows inserts exact close prices for the given security, one row per date.
// Use this instead of insertPriceData when tests need specific close values (e.g. 2x/0.5x
// relationships) that the basePrice+2 offset in insertPriceData would distort.
// Also inserts/updates fact_price_range to cover the full span of inserted dates.
func insertPriceRows(t *testing.T, pool *pgxpool.Pool, secID int64, prices map[time.Time]float64) {
	t.Helper()
	ctx := context.Background()

	for d, p := range prices {
		_, err := pool.Exec(ctx, `
			INSERT INTO fact_price (security_id, date, open, high, low, close, volume)
			VALUES ($1, $2, $3, $3, $3, $3, 1000000)
			ON CONFLICT (security_id, date)
			DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high,
			              low=EXCLUDED.low,  close=EXCLUDED.close`,
			secID, d, p)
		if err != nil {
			t.Fatalf("insertPriceRows: insert date %s: %v", d.Format("2006-01-02"), err)
		}
	}

	var minD, maxD time.Time
	first := true
	for d := range prices {
		if first || d.Before(minD) {
			minD = d
		}
		if first || d.After(maxD) {
			maxD = d
		}
		first = false
	}
	if first {
		return // no rows
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO fact_price_range (security_id, start_date, end_date, next_update)
		VALUES ($1, $2, $3, '2099-01-01')
		ON CONFLICT (security_id) DO UPDATE
		    SET start_date  = LEAST(fact_price_range.start_date, EXCLUDED.start_date),
		        end_date    = GREATEST(fact_price_range.end_date, EXCLUDED.end_date),
		        next_update = '2099-01-01'`,
		secID, minD, maxD)
	if err != nil {
		t.Fatalf("insertPriceRows: fact_price_range upsert: %v", err)
	}
}

// sourcesSum returns the sum of source allocations
func sourcesSum(sources []models.MembershipSource) float64 {
	sum := 0.0
	for _, s := range sources {
		sum += s.Allocation
	}
	return sum
}

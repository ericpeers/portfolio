package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epeers/portfolio/config"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/jackc/pgx/v5/pgxpool"
	logrus "github.com/sirupsen/logrus"
)

func TestMain(m *testing.M) {
	var err error

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Get database URL from environment
	pgURL := cfg.PGURL
	/*
		os.Getenv("PG_URL")
	*/
	if pgURL == "" {
		log.Fatalf("PG_URL environment variable not set, skipping integration tests")
	}

	// Create connection pool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testPool, err = pgxpool.New(ctx, pgURL)
	if err != nil {
		fmt.Printf("Failed to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer testPool.Close()

	// Verify connection
	if err := testPool.Ping(ctx); err != nil {
		fmt.Printf("Failed to ping database: %v\n", err)
		os.Exit(1)
	}

	// Suppress application logrus output during test execution so error/warn/info
	// messages from code under test don't pollute the terminal.
	logrus.SetOutput(io.Discard)

	// Run tests
	code := m.Run()

	os.Exit(code)
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

// insertPriceData inserts price data for a security
func insertPriceData(pool *pgxpool.Pool, securityID int64, startDate, endDate time.Time, basePrice float64) error {
	ctx := context.Background()

	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		// Skip weekends
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO fact_price (security_id, date, open, high, low, close, volume)
			VALUES ($1, $2, $3, $4, $5, $6, 1000000)
			ON CONFLICT (security_id, date) DO NOTHING
		`, securityID, d, basePrice, basePrice+5, basePrice-1, basePrice+2)
		if err != nil {
			return fmt.Errorf("failed to insert price data: %w", err)
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
		INSERT INTO portfolio (name, owner, portfolio_type, objective, created, updated)
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

// createMockETFServer creates a mock AV server that returns the given holdings
// for ETF_PROFILE requests. Pass nil for holdings to return an empty profile.
// Pass nil for callCounter if call tracking is not needed.
// TREASURY_YIELD requests return a minimal valid CSV so ComputeSharpe doesn't fail
// when the US10Y fact_price_range next_update has elapsed.
func createMockETFServer(holdings []alphavantage.ETFHolding, callCounter *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCounter != nil {
			atomic.AddInt32(callCounter, 1)
		}
		switch r.URL.Query().Get("function") {
		case "ETF_PROFILE":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(alphavantage.ETFProfileResponse{Holdings: holdings})
		case "TREASURY_YIELD":
			w.Header().Set("Content-Type", "text/csv")
			w.Write([]byte("timestamp,value\n2026-02-24,4.52\n2026-02-21,4.50\n"))
		default:
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{}`))
		}
	}))
}

// createMockPriceServer creates a mock AlphaVantage-compatible price server.
// Pass nil for prices to return a header-only CSV (no data rows).
// Pass nil for callCounter if call tracking is not needed.
func createMockPriceServer(prices []providers.ParsedPriceData, callCounter *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCounter != nil {
			atomic.AddInt32(callCounter, 1)
		}

		// Return AlphaVantage-format CSV: header + one row per price entry.
		// Column order matches what alphavantage.Client.GetDailyPrices expects:
		//   timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient
		w.Header().Set("Content-Type", "text/csv")
		fmt.Fprint(w, "timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient\n")
		for _, p := range prices {
			fmt.Fprintf(w, "%s,%.5f,%.5f,%.5f,%.5f,%.5f,%d,0.0000,1.0000\n",
				p.Date.Format("2006-01-02"), p.Open, p.High, p.Low, p.Close, p.Close, p.Volume)
		}
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

// sourcesSum returns the sum of source allocations
func sourcesSum(sources []models.MembershipSource) float64 {
	sum := 0.0
	for _, s := range sources {
		sum += s.Allocation
	}
	return sum
}

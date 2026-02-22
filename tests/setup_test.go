package tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/epeers/portfolio/config"
	"github.com/epeers/portfolio/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
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
		fmt.Println("PG_URL environment variable not set, skipping integration tests")
		os.Exit(0)
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

	// Seed US10Y treasury data (persistent fixture, never cleaned up)
	if err := ensureUS10YData(testPool); err != nil {
		fmt.Printf("Failed to seed US10Y data: %v\n", err)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	os.Exit(code)
}

// ensureUS10YData seeds the US10Y treasury security and its price data from CSV.
// This is idempotent: if data already exists, it returns early.
func ensureUS10YData(pool *pgxpool.Pool) error {
	ctx := context.Background()

	// Check if US10Y already has price data
	var securityID int64
	err := pool.QueryRow(ctx, `SELECT id FROM dim_security WHERE ticker = 'US10Y'`).Scan(&securityID)
	if err == nil {
		var count int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM fact_price WHERE security_id = $1`, securityID).Scan(&count)
		if count >= 16013 { //on 2-17-26 we had this many rows.
			log.Printf("US10Y already seeded (%d price rows), skipping", count)
			return nil
		}
	}

	// Read the seed file
	sqlBytes, err := os.ReadFile("../seed_us10y.sql")
	if err != nil {
		return fmt.Errorf("failed to read seed file: %w", err)
	}

	// Execute the SQL
	_, err = pool.Exec(ctx, string(sqlBytes))
	if err != nil {
		return fmt.Errorf("seeding failed: %w", err)
	}

	return nil
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

	// Set pull range with far-future next_update so cache is used
	futureUpdate := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
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
		DELETE FROM portfolio_membership
		WHERE portfolio_id IN (
			SELECT id FROM portfolio WHERE name = $1 AND owner = $2
		)
	`, name, ownerID)
	pool.Exec(ctx, `DELETE FROM portfolio WHERE name = $1 AND owner = $2`, name, ownerID)
}

// findMembership finds an ExpandedMembership by symbol in a slice
func findMembership(memberships []models.ExpandedMembership, symbol string) *models.ExpandedMembership {
	for i := range memberships {
		if memberships[i].Symbol == symbol {
			return &memberships[i]
		}
	}
	return nil
}

// findSource finds a MembershipSource by symbol in a slice
func findSource(sources []models.MembershipSource, symbol string) *models.MembershipSource {
	for i := range sources {
		if sources[i].Symbol == symbol {
			return &sources[i]
		}
	}
	return nil
}

// sourcesSum returns the sum of source allocations
func sourcesSum(sources []models.MembershipSource) float64 {
	sum := 0.0
	for _, s := range sources {
		sum += s.Allocation
	}
	return sum
}

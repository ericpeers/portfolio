package tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/epeers/portfolio/config"
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

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

	// Run tests
	code := m.Run()

	os.Exit(code)
}

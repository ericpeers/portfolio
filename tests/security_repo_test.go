package tests

import (
	"context"
	"testing"

	"github.com/epeers/portfolio/internal/repository"
)

// TestGetMultipleBySymbolsEmpty tests that empty input returns empty map
func TestGetMultipleBySymbolsEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	repo := repository.NewSecurityRepository(pool)

	result, err := repo.GetMultipleBySymbols(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected empty map, got %d entries", len(result))
	}
}

// TestGetMultipleBySymbolsMultipleValid tests fetching multiple existing securities
func TestGetMultipleBySymbolsMultipleValid(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test securities
	tickers := []string{"TSTSYM1", "TSTSYM2", "TSTSYM3"}
	names := []string{"Test Symbol One", "Test Symbol Two", "Test Symbol Three"}
	createdIDs := make(map[string]int64)

	for i, ticker := range tickers {
		id, err := createTestStock(pool, ticker, names[i])
		if err != nil {
			t.Fatalf("Failed to insert test security %s: %v", ticker, err)
		}
		createdIDs[ticker] = id
	}
	defer func() {
		for _, ticker := range tickers {
			cleanupTestSecurity(pool, ticker)
		}
	}()

	// Test: Fetch all three by symbol
	repo := repository.NewSecurityRepository(pool)
	result, err := repo.GetMultipleBySymbols(ctx, tickers)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 securities, got %d", len(result))
	}

	for ticker, expectedID := range createdIDs {
		sec, exists := result[ticker]
		if !exists {
			t.Errorf("Security %s not found in result", ticker)
			continue
		}
		if sec.ID != expectedID {
			t.Errorf("Security %s: expected ID %d, got %d", ticker, expectedID, sec.ID)
		}
		if sec.Symbol != ticker {
			t.Errorf("Security %s: expected symbol %s, got %s", ticker, ticker, sec.Symbol)
		}
	}
}

// TestGetMultipleBySymbolsMixedValidInvalid tests that only valid symbols are returned
func TestGetMultipleBySymbolsMixedValidInvalid(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create one test security
	ticker := "TSTMIXED1"
	id, err := createTestStock(pool, ticker, "Test Mixed One")
	if err != nil {
		t.Fatalf("Failed to insert test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	// Test: Fetch with mix of valid and invalid symbols
	repo := repository.NewSecurityRepository(pool)
	result, err := repo.GetMultipleBySymbols(ctx, []string{ticker, "NONEXISTENT123", "ALSONOTREAL456"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 security (only the valid one), got %d", len(result))
	}

	sec, exists := result[ticker]
	if !exists {
		t.Errorf("Valid security %s not found in result", ticker)
	} else if sec.ID != id {
		t.Errorf("Expected ID %d, got %d", id, sec.ID)
	}
}

// TestGetMultipleBySymbolsDuplicates tests that duplicate symbols in input are handled correctly
func TestGetMultipleBySymbolsDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test security
	ticker := "TSTDUP1"
	id, err := createTestStock(pool, ticker, "Test Duplicate One")
	if err != nil {
		t.Fatalf("Failed to insert test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	// Test: Fetch with duplicate symbols in input
	repo := repository.NewSecurityRepository(pool)
	result, err := repo.GetMultipleBySymbols(ctx, []string{ticker, ticker, ticker})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should only return one entry (map keys are unique)
	if len(result) != 1 {
		t.Errorf("Expected 1 security (duplicates collapsed), got %d", len(result))
	}

	sec, exists := result[ticker]
	if !exists {
		t.Errorf("Security %s not found in result", ticker)
	} else if sec.ID != id {
		t.Errorf("Expected ID %d, got %d", id, sec.ID)
	}
}


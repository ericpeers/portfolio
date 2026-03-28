package tests

import (
	"context"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

// TestBulkFetchPricesRejectsIncompleteResponse verifies that BulkFetchPrices returns
// an error when EODHD returns fewer than 30,000 matched prices (premature/partial fetch),
// and that no data is written to fact_price, fact_price_range, or fact_fetch_log.
//
// This guards the PrefetchService retry logic: the error causes the scheduler's
// break-on-error to fire, keeping the fact_price_range watermark at the previous date
// so the next 5-minute poll retries the same day with complete data.
func TestBulkFetchPricesRejectsIncompleteResponse(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ticker1 := nextTicker()
	ticker2 := nextTicker()
	secID1, err := createTestStock(pool, ticker1, "Prefetch Test Stock 1")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	secID2, err := createTestStock(pool, ticker2, "Prefetch Test Stock 2")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker1)
	defer cleanupTestSecurity(pool, ticker2)

	// Record pre-test row count in fact_fetch_log to confirm nothing is added.
	var logCountBefore int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_fetch_log WHERE fetch_type = 'BULK_PRICE_FETCH'`,
	).Scan(&logCountBefore); err != nil {
		t.Fatalf("count fact_fetch_log: %v", err)
	}

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avDummy := alphavantage.NewClient("test-key", "http://localhost:9999")

	fetchDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC) // Monday

	// Mock returns only 2 EOD records — far below the 30,000 threshold.
	bulk := &mockBulkFetcher{
		eodRecords: []providers.BulkEODRecord{
			{Code: ticker1, Date: fetchDate, Open: 100, High: 105, Low: 99, Close: 102, AdjClose: 102, Volume: 1000},
			{Code: ticker2, Date: fetchDate, Open: 200, High: 205, Low: 199, Close: 202, AdjClose: 202, Volume: 2000},
		},
	}

	svc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    avDummy,
		Treasury: avDummy,
		Bulk:     bulk,
	})

	secsByTicker := map[string]*models.Security{
		ticker1: {ID: secID1, Ticker: ticker1},
		ticker2: {ID: secID2, Ticker: ticker2},
	}

	_, fetchErr := svc.BulkFetchPrices(ctx, "US", fetchDate, secsByTicker, models.MinBulkFetchPrices)

	// Must return an error — callers rely on this to trigger retry.
	if fetchErr == nil {
		t.Fatal("BulkFetchPrices: expected error for incomplete response (<30k prices), got nil")
	}

	// fact_price must have no rows for the test securities.
	var priceCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_price WHERE security_id IN ($1, $2) AND date = $3`,
		secID1, secID2, fetchDate,
	).Scan(&priceCount); err != nil {
		t.Fatalf("count fact_price: %v", err)
	}
	if priceCount != 0 {
		t.Errorf("fact_price: expected 0 rows after incomplete fetch, got %d", priceCount)
	}

	// fact_price_range must have no rows for the test securities (no partial watermark advance).
	var rangeCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_price_range WHERE security_id IN ($1, $2)`,
		secID1, secID2,
	).Scan(&rangeCount); err != nil {
		t.Fatalf("count fact_price_range: %v", err)
	}
	if rangeCount != 0 {
		t.Errorf("fact_price_range: expected 0 rows after incomplete fetch, got %d", rangeCount)
	}

	// fact_fetch_log must be unchanged — no new entry for this fetch.
	var logCountAfter int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_fetch_log WHERE fetch_type = 'BULK_PRICE_FETCH'`,
	).Scan(&logCountAfter); err != nil {
		t.Fatalf("count fact_fetch_log after: %v", err)
	}
	if logCountAfter != logCountBefore {
		t.Errorf("fact_fetch_log: count changed from %d to %d; expected no new entries on incomplete fetch",
			logCountBefore, logCountAfter)
	}
}

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

// TestBulkFetchHistoricalDateDoesNotSuppressGapDetection verifies that a bulk fetch for a
// historical date does not advance next_update to a future timestamp. If it did, DetermineFetch
// Case A would silently skip the gap between the historical date and today, causing /glance to
// serve stale prices without triggering a re-fetch for the missing days.
//
// Regression: BulkFetchPrices previously used NextMarketDate(time.Now()) unconditionally,
// which set next_update = next market close (future) even for past-date backfills.
func TestBulkFetchHistoricalDateDoesNotSuppressGapDetection(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ticker := nextTicker()
	secID, err := createTestStock(pool, ticker, "Bulk Historical Gap Test")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avDummy := alphavantage.NewClient("test-key", "http://localhost:9999")

	// A clearly historical date — well over a year in the past.
	backfillDate := time.Date(2024, 6, 3, 0, 0, 0, 0, time.UTC) // Monday

	bulk := &mockBulkFetcher{
		eodRecords: []providers.BulkEODRecord{
			{Code: ticker, Date: backfillDate, Open: 50, High: 52, Low: 49, Close: 51, AdjClose: 51, Volume: 5000},
		},
	}

	svc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    avDummy,
		Treasury: avDummy,
		Bulk:     bulk,
	})

	secsByTicker := map[string]*models.Security{
		ticker: {ID: secID, Ticker: ticker},
	}

	if _, err := svc.BulkFetchPrices(ctx, "US", backfillDate, secsByTicker, 0); err != nil {
		t.Fatalf("BulkFetchPrices: %v", err)
	}

	var nextUpdate time.Time
	if err := pool.QueryRow(ctx,
		`SELECT next_update FROM fact_price_range WHERE security_id = $1`, secID,
	).Scan(&nextUpdate); err != nil {
		t.Fatalf("read fact_price_range: %v", err)
	}

	// next_update must be in the past. A future value would suppress DetermineFetch
	// Case A and hide the gap between backfillDate and today.
	if !nextUpdate.Before(time.Now()) {
		t.Errorf("next_update = %v is in the future after a historical bulk fetch; "+
			"DetermineFetch Case A will not fire and the gap to today will not be filled",
			nextUpdate)
	}
}

// TestBulkFetchPricesRejectsIncompleteResponse verifies that BulkFetchPrices returns
// an error when EODHD returns fewer than 30,000 matched prices (premature/partial fetch),
// and that no data is written to fact_price or fact_price_range.
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

}

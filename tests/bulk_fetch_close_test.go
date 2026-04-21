package tests

import (
	"context"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

// TestBulkFetchStoresUnadjustedClose verifies that BulkFetchPrices stores the
// unadjusted close price (rec.Close), not the split-adjusted close (rec.AdjClose).
//
// When Close and AdjClose differ — as they do for securities that had a subsequent
// stock split — storing AdjClose creates a price discontinuity at the boundary
// between historical inline-fetched data and daily bulk-fetched data.
// This was the root cause of the May 31 → Jun 1, 2023 price seam observed in
// portfolios 2626/2627, where NVDA appeared to jump ~10× overnight.
func TestBulkFetchStoresUnadjustedClose(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ticker := nextTicker()
	secID, err := createTestStock(pool, ticker, "Bulk Close Test Stock")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)

	fetchDate := time.Date(2025, 3, 3, 0, 0, 0, 0, time.UTC) // Monday

	// Simulate a security that had a 10:1 split after this date.
	// EODHD bulk endpoint returns both the actual close and the split-adjusted close.
	// We want the actual (unadjusted) close stored.
	const actualClose = 397.70
	const adjClose = 39.77 // actualClose / 10

	bulk := &mockBulkFetcher{
		eodRecords: []providers.BulkEODRecord{
			{
				Code:     ticker,
				Date:     fetchDate,
				Open:     390.00,
				High:     400.00,
				Low:      388.00,
				Close:    actualClose,
				AdjClose: adjClose,
				Volume:   1_000_000,
			},
		},
	}

	svc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
		Bulk:     bulk,
	})

	secsByTicker := map[string]*models.Security{
		ticker: {ID: secID, Ticker: ticker},
	}

	if _, err := svc.BulkFetchPrices(ctx, "US", fetchDate, secsByTicker, 0); err != nil {
		t.Fatalf("BulkFetchPrices: %v", err)
	}

	var storedClose float64
	if err := pool.QueryRow(ctx,
		`SELECT close FROM fact_price WHERE security_id = $1 AND date = $2`,
		secID, fetchDate,
	).Scan(&storedClose); err != nil {
		t.Fatalf("read fact_price: %v", err)
	}

	if storedClose != actualClose {
		t.Errorf("stored close = %.4f, want unadjusted close %.4f (not adjusted %.4f); "+
			"BulkFetchPrices must store rec.Close, not rec.AdjClose",
			storedClose, actualClose, adjClose)
	}
}

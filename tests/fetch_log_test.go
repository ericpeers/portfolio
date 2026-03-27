package tests

import (
	"context"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/repository"
)

// testFetchLogSentinel is a date far enough in the future that the real bulk-fetch service
// will never produce it. Cleanup targets only rows with this date, leaving production rows
// (which always carry the actual market date) untouched.
var testFetchLogSentinelBase = time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)

// cleanupFetchLog removes only sentinel-dated rows inserted by tests.
// It never touches rows the real bulk-fetch service inserted.
func cleanupFetchLog(t *testing.T) {
	t.Helper()
	pool := getTestPool(t)
	_, err := pool.Exec(context.Background(),
		`DELETE FROM fact_fetch_log WHERE fetch_date >= '2099-01-01'`)
	if err != nil {
		t.Fatalf("cleanupFetchLog: %v", err)
	}
}

// TestLogBulkFetch verifies that LogBulkFetch inserts a row into fact_fetch_log.
// Uses a sentinel date (2099) for cleanup safety. Verifies via direct query rather than
// GetLastBulkFetchDate, since that function now uses fact_price_range MODE as primary.
func TestLogBulkFetch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	pool := getTestPool(t)
	priceRepo := repository.NewPriceRepository(pool)

	defer cleanupFetchLog(t)

	ctx := context.Background()
	fetchDate := testFetchLogSentinelBase

	if err := priceRepo.LogBulkFetch(ctx, fetchDate); err != nil {
		t.Fatalf("LogBulkFetch: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_fetch_log WHERE fetch_date = $1 AND fetch_type = 'BULK_PRICE_FETCH'`,
		fetchDate,
	).Scan(&count); err != nil {
		t.Fatalf("verify query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row in fact_fetch_log for %s, got %d", fetchDate.Format("2006-01-02"), count)
	}
}

// TestGetLastBulkFetchDate_PriceRangePrimary verifies that fact_price_range MODE is the
// primary watermark source, not fact_fetch_log MAX.
//
// Inserts a far-future sentinel (2099-01-01) into fact_fetch_log. Under the old MAX
// implementation this would have been returned as the watermark. Under the new
// MODE(fact_price_range) implementation it must be ignored — the result must come from
// fact_price_range and therefore be well below 2099.
func TestGetLastBulkFetchDate_PriceRangePrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	pool := getTestPool(t)
	priceRepo := repository.NewPriceRepository(pool)
	ctx := context.Background()

	// Skip if fact_price_range has no data — the primary source would be empty
	// and this test cannot verify MODE behavior without production votes.
	var rangeCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_price_range`).Scan(&rangeCount); err != nil {
		t.Fatalf("count fact_price_range: %v", err)
	}
	if rangeCount == 0 {
		t.Skip("fact_price_range is empty; skipping (no production data to test MODE)")
	}

	defer cleanupFetchLog(t)
	if err := priceRepo.LogBulkFetch(ctx, testFetchLogSentinelBase); err != nil {
		t.Fatalf("LogBulkFetch sentinel: %v", err)
	}

	got, err := priceRepo.GetLastBulkFetchDate(ctx)
	if err != nil {
		t.Fatalf("GetLastBulkFetchDate: %v", err)
	}

	// Must NOT return the sentinel — fact_fetch_log is no longer the primary.
	if !got.Before(testFetchLogSentinelBase) {
		t.Errorf("GetLastBulkFetchDate returned %s — fact_fetch_log sentinel should not influence primary",
			got.Format("2006-01-02"))
	}
	// Must return a meaningful date (not epoch).
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if got.Equal(epoch) {
		t.Error("GetLastBulkFetchDate returned epoch — expected a real watermark from fact_price_range")
	}
	t.Logf("watermark from fact_price_range MODE: %s", got.Format("2006-01-02"))
}


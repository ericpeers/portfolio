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

// TestGetLastBulkFetchDate_IgnoresFetchLog verifies that GetLastBulkFetchDate uses
// fact_price_range COUNT (not fact_fetch_log) as its watermark source.
//
// Inserts a far-future sentinel (2099-01-01) into fact_fetch_log. Under an old MAX
// implementation this would have corrupted the watermark. The current COUNT-based
// implementation queries fact_price_range exclusively — fact_fetch_log must be ignored.
//
// Skips if no end_date in fact_price_range has >= MinBulkFetchPrices rows, since there
// is no complete bulk fetch data to test against.
func TestGetLastBulkFetchDate_IgnoresFetchLog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	pool := getTestPool(t)
	priceRepo := repository.NewPriceRepository(pool)
	ctx := context.Background()

	// Skip if no complete bulk fetch exists in fact_price_range.
	var qualifyingCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT end_date FROM fact_price_range
			GROUP BY end_date HAVING COUNT(*) >= 30000
		) q`).Scan(&qualifyingCount); err != nil {
		t.Fatalf("count qualifying dates: %v", err)
	}
	if qualifyingCount == 0 {
		t.Skip("no end_date with >= 30000 rows in fact_price_range; skipping")
	}

	defer cleanupFetchLog(t)
	if err := priceRepo.LogBulkFetch(ctx, testFetchLogSentinelBase); err != nil {
		t.Fatalf("LogBulkFetch sentinel: %v", err)
	}

	got, err := priceRepo.GetLastBulkFetchDate(ctx)
	if err != nil {
		t.Fatalf("GetLastBulkFetchDate: %v", err)
	}

	// Must NOT return the sentinel — fact_fetch_log is never consulted.
	if !got.Before(testFetchLogSentinelBase) {
		t.Errorf("GetLastBulkFetchDate returned %s — fact_fetch_log sentinel should not influence result",
			got.Format("2006-01-02"))
	}
	// Must return a non-zero date (a real watermark from fact_price_range).
	if got.IsZero() {
		t.Error("GetLastBulkFetchDate returned zero — expected a real watermark from fact_price_range")
	}
	t.Logf("watermark from fact_price_range COUNT: %s", got.Format("2006-01-02"))
}


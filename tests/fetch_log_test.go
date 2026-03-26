package tests

import (
	"context"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/repository"
)

// cleanupFetchLog removes all BULK_PRICE_FETCH rows inserted during tests.
func cleanupFetchLog(t *testing.T) {
	t.Helper()
	pool := getTestPool(t)
	_, err := pool.Exec(context.Background(), `DELETE FROM fact_fetch_log WHERE fetch_type = 'BULK_PRICE_FETCH'`)
	if err != nil {
		t.Fatalf("cleanupFetchLog: %v", err)
	}
}

// TestLogBulkFetch verifies that LogBulkFetch inserts a row and GetLastBulkFetchDate reads it back.
func TestLogBulkFetch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	pool := getTestPool(t)
	priceRepo := repository.NewPriceRepository(pool)

	cleanupFetchLog(t)
	t.Cleanup(func() { cleanupFetchLog(t) })

	ctx := context.Background()
	fetchDate := time.Date(2025, 3, 14, 0, 0, 0, 0, time.UTC)

	if err := priceRepo.LogBulkFetch(ctx, fetchDate); err != nil {
		t.Fatalf("LogBulkFetch: %v", err)
	}

	got, err := priceRepo.GetLastBulkFetchDate(ctx)
	if err != nil {
		t.Fatalf("GetLastBulkFetchDate: %v", err)
	}
	if !got.Equal(fetchDate) {
		t.Errorf("got %s, want %s", got.Format("2006-01-02"), fetchDate.Format("2006-01-02"))
	}
}

// TestGetLastBulkFetchDate_ReturnsMax verifies that MAX is returned when multiple rows exist.
func TestGetLastBulkFetchDate_ReturnsMax(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	pool := getTestPool(t)
	priceRepo := repository.NewPriceRepository(pool)

	cleanupFetchLog(t)
	t.Cleanup(func() { cleanupFetchLog(t) })

	ctx := context.Background()
	dates := []time.Time{
		time.Date(2025, 3, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 12, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 14, 0, 0, 0, 0, time.UTC),
	}
	for _, d := range dates {
		if err := priceRepo.LogBulkFetch(ctx, d); err != nil {
			t.Fatalf("LogBulkFetch(%s): %v", d.Format("2006-01-02"), err)
		}
	}

	got, err := priceRepo.GetLastBulkFetchDate(ctx)
	if err != nil {
		t.Fatalf("GetLastBulkFetchDate: %v", err)
	}
	want := dates[len(dates)-1]
	if !got.Equal(want) {
		t.Errorf("got %s, want %s", got.Format("2006-01-02"), want.Format("2006-01-02"))
	}
}

// TestGetLastBulkFetchDate_FallbackToMode verifies that when fact_fetch_log is empty the
// function falls back to MODE() from fact_price_range. The live DB has real price data, so
// the fallback should return a non-epoch date.
func TestGetLastBulkFetchDate_FallbackToMode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	pool := getTestPool(t)
	priceRepo := repository.NewPriceRepository(pool)

	cleanupFetchLog(t)
	t.Cleanup(func() { cleanupFetchLog(t) })

	ctx := context.Background()

	// Confirm log is empty after cleanup.
	var logCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_fetch_log WHERE fetch_type = 'BULK_PRICE_FETCH'`).Scan(&logCount); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if logCount != 0 {
		t.Fatalf("expected empty fact_fetch_log, got %d rows", logCount)
	}

	got, err := priceRepo.GetLastBulkFetchDate(ctx)
	if err != nil {
		t.Fatalf("GetLastBulkFetchDate: %v", err)
	}
	t.Logf("fallback MODE() result: %s", got.Format("2006-01-02"))

	// If fact_price_range has rows, the fallback must return something meaningful.
	var rangeCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_price_range`).Scan(&rangeCount); err != nil {
		t.Fatalf("fact_price_range count: %v", err)
	}
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if rangeCount > 0 && got.Equal(epoch) {
		t.Errorf("fact_price_range has %d rows but GetLastBulkFetchDate returned epoch", rangeCount)
	}
}

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

// TestPriceRangeUpsert_BackfillPreservesNextUpdate verifies that writing a historical
// backfill does not overwrite a more recent next_update already in fact_price_range.
//
// Scenario:
//  1. A recent bulk fetch sets end_date=today and next_update=tomorrow (cache is fresh).
//  2. A backfill for a date 100 days ago is written with next_update=99 days ago (already past).
//  3. The row's next_update must still be tomorrow — the backfill must not regress it.
//
// Both UpsertPriceRange (singleton) and BatchUpsertPriceRange (bulk) are exercised
// because both had the same ON CONFLICT clause bug.
func TestPriceRangeUpsert_BackfillPreservesNextUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Parallel()

	pool := getTestPool(t)
	ctx := context.Background()

	ticker := "TSTBKFLLTST"
	secID, err := createTestStock(pool, ticker, "TST Backfill NextUpdate")
	if err != nil {
		t.Fatalf("createTestStock: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	priceRepo := repository.NewPriceRepository(pool)

	today := time.Now().UTC().Truncate(24 * time.Hour)
	futureNextUpdate := today.AddDate(0, 0, 1)     // tomorrow — cache is fresh
	historical := today.AddDate(0, 0, -100)        // 100 days ago
	pastNextUpdate := historical.AddDate(0, 0, 1)  // 99 days ago — backfill's stale next_update

	t.Run("UpsertPriceRange", func(t *testing.T) {
		// Step 1: simulate a fresh current-day bulk fetch.
		if err := priceRepo.UpsertPriceRange(ctx, secID,
			today.AddDate(0, -1, 0), today, futureNextUpdate); err != nil {
			t.Fatalf("initial UpsertPriceRange: %v", err)
		}

		// Step 2: backfill a historical day — next_update is already in the past.
		if err := priceRepo.UpsertPriceRange(ctx, secID,
			historical, historical, pastNextUpdate); err != nil {
			t.Fatalf("backfill UpsertPriceRange: %v", err)
		}

		// Step 3: next_update must not have been regressed by the backfill.
		var gotNextUpdate time.Time
		if err := pool.QueryRow(ctx,
			`SELECT next_update FROM fact_price_range WHERE security_id = $1`, secID,
		).Scan(&gotNextUpdate); err != nil {
			t.Fatalf("query next_update: %v", err)
		}
		if !gotNextUpdate.UTC().Equal(futureNextUpdate) {
			t.Errorf("UpsertPriceRange: next_update regressed by backfill\n  got:  %s\n  want: %s",
				gotNextUpdate.UTC().Format(time.RFC3339), futureNextUpdate.Format(time.RFC3339))
		}
	})

	t.Run("BatchUpsertPriceRange", func(t *testing.T) {
		// Reset between sub-tests.
		pool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id = $1`, secID)

		// Step 1: simulate a fresh current-day bulk fetch.
		if err := priceRepo.BatchUpsertPriceRange(ctx, []models.PriceRangeData{{
			SecurityID: secID,
			StartDate:  today.AddDate(0, -1, 0),
			EndDate:    today,
			NextUpdate: futureNextUpdate,
		}}); err != nil {
			t.Fatalf("initial BatchUpsertPriceRange: %v", err)
		}

		// Step 2: backfill a historical day — next_update is already in the past.
		if err := priceRepo.BatchUpsertPriceRange(ctx, []models.PriceRangeData{{
			SecurityID: secID,
			StartDate:  historical,
			EndDate:    historical,
			NextUpdate: pastNextUpdate,
		}}); err != nil {
			t.Fatalf("backfill BatchUpsertPriceRange: %v", err)
		}

		// Step 3: next_update must not have been regressed by the backfill.
		var gotNextUpdate time.Time
		if err := pool.QueryRow(ctx,
			`SELECT next_update FROM fact_price_range WHERE security_id = $1`, secID,
		).Scan(&gotNextUpdate); err != nil {
			t.Fatalf("query next_update: %v", err)
		}
		if !gotNextUpdate.UTC().Equal(futureNextUpdate) {
			t.Errorf("BatchUpsertPriceRange: next_update regressed by backfill\n  got:  %s\n  want: %s",
				gotNextUpdate.UTC().Format(time.RFC3339), futureNextUpdate.Format(time.RFC3339))
		}
	})
}

package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
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

	// A clearly historical date — well over a year in the past.
	backfillDate := time.Date(2024, 6, 3, 0, 0, 0, 0, time.UTC) // Monday

	bulk := &mockBulkFetcher{
		eodRecords: []providers.BulkEODRecord{
			{Code: ticker, Date: backfillDate, Open: 50, High: 52, Low: 49, Close: 51, AdjClose: 51, Volume: 5000},
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

	fetchDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC) // Monday

	// Mock returns only 2 EOD records — far below the 30,000 threshold.
	bulk := &mockBulkFetcher{
		eodRecords: []providers.BulkEODRecord{
			{Code: ticker1, Date: fetchDate, Open: 100, High: 105, Low: 99, Close: 102, AdjClose: 102, Volume: 1000},
			{Code: ticker2, Date: fetchDate, Open: 200, High: 205, Low: 199, Close: 202, AdjClose: 202, Volume: 2000},
		},
	}

	svc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
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

// countingBulkFetcher wraps mockBulkFetcher and records how many times GetBulkEOD is called.
type countingBulkFetcher struct {
	mockBulkFetcher
	calls atomic.Int32
}

func (c *countingBulkFetcher) GetBulkEOD(ctx context.Context, exchange string, date time.Time) ([]providers.BulkEODRecord, error) {
	c.calls.Add(1)
	return c.mockBulkFetcher.GetBulkEOD(ctx, exchange, date)
}

// newN2TestPrefetchService builds a PrefetchService backed by the test DB and the given
// bulk fetcher. The security sync hint is pre-set to targetDate so SyncSecurities is
// skipped; callers are responsible for setting the bulk-fetch and N-2 correction hints.
func newN2TestPrefetchService(t *testing.T, ctx context.Context, bulk providers.BulkFetcher, targetDate time.Time) *services.PrefetchService {
	t.Helper()
	pool := getTestPool(t)
	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	hintsRepo := repository.NewHintsRepository(pool)
	eodhdDummy := eodhd.NewClient("test-key", "http://localhost:9999")
	adminSvc := services.NewAdminService(secRepo, exchangeRepo, priceRepo, eodhdDummy, 1)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
		Bulk:     bulk,
	})

	// Pre-set the sync hint so maybeSyncSecurities is a no-op.
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastSecuritiesSyncDate, targetDate); err != nil {
		t.Fatalf("set sync hint: %v", err)
	}

	return services.NewPrefetchService(pricingSvc, secRepo, adminSvc, hintsRepo)
}

// saveAndRestoreHint saves the current value of a hint and restores it via t.Cleanup.
// This ensures test runs don't permanently alter production app_hints values.
func saveAndRestoreHint(t *testing.T, ctx context.Context, hintsRepo *repository.HintsRepository, key string) {
	t.Helper()
	saved, err := hintsRepo.GetDateHint(ctx, key)
	if err != nil {
		t.Fatalf("saveAndRestoreHint: read %q: %v", key, err)
	}
	t.Cleanup(func() {
		if saved.IsZero() {
			// Key was absent; delete it so the DB is back to the original state.
			pool := getTestPool(t)
			pool.Exec(context.Background(), `DELETE FROM app_hints WHERE key = $1`, key)
		} else {
			// Restore the original date. Use Exec directly to bypass GREATEST semantics.
			pool := getTestPool(t)
			pool.Exec(context.Background(),
				`INSERT INTO app_hints (key, value, updated_at) VALUES ($1, $2, NOW())
				 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
				key, saved.Format("2006-01-02"),
			)
		}
	})
}

// TestN2CorrectionFetchSkippedWhenHintCurrent verifies that doN2CorrectionFetch does NOT
// call BulkFetchPrices when HintLastN2CorrectionFetchDate is already set to the N-2 date.
func TestN2CorrectionFetchSkippedWhenHintCurrent(t *testing.T) {
	// Not parallel — modifies shared app_hints rows.
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)

	nyLoc, _ := time.LoadLocation("America/New_York")
	// target = Monday 2025-02-10, n2 = Friday 2025-02-07
	// now = Tuesday 2025-02-11 10am ET (after 6am D+1 cutoff, before 4:20pm partial cutoff)
	target := time.Date(2025, 2, 10, 0, 0, 0, 0, nyLoc)
	n2 := time.Date(2025, 2, 7, 0, 0, 0, 0, nyLoc)
	now := time.Date(2025, 2, 11, 10, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)

	// Cache is current through target, sync is done — only N-2 correction would fire.
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate, target); err != nil {
		t.Fatalf("set bulk hint: %v", err)
	}
	// N-2 hint is already set to n2 → guard should skip the fetch.
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastN2CorrectionFetchDate, n2); err != nil {
		t.Fatalf("set n2 hint: %v", err)
	}

	bulk := &countingBulkFetcher{}
	svc := newN2TestPrefetchService(t, ctx, bulk, target)
	svc.RunFetchAt(ctx, now)

	if n := bulk.calls.Load(); n != 0 {
		t.Errorf("expected 0 bulk fetch calls when N-2 hint is current, got %d", n)
	}
}

// TestN2CorrectionFetchFiresWhenHintAbsent verifies that doN2CorrectionFetch DOES call
// BulkFetchPrices when HintLastN2CorrectionFetchDate is absent (or older than N-2).
func TestN2CorrectionFetchFiresWhenHintAbsent(t *testing.T) {
	// Not parallel — modifies shared app_hints rows.
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)

	nyLoc, _ := time.LoadLocation("America/New_York")
	target := time.Date(2025, 2, 10, 0, 0, 0, 0, nyLoc)
	n2 := time.Date(2025, 2, 7, 0, 0, 0, 0, nyLoc)
	now := time.Date(2025, 2, 11, 10, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)

	if err := hintsRepo.SetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate, target); err != nil {
		t.Fatalf("set bulk hint: %v", err)
	}
	// N-2 hint absent → guard should allow the fetch.
	pool.Exec(ctx, `DELETE FROM app_hints WHERE key = $1`, repository.HintLastN2CorrectionFetchDate)

	bulk := &countingBulkFetcher{}
	svc := newN2TestPrefetchService(t, ctx, bulk, target)
	svc.RunFetchAt(ctx, now)

	if n := bulk.calls.Load(); n == 0 {
		t.Error("expected at least one bulk fetch call when N-2 hint is absent, got 0")
	}

	// Hint is NOT updated after a failed fetch (mock returns 0 records < minRequired).
	// Verify it remains absent — the next poll will retry.
	written, err := hintsRepo.GetDateHint(ctx, repository.HintLastN2CorrectionFetchDate)
	if err != nil {
		t.Fatalf("read n2 hint: %v", err)
	}
	if !written.IsZero() {
		t.Errorf("N-2 hint should not be written after a failed fetch, got %s", written.Format("2006-01-02"))
	}
	_ = n2 // confirmed above via bulk.calls; n2 used to set up test intent
}

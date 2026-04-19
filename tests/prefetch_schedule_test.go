package tests

import (
	"context"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

// forceSetDateHint writes a date hint directly, bypassing the GREATEST-semantics
// of SetDateHint. Required when a test needs to set a hint to a historical date that
// is earlier than the current production value.
func forceSetDateHint(t *testing.T, key string, date time.Time) {
	t.Helper()
	pool := getTestPool(t)
	_, err := pool.Exec(context.Background(),
		`INSERT INTO app_hints (key, value, updated_at) VALUES ($1, $2, NOW())
		 ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()`,
		key, date.Format("2006-01-02"),
	)
	if err != nil {
		t.Fatalf("forceSetDateHint %s=%s: %v", key, date.Format("2006-01-02"), err)
	}
}

// newSyncTestPrefetchService builds a PrefetchService without pre-setting the sync hint,
// so tests can control it themselves.
func newSyncTestPrefetchService(t *testing.T, ctx context.Context, bulk providers.BulkFetcher) *services.PrefetchService {
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
	return services.NewPrefetchService(pricingSvc, secRepo, adminSvc, hintsRepo)
}

// --- maybePartialFetch tests ---

// TestPartialFetchSkipsBeforeCutoff verifies that no bulk fetch is triggered before
// the 4:20pm ET market-data-ready cutoff.
func TestPartialFetchSkipsBeforeCutoff(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	// Monday 2025-02-10 at 3:00pm ET — trading day but before 4:20pm cutoff.
	now := time.Date(2025, 2, 10, 15, 0, 0, 0, nyLoc)
	target := time.Date(2025, 2, 10, 0, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	if err := hintsRepo.SetDateHint(ctx, repository.HintLastSecuritiesSyncDate, target); err != nil {
		t.Fatalf("set sync hint: %v", err)
	}
	// Set bulk hint to target so maybeCompleteFetch also skips.
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate, target); err != nil {
		t.Fatalf("set bulk hint: %v", err)
	}

	bulk := &countingBulkFetcher{}
	svc := newSyncTestPrefetchService(t, ctx, bulk)
	svc.RunFetchAt(ctx, now)

	if n := bulk.calls.Load(); n != 0 {
		t.Errorf("expected 0 bulk calls before 4:20pm cutoff, got %d", n)
	}
}

// TestPartialFetchSkipsOnWeekend verifies that no bulk fetch fires on a weekend.
func TestPartialFetchSkipsOnWeekend(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	// Saturday 2025-02-08 at 5:00pm ET.
	now := time.Date(2025, 2, 8, 17, 0, 0, 0, nyLoc)
	// Complete-fetch target would be Friday 2025-02-07 but data not published until 6am Sat.
	// We set bulk hint to Friday so maybeCompleteFetch skips too.
	friday := time.Date(2025, 2, 7, 0, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	if err := hintsRepo.SetDateHint(ctx, repository.HintLastSecuritiesSyncDate, friday); err != nil {
		t.Fatalf("set sync hint: %v", err)
	}
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate, friday); err != nil {
		t.Fatalf("set bulk hint: %v", err)
	}
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastN2CorrectionFetchDate, friday); err != nil {
		t.Fatalf("set n2 hint: %v", err)
	}

	bulk := &countingBulkFetcher{}
	svc := newSyncTestPrefetchService(t, ctx, bulk)
	svc.RunFetchAt(ctx, now)

	if n := bulk.calls.Load(); n != 0 {
		t.Errorf("expected 0 bulk calls on weekend, got %d", n)
	}
}

// TestPartialFetchSkipsWhenHintCurrent verifies that no bulk fetch fires when the
// partial-fetch hint already records today.
func TestPartialFetchSkipsWhenHintCurrent(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	// Monday 2025-02-10 at 5:00pm ET — after cutoff, hint already set to today.
	today := time.Date(2025, 2, 10, 0, 0, 0, 0, nyLoc)
	now := time.Date(2025, 2, 10, 17, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	if err := hintsRepo.SetDateHint(ctx, repository.HintLastUSPartialPriceFetchDate, today); err != nil {
		t.Fatalf("set partial hint: %v", err)
	}
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastSecuritiesSyncDate, today); err != nil {
		t.Fatalf("set sync hint: %v", err)
	}
	// Complete-fetch path: cache current through today, n2 also done.
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate, today); err != nil {
		t.Fatalf("set bulk hint: %v", err)
	}
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastN2CorrectionFetchDate, today); err != nil {
		t.Fatalf("set n2 hint: %v", err)
	}

	bulk := &countingBulkFetcher{}
	svc := newSyncTestPrefetchService(t, ctx, bulk)
	svc.RunFetchAt(ctx, now)

	if n := bulk.calls.Load(); n != 0 {
		t.Errorf("expected 0 bulk calls when partial hint is current, got %d", n)
	}
}

// TestPartialFetchFiresWhenHintAbsent verifies that a bulk fetch fires after 4:20pm
// on a trading day when no partial-fetch hint exists. Uses minRequired=0 so the mock
// returning 0 records is still treated as success.
func TestPartialFetchFiresWhenHintAbsent(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	// Monday 2025-02-10 at 5:00pm ET — after 4:20pm cutoff.
	today := time.Date(2025, 2, 10, 0, 0, 0, 0, nyLoc)
	now := time.Date(2025, 2, 10, 17, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	// Delete partial hint so the guard allows the fetch.
	pool.Exec(ctx, `DELETE FROM app_hints WHERE key = $1`, repository.HintLastUSPartialPriceFetchDate)
	// Set remaining hints so only maybePartialFetch fires.
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastSecuritiesSyncDate, today); err != nil {
		t.Fatalf("set sync hint: %v", err)
	}
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate, today); err != nil {
		t.Fatalf("set bulk hint: %v", err)
	}
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastN2CorrectionFetchDate, today); err != nil {
		t.Fatalf("set n2 hint: %v", err)
	}

	bulk := &countingBulkFetcher{}
	svc := newSyncTestPrefetchService(t, ctx, bulk)
	svc.RunFetchAt(ctx, now)

	if n := bulk.calls.Load(); n != 1 {
		t.Errorf("expected exactly 1 partial fetch call when hint is absent, got %d", n)
	}

	// Hint should be written after successful fetch (mock returns 0 records but minRequired=0).
	written, err := hintsRepo.GetDateHint(ctx, repository.HintLastUSPartialPriceFetchDate)
	if err != nil {
		t.Fatalf("read partial hint: %v", err)
	}
	wantDate := today.Format("2006-01-02")
	gotDate := time.Date(written.Year(), written.Month(), written.Day(), 0, 0, 0, 0, nyLoc).Format("2006-01-02")
	if gotDate != wantDate {
		t.Errorf("partial hint: want %s, got %s", wantDate, gotDate)
	}
}

// --- maybeCompleteFetch tests ---

// TestCompleteFetchSkipsBeforeSixAM verifies that the complete-fetch path skips when
// the wall clock is before 6am ET on D+1 (data not yet published by EODHD).
func TestCompleteFetchSkipsBeforeSixAM(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	// Target = Monday 2025-02-10. D+1 = Tuesday 2025-02-11.
	// now = 2025-02-11 05:59 ET — one minute before the 6am gate.
	friday := time.Date(2025, 2, 7, 0, 0, 0, 0, nyLoc)
	now := time.Date(2025, 2, 11, 5, 59, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	// Bulk hint behind (Friday) to ensure it would fire if the gate were absent.
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate, friday); err != nil {
		t.Fatalf("set bulk hint: %v", err)
	}
	if err := hintsRepo.SetDateHint(ctx, repository.HintLastSecuritiesSyncDate, friday); err != nil {
		t.Fatalf("set sync hint: %v", err)
	}
	// now is before 4:20pm so partial fetch also won't fire.

	bulk := &countingBulkFetcher{}
	svc := newSyncTestPrefetchService(t, ctx, bulk)
	svc.RunFetchAt(ctx, now)

	if n := bulk.calls.Load(); n != 0 {
		t.Errorf("expected 0 bulk calls before 6am D+1 gate, got %d", n)
	}
}

// TestCompleteFetchGapFillFires verifies that the gap-fill loop fires when the bulk-fetch
// hint is behind the target date. The mock returns 0 records (< minRequired), so each
// attempt fails and the loop breaks after the first call — but the call confirms the path
// was entered.
func TestCompleteFetchGapFillFires(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	// Hint = 2025-02-05 (Wednesday), target = Monday 2025-02-10 → 3-day gap.
	// now = 2025-02-11 10:00 ET (after 6am D+1 gate).
	lastFetched := time.Date(2025, 2, 5, 0, 0, 0, 0, nyLoc)
	now := time.Date(2025, 2, 11, 10, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	// Force-set to historical dates, bypassing GREATEST semantics.
	forceSetDateHint(t, repository.HintLastUSBulkPriceFetchDate, lastFetched)
	forceSetDateHint(t, repository.HintLastSecuritiesSyncDate, lastFetched)
	// now is before 4:20pm so partial fetch skips.

	bulk := &countingBulkFetcher{}
	svc := newSyncTestPrefetchService(t, ctx, bulk)
	svc.RunFetchAt(ctx, now)

	// Mock returns 0 records; first gap-fill call fails (< MinBulkFetchPrices), loop breaks.
	if n := bulk.calls.Load(); n < 1 {
		t.Errorf("expected ≥1 bulk call for gap-fill, got %d", n)
	}
}

// TestCompleteFetchTooLargeGap verifies that when the hint is >30 trading days behind,
// the service logs a warning and skips the fetch entirely.
func TestCompleteFetchTooLargeGap(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	// Hint = 2024-12-01, now = 2025-02-11 10:00 ET → far more than 30 trading days.
	veryOld := time.Date(2024, 12, 1, 0, 0, 0, 0, nyLoc)
	now := time.Date(2025, 2, 11, 10, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	// Force-set to historical dates, bypassing GREATEST semantics.
	forceSetDateHint(t, repository.HintLastUSBulkPriceFetchDate, veryOld)
	forceSetDateHint(t, repository.HintLastSecuritiesSyncDate, veryOld)
	// now is before 4:20pm so partial fetch skips.

	bulk := &countingBulkFetcher{}
	svc := newSyncTestPrefetchService(t, ctx, bulk)
	svc.RunFetchAt(ctx, now)

	if n := bulk.calls.Load(); n != 0 {
		t.Errorf("expected 0 bulk calls when gap >30 trading days, got %d", n)
	}
}

// --- maybeSyncSecurities test ---

// TestSyncSecuritiesFiresWhenHintOld verifies that the security sync is attempted when
// the sync hint is older than the target date. The admin service uses a dead URL so
// SyncSecurities fails gracefully — the test just confirms the code path is reached
// without a panic and that the service continues normally.
func TestSyncSecuritiesFiresWhenHintOld(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	// Sync hint = 2025-02-09 (yesterday), target = 2025-02-10, now = 2025-02-11 10:00 ET.
	yesterday := time.Date(2025, 2, 9, 0, 0, 0, 0, nyLoc)
	target := time.Date(2025, 2, 10, 0, 0, 0, 0, nyLoc)
	now := time.Date(2025, 2, 11, 10, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	// Force-set hints to historical dates, bypassing GREATEST semantics.
	// Sync hint = yesterday so maybeSyncSecurities fires.
	forceSetDateHint(t, repository.HintLastSecuritiesSyncDate, yesterday)
	// Bulk hint = target so the cache is considered current (no gap fill).
	forceSetDateHint(t, repository.HintLastUSBulkPriceFetchDate, target)
	// n2 hint = n2 date (Feb 7) so doN2CorrectionFetch skips; isolates the sync assertion.
	n2 := time.Date(2025, 2, 7, 0, 0, 0, 0, nyLoc)
	forceSetDateHint(t, repository.HintLastN2CorrectionFetchDate, n2)

	bulk := &countingBulkFetcher{}
	// newSyncTestPrefetchService does NOT pre-set the sync hint, so our yesterday value is used.
	svc := newSyncTestPrefetchService(t, ctx, bulk)

	// Should not panic — SyncSecurities fails gracefully with dead URL.
	svc.RunFetchAt(ctx, now)

	// Sync hint should NOT be updated because SyncSecurities failed.
	syncHint, err := hintsRepo.GetDateHint(ctx, repository.HintLastSecuritiesSyncDate)
	if err != nil {
		t.Fatalf("read sync hint: %v", err)
	}
	syncDate := time.Date(syncHint.Year(), syncHint.Month(), syncHint.Day(), 0, 0, 0, 0, nyLoc)
	if !syncDate.Equal(yesterday) {
		t.Errorf("sync hint should remain at yesterday after failed sync; got %s", syncDate.Format("2006-01-02"))
	}
}

// TestSyncSecuritiesFiresWhenHintAbsent verifies the maybeSyncSecurities code path where
// the sync hint is completely absent (IsZero == true), causing the date-comparison block
// to be skipped and the sync to fire directly.
func TestSyncSecuritiesFiresWhenHintAbsent(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	target := time.Date(2025, 2, 10, 0, 0, 0, 0, nyLoc)
	now := time.Date(2025, 2, 11, 10, 0, 0, 0, nyLoc)
	n2 := time.Date(2025, 2, 7, 0, 0, 0, 0, nyLoc)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	// Delete the sync hint entirely — IsZero() path.
	pool.Exec(ctx, `DELETE FROM app_hints WHERE key = $1`, repository.HintLastSecuritiesSyncDate)
	forceSetDateHint(t, repository.HintLastUSBulkPriceFetchDate, target)
	forceSetDateHint(t, repository.HintLastN2CorrectionFetchDate, n2)

	bulk := &countingBulkFetcher{}
	svc := newSyncTestPrefetchService(t, ctx, bulk)
	// Should not panic — SyncSecurities fails gracefully with dead admin URL.
	svc.RunFetchAt(ctx, now)
}

// --- goroutine lifecycle test ---

// TestPrefetchServiceWarmingDoneCloses verifies that Start() eventually closes the
// warmingDone channel and that WarmingDone() returns it. All hints are pre-set so no
// real bulk fetches are triggered; the context is cancelled immediately after warming
// to prevent the 5-minute poll loop from running.
func TestPrefetchServiceWarmingDoneCloses(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	hintsRepo := repository.NewHintsRepository(pool)
	nyLoc, _ := time.LoadLocation("America/New_York")

	today := time.Now().In(nyLoc).Truncate(24 * time.Hour)

	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSPartialPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastUSBulkPriceFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastN2CorrectionFetchDate)
	saveAndRestoreHint(t, ctx, hintsRepo, repository.HintLastSecuritiesSyncDate)

	// Pre-set all hints to today to prevent any real fetches from doFetch.
	for _, key := range []string{
		repository.HintLastUSPartialPriceFetchDate,
		repository.HintLastUSBulkPriceFetchDate,
		repository.HintLastN2CorrectionFetchDate,
		repository.HintLastSecuritiesSyncDate,
	} {
		if err := hintsRepo.SetDateHint(ctx, key, today); err != nil {
			t.Fatalf("set hint %s: %v", key, err)
		}
	}

	bulk := &countingBulkFetcher{}
	svc := newSyncTestPrefetchService(t, ctx, bulk)

	runCtx, cancel := context.WithCancel(ctx)
	svc.Start(runCtx)

	select {
	case <-svc.WarmingDone():
		// Success — channel closed as expected.
	case <-time.After(5 * time.Second):
		t.Fatal("WarmingDone channel was not closed within 5 seconds")
	}

	// Cancel immediately to stop the 5-minute poll loop.
	cancel()
}

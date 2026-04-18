package services

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

// PrefetchService keeps the price cache warm in the background via a single goroutine
// that runs once at startup (startup catch-up) and then polls every 5 minutes.
//
// On each poll it runs two independent fetch paths:
//
//   - 4:20pm ET partial fetch (same trading day): EODHD guarantees NYSE/NASDAQ data
//     within 15 minutes of the 4pm close. Runs with minRequired=0 since the response
//     may not yet reach the full-exchange threshold. Recorded in app_hints so it only
//     fires once per afternoon even across restarts.
//
//   - 6am ET complete fetch (D+1): gap-fills any missing trading days up to yesterday
//     (30-day limit), then always re-fetches N-2 to pick up EODHD backfill corrections.
//     The watermark is stored in app_hints (O(1) lookup) with a bootstrap fallback to
//     GetLastBulkFetchDate when the hint is absent (first run after migration).
//
// Security sync runs once per trading day (before the complete fetch), persisted in
// app_hints so a restart mid-day does not trigger a redundant sync.
type PrefetchService struct {
	pricingSvc  *PricingService
	secRepo     *repository.SecurityRepository
	adminSvc    *AdminService
	hintsRepo   *repository.HintsRepository
	warmingDone chan struct{}
}

func NewPrefetchService(
	pricingSvc *PricingService,
	secRepo *repository.SecurityRepository,
	adminSvc *AdminService,
	hintsRepo *repository.HintsRepository,
) *PrefetchService {
	return &PrefetchService{
		pricingSvc:  pricingSvc,
		secRepo:     secRepo,
		adminSvc:    adminSvc,
		hintsRepo:   hintsRepo,
		warmingDone: make(chan struct{}),
	}
}

// WarmingDone returns a channel that is closed when the startup warm-up completes.
func (s *PrefetchService) WarmingDone() <-chan struct{} { return s.warmingDone }

// Start launches the background prefetch goroutine. Call once from main after service init.
func (s *PrefetchService) Start(ctx context.Context) {
	go s.run(ctx)
}

func (s *PrefetchService) run(ctx context.Context) {
	// Warm the security snapshot before closing warmingDone. Ordering matters:
	// WarmingMiddleware unblocks as soon as the channel closes, so the snapshot
	// must be ready before the first request is served.
	if _, _, err := s.secRepo.GetAllSecurities(ctx); err != nil {
		log.Warnf("PrefetchService: failed to warm security snapshot: %v", err)
	} else {
		log.Info("PrefetchService: security snapshot warmed")
	}
	close(s.warmingDone)

	// Run immediately on startup (handles catch-up after downtime), then poll every 5 minutes.
	s.doFetch(ctx)
	for {
		select {
		case <-time.After(5 * time.Minute):
		case <-ctx.Done():
			return
		}
		s.doFetch(ctx)
	}
}

// doFetch runs both fetch paths for the current moment. Called once at startup and
// then on each 5-minute tick. The poll loop is single-goroutine, so concurrent
// calls to doFetch cannot occur — no mutex is needed.
func (s *PrefetchService) doFetch(ctx context.Context) {
	nyLoc, _ := time.LoadLocation("America/New_York")
	now := time.Now().In(nyLoc)
	s.maybePartialFetch(ctx, now, nyLoc)
	s.maybeCompleteFetch(ctx, now, nyLoc)
}

// minSecuritiesForBulkFetch is the minimum number of US securities that must be in
// the DB before bulk fetches are attempted. Guards against fetching before the
// initial import has run.
const minSecuritiesForBulkFetch = 1000

// maybePartialFetch runs the 4:20pm ET same-day partial bulk fetch.
// Fires once per trading afternoon; minRequired=0 accepts partial EODHD responses.
func (s *PrefetchService) maybePartialFetch(ctx context.Context, now time.Time, nyLoc *time.Location) {
	if !IsTradingDay(now) {
		return
	}
	cutoff := time.Date(now.Year(), now.Month(), now.Day(), marketDataReadyHour, marketDataReadyMinute, 0, 0, nyLoc)
	if now.Before(cutoff) {
		return
	}
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, nyLoc)

	lastPartial, err := s.hintsRepo.GetDateHint(ctx, repository.HintLastUSPartialPriceFetchDate)
	if err != nil {
		log.Warnf("PrefetchService: could not read partial fetch hint: %v", err)
		return
	}
	if !lastPartial.IsZero() {
		lastPartialDate := time.Date(lastPartial.Year(), lastPartial.Month(), lastPartial.Day(), 0, 0, 0, 0, nyLoc)
		if !lastPartialDate.Before(today) {
			log.Debugf("PrefetchService: partial fetch already done for %s", today.Format("2006-01-02"))
			return
		}
	}

	secsByTicker, ok := s.loadSecsByTicker(ctx, "partial fetch")
	if !ok {
		return
	}

	log.Infof("PrefetchService: 4:20pm ET partial fetch for %s", today.Format("2006-01-02"))
	if _, err := s.pricingSvc.BulkFetchPrices(ctx, "US", today, secsByTicker, 0); err != nil {
		log.Warnf("PrefetchService: partial fetch failed for %s: %v", today.Format("2006-01-02"), err)
		return
	}
	if err := s.hintsRepo.SetDateHint(ctx, repository.HintLastUSPartialPriceFetchDate, today); err != nil {
		log.Warnf("PrefetchService: failed to update partial fetch hint: %v", err)
	}
}

// maybeCompleteFetch runs the 6am ET gap-fill and N-2 correction sweep.
func (s *PrefetchService) maybeCompleteFetch(ctx context.Context, now time.Time, nyLoc *time.Location) {
	target := LastMarketClose(now)
	targetDate := time.Date(target.Year(), target.Month(), target.Day(), 0, 0, 0, 0, nyLoc)

	// Guard: wait until 6am ET on D+1 before treating the complete dataset as available.
	nextDay := targetDate.AddDate(0, 0, 1)
	dataPublishedAt := time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(), 6, 0, 0, 0, nyLoc)
	if now.Before(dataPublishedAt) {
		log.Debugf("PrefetchService: target %s data not yet available (< 6am ET %s), skipping",
			targetDate.Format("2006-01-02"), nextDay.Format("2006-01-02"))
		return
	}

	s.maybeSyncSecurities(ctx, targetDate, nyLoc)

	// Watermark: app_hints is canonical; fall back to GetLastBulkFetchDate for bootstrap.
	lastFetched, err := s.hintsRepo.GetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate)
	if err != nil {
		log.Warnf("PrefetchService: could not read bulk fetch hint: %v", err)
		return
	}
	if lastFetched.IsZero() {
		lastFetched, err = s.pricingSvc.priceRepo.GetLastBulkFetchDate(ctx)
		if err != nil {
			log.Warnf("PrefetchService: could not read last bulk fetch date: %v", err)
			return
		}
	}

	secsByTicker, ok := s.loadSecsByTicker(ctx, "complete fetch")
	if !ok {
		return
	}

	if lastFetched.IsZero() {
		log.Infof("PrefetchService: no prior bulk fetch recorded, bootstrapping with %s",
			targetDate.Format("2006-01-02"))
		if _, err := s.pricingSvc.BulkFetchPrices(ctx, "US", targetDate, secsByTicker, models.MinBulkFetchPrices); err != nil {
			log.Errorf("PrefetchService: bootstrap fetch failed: %v", err)
			return
		}
		if err := s.hintsRepo.SetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate, targetDate); err != nil {
			log.Warnf("PrefetchService: failed to update bulk fetch hint: %v", err)
		}
		return
	}

	lastFetchedDate := time.Date(lastFetched.Year(), lastFetched.Month(), lastFetched.Day(), 0, 0, 0, 0, nyLoc)

	if !lastFetchedDate.Before(targetDate) {
		log.Debugf("PrefetchService: cache is current (last=%s), running N-2 correction only",
			lastFetchedDate.Format("2006-01-02"))
		s.doN2CorrectionFetch(ctx, target, secsByTicker)
		return
	}

	daysToPrefetch := countTradingDays(lastFetchedDate, targetDate)
	if daysToPrefetch > 30 {
		log.Warnf("PrefetchService: skipping — %d trading days to fetch exceeds the 30-day limit. "+
			"Use GET /admin/bulk-fetch-eodhd-prices to backfill historical data.", daysToPrefetch)
		return
	}

	log.Infof("PrefetchService: fetching %d missing trading day(s) (last=%s, target=%s)",
		daysToPrefetch, lastFetchedDate.Format("2006-01-02"), targetDate.Format("2006-01-02"))

	for d := NextTradingDay(lastFetchedDate); !d.After(targetDate); d = NextTradingDay(d) {
		if ctx.Err() != nil {
			return
		}
		log.Infof("PrefetchService: bulk fetch for %s", d.Format("2006-01-02"))
		if _, err := s.pricingSvc.BulkFetchPrices(ctx, "US", d, secsByTicker, models.MinBulkFetchPrices); err != nil {
			log.Errorf("PrefetchService: bulk fetch failed for %s: %v", d.Format("2006-01-02"), err)
			return // retry this day on the next 5-minute tick
		}
		if err := s.hintsRepo.SetDateHint(ctx, repository.HintLastUSBulkPriceFetchDate, d); err != nil {
			log.Warnf("PrefetchService: failed to update bulk fetch hint: %v", err)
		}
	}

	s.doN2CorrectionFetch(ctx, target, secsByTicker)
}

// doN2CorrectionFetch re-fetches N-2 (trading day before target) to pick up any
// EODHD backfill corrections published after the original fetch. Non-fatal on error.
func (s *PrefetchService) doN2CorrectionFetch(ctx context.Context, target time.Time, secsByTicker map[string]*models.Security) {
	n2 := PreviousMarketDay(target)
	log.Infof("PrefetchService: N-2 correction fetch for %s", n2.Format("2006-01-02"))
	if _, err := s.pricingSvc.BulkFetchPrices(ctx, "US", n2, secsByTicker, models.MinBulkFetchPrices); err != nil {
		log.Warnf("PrefetchService: N-2 correction fetch failed for %s (non-fatal): %v", n2.Format("2006-01-02"), err)
	}
}

// maybeSyncSecurities syncs the security list from EODHD once per trading day.
// Persists the sync date to app_hints so restarts don't trigger a redundant sync.
func (s *PrefetchService) maybeSyncSecurities(ctx context.Context, targetDate time.Time, nyLoc *time.Location) {
	lastSync, err := s.hintsRepo.GetDateHint(ctx, repository.HintLastSecuritiesSyncDate)
	if err != nil {
		log.Warnf("PrefetchService: could not read sync hint: %v", err)
		return
	}
	if !lastSync.IsZero() {
		lastSyncDate := time.Date(lastSync.Year(), lastSync.Month(), lastSync.Day(), 0, 0, 0, 0, nyLoc)
		if !lastSyncDate.Before(targetDate) {
			return // already synced for this trading day
		}
	}

	log.Infof("PrefetchService: syncing securities from provider for %s", targetDate.Format("2006-01-02"))
	result, err := s.adminSvc.SyncSecurities(ctx, false)
	if err != nil {
		log.Warnf("PrefetchService: security sync failed: %v", err)
		return // continue — a failed sync should not block the price fetch
	}
	log.Infof("PrefetchService: security sync complete: inserted=%d skipped=%d errors=%d",
		result.SecuritiesInserted, result.SecuritiesSkipped, len(result.Errors))
	if err := s.hintsRepo.SetDateHint(ctx, repository.HintLastSecuritiesSyncDate, targetDate); err != nil {
		log.Warnf("PrefetchService: failed to update sync hint: %v", err)
	}
}

// loadSecsByTicker loads US securities and builds the ticker→Security map needed by
// BulkFetchPrices. Returns false and logs a warning when the DB looks under-populated.
func (s *PrefetchService) loadSecsByTicker(ctx context.Context, caller string) (map[string]*models.Security, bool) {
	allSecs, err := s.secRepo.GetAllUS(ctx)
	if err != nil {
		log.Warnf("PrefetchService: %s: could not load securities: %v", caller, err)
		return nil, false
	}
	if len(allSecs) < minSecuritiesForBulkFetch {
		log.Warnf("PrefetchService: %s: skipping — only %d securities in DB (need ≥%d). "+
			"Import securities first, then use GET /admin/bulk-fetch-eodhd-prices to backfill.", caller, len(allSecs), minSecuritiesForBulkFetch)
		return nil, false
	}
	return buildSecsByTicker(allSecs), true
}

// buildSecsByTicker converts a slice of Security pointers into a ticker→Security map
// for O(1) lookup during bulk fetch record matching.
func buildSecsByTicker(secs []*models.Security) map[string]*models.Security {
	m := make(map[string]*models.Security, len(secs))
	for _, s := range secs {
		m[s.Ticker] = s
	}
	return m
}

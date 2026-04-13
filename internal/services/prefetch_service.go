package services

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

// PrefetchService keeps the price cache warm in the background via a single goroutine
// that runs once at startup (startup catch-up) and then polls every 5 minutes (nightly).
//
// On each poll it:
//   - Computes the last market close as the target date
//   - Skips if EODHD hasn't published data yet (before 4am ET on the day after the target)
//   - Reads the watermark from fact_price_range to find the last complete bulk fetch
//   - Bulk-fetches any trading days missing between the watermark and the target
//   - Stops on the first fetch error and retries the same day on the next tick
//
// Both startup catch-up and ongoing nightly fetches use the same logic, eliminating
// the asymmetry between the former runCatchup and runNightly goroutines.
type PrefetchService struct {
	pricingSvc  *PricingService
	secRepo     *repository.SecurityRepository
	warmingDone chan struct{}
}

func NewPrefetchService(pricingSvc *PricingService, secRepo *repository.SecurityRepository) *PrefetchService {
	return &PrefetchService{
		pricingSvc:  pricingSvc,
		secRepo:     secRepo,
		warmingDone: make(chan struct{}),
	}
}

// WarmingDone returns a channel that is closed when the startup warm-up completes.
// Middleware may select on this channel to block requests until the cache is warm.
func (s *PrefetchService) WarmingDone() <-chan struct{} { return s.warmingDone }

// Start launches the background prefetch goroutine. Call once from main after service init.
func (s *PrefetchService) Start(ctx context.Context) {
	go s.run(ctx)
}

func (s *PrefetchService) run(ctx context.Context) {
	// Warm the security snapshot first so it is ready before any request is served,
	// regardless of whether price catch-up proceeds below.
	if _, _, err := s.secRepo.GetAllSecurities(ctx); err != nil {
		log.Warnf("PrefetchService: failed to warm security snapshot: %v", err)
	} else {
		log.Info("PrefetchService: security snapshot warmed")
	}

	// Unblock WarmingMiddleware now that the security snapshot is ready.
	// Price fetching continues asynchronously; callers should not wait on it.
	close(s.warmingDone)

	// Run fetch immediately on startup (handles catch-up after sleep/resume),
	// then poll every 5 minutes.
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

// doFetch checks whether any trading days need bulk-fetching and fetches them in order.
// It is safe to call concurrently with itself only if calls are serialized (the poll loop
// ensures this — a new tick is never dispatched while the previous doFetch is running).
func (s *PrefetchService) doFetch(ctx context.Context) {
	nyLoc, _ := time.LoadLocation("America/New_York")
	now := time.Now()

	target := LastMarketClose(now)
	targetDate := time.Date(target.Year(), target.Month(), target.Day(), 0, 0, 0, 0, nyLoc)

	// EODHD bulk data for trading day D is reliably published by 4am ET on D+1.
	// Skip if that window hasn't opened yet — avoids an API call that would fail
	// BulkFetchPrices' completeness check and be retried on the next tick anyway.
	nextDay := targetDate.AddDate(0, 0, 1)
	dataPublishedAt := time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(), 4, 0, 0, 0, nyLoc)
	if now.Before(dataPublishedAt) {
		log.Debugf("PrefetchService: target %s data not yet published (< 4am ET %s), skipping",
			targetDate.Format("2006-01-02"), nextDay.Format("2006-01-02"))
		return
	}

	lastFetched, err := s.pricingSvc.priceRepo.GetLastBulkFetchDate(ctx)
	if err != nil {
		log.Warnf("PrefetchService: could not read last bulk fetch date: %v", err)
		return
	}

	// Load securities and guard against a sparse/cold DB before any fetch attempt
	// (including bootstrap). This avoids hammering EODHD before the initial import.
	allSecs, err := s.secRepo.GetAllUS(ctx)
	if err != nil {
		log.Warnf("PrefetchService: could not load securities: %v", err)
		return
	}
	if len(allSecs) < 1000 {
		log.Warnf("PrefetchService: skipping — only %d securities in DB (need ≥1000). "+
			"Import securities first, then use GET /admin/bulk-fetch-eodhd-prices to backfill.", len(allSecs))
		return
	}

	secsByTicker := buildSecsByTicker(allSecs)

	// Bootstrap: no complete bulk fetch has ever been recorded. Fetch only targetDate
	// (one day). If it fails, the next 5-minute tick retries. Once it succeeds,
	// GetLastBulkFetchDate will return targetDate and normal catch-up takes over.
	if lastFetched.IsZero() {
		log.Infof("PrefetchService: no prior bulk fetch recorded, bootstrapping with %s",
			targetDate.Format("2006-01-02"))
		if _, err := s.pricingSvc.BulkFetchPrices(ctx, "US", targetDate, secsByTicker, models.MinBulkFetchPrices); err != nil {
			log.Errorf("PrefetchService: bootstrap fetch failed: %v", err)
		}
		return
	}

	// Normalize lastFetched (pgx returns Postgres DATE as midnight UTC) to midnight NY
	// so all subsequent comparisons use a consistent timezone.
	// The calendar date is preserved: pgx encodes it in the Year/Month/Day UTC fields.
	lastFetchedDate := time.Date(lastFetched.Year(), lastFetched.Month(), lastFetched.Day(), 0, 0, 0, 0, nyLoc)

	if !lastFetchedDate.Before(targetDate) {
		log.Debugf("PrefetchService: cache is current (end_date=%s), no fetch needed", lastFetchedDate.Format("2006-01-02"))
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
			break // retry this day on the next 5-minute tick
		}
	}
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

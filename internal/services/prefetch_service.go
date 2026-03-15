package services

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

// PrefetchService keeps the price cache warm in the background via two goroutines:
//   - runCatchup: fires once at startup, bulk-fetches any trading days missing since the last
//     cached end_date (handles laptop sleep/resume gaps of 1–4 days).
//   - runNightly: wakes at 4am ET each trading day and bulk-fetches the previous market close.
//
// Both goroutines write to the same fact_price / fact_price_range tables as the on-demand
// singleton path, so no schema changes are required.
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

// WarmingDone returns a channel that is closed when the startup catch-up completes.
// Middleware may select on this channel to block requests until the cache is warm.
func (s *PrefetchService) WarmingDone() <-chan struct{} { return s.warmingDone }

// StartCatchup launches the startup catch-up goroutine. Call once from main after service init.
func (s *PrefetchService) StartCatchup(ctx context.Context) {
	go s.runCatchup(ctx)
}

// StartNightly launches the 4am ET recurring goroutine. Call once from main after service init.
func (s *PrefetchService) StartNightly(ctx context.Context) {
	go s.runNightly(ctx)
}

// runCatchup checks whether fact_price_range is up to date. If any trading days are missing
// between the last cached end_date and LastMarketClose(now), it bulk-fetches them in order.
// GetAllUS is called once before the day loop and reused for every missing day.
//
// Guards: catch-up is skipped (with a WARNING) if the DB has fewer than 1000 securities or
// more than 30 trading days need to be fetched. In either case the user is directed to use
// POST /admin/bulk-fetch-eodhd-prices for the initial backfill.
func (s *PrefetchService) runCatchup(ctx context.Context) {
	defer close(s.warmingDone) // always close — unblocks WarmingMiddleware even on error

	// Warm the security snapshot first so it is ready before any request is served,
	// and regardless of whether price catch-up proceeds below.
	if _, _, err := s.secRepo.GetAllSecurities(ctx); err != nil {
		log.Warnf("PrefetchService: failed to warm security snapshot: %v", err)
	} else {
		log.Info("PrefetchService: security snapshot warmed")
	}

	lastCached, err := s.pricingSvc.priceRepo.GetMaxPriceEndDate(ctx)
	if err != nil {
		log.Warnf("PrefetchService: could not read max price end date: %v", err)
		return
	}

	target := LastMarketClose(time.Now())
	targetDate := time.Date(target.Year(), target.Month(), target.Day(), 0, 0, 0, 0, time.UTC)
	if !lastCached.Before(targetDate) {
		log.Debugf("PrefetchService: cache is current (end_date=%s), no catch-up needed", lastCached.Format("2006-01-02"))
		return
	}

	// Load all US securities once; the same map is reused for every missing day.
	allSecs, err := s.secRepo.GetAllUS(ctx)
	if err != nil {
		log.Warnf("PrefetchService: could not load securities for catch-up: %v", err)
		return
	}

	// Guard: skip auto catch-up on a sparse or cold database to avoid hammering EODHD
	// with hundreds of requests before the initial import is complete.
	daysToPrefetch := countTradingDays(lastCached, targetDate)
	skip := false
	if len(allSecs) < 1000 {
		log.Warnf("PrefetchService: skipping catch-up — only %d securities in DB (need ≥1000). "+
			"Import securities first, then use POST /admin/bulk-fetch-eodhd-prices to backfill.", len(allSecs))
		skip = true
	}
	if daysToPrefetch > 30 {
		log.Warnf("PrefetchService: skipping catch-up — %d trading days to prefetch exceeds the 30-day limit. "+
			"Use POST /admin/bulk-fetch-eodhd-prices to backfill historical data.", daysToPrefetch)
		skip = true
	}
	if skip {
		return
	}

	log.Infof("PrefetchService: cache stale (end_date=%s, target=%s), starting catch-up",
		lastCached.Format("2006-01-02"), targetDate.Format("2006-01-02"))

	secsByTicker := buildSecsByTicker(allSecs)
	for d := nextTradingDay(lastCached); !d.After(targetDate); d = nextTradingDay(d) {
		if ctx.Err() != nil {
			return
		}
		if _, err := s.pricingSvc.BulkFetchPrices(ctx, "US", d, secsByTicker); err != nil {
			log.Warnf("PrefetchService: catch-up bulk fetch failed for %s: %v", d.Format("2006-01-02"), err)
		}
	}
	log.Infof("PrefetchService: catch-up complete")
}

// runNightly sleeps until 4am ET, then bulk-fetches the last market close for the US exchange.
// GetAllUS is called once per nightly wake-up (not per day) so newly added securities are
// included without requiring a server restart.
func (s *PrefetchService) runNightly(ctx context.Context) {
	for {
		select {
		case <-time.After(time.Until(next4amET())):
		case <-ctx.Done():
			return
		}

		now := time.Now()
		nyLoc, _ := time.LoadLocation("America/New_York")
		nowNY := now.In(nyLoc)
		if nowNY.Weekday() == time.Saturday || nowNY.Weekday() == time.Sunday || IsUSMarketHoliday(nowNY) {
			log.Debugf("PrefetchService: nightly skipping non-trading day %s", nowNY.Format("2006-01-02"))
			continue
		}

		// Refresh securities list once per wake-up.
		allSecs, err := s.secRepo.GetAllUS(ctx)
		if err != nil {
			log.Warnf("PrefetchService: nightly fetch could not load securities: %v", err)
			continue
		}
		secsByTicker := buildSecsByTicker(allSecs)

		lmc := LastMarketClose(now)
		d := time.Date(lmc.Year(), lmc.Month(), lmc.Day(), 0, 0, 0, 0, time.UTC)
		log.Infof("PrefetchService: nightly bulk fetch for %s", d.Format("2006-01-02"))
		if _, err := s.pricingSvc.BulkFetchPrices(ctx, "US", d, secsByTicker); err != nil {
			log.Warnf("PrefetchService: nightly bulk fetch failed: %v", err)
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

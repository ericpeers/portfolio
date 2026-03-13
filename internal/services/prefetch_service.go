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
func (s *PrefetchService) runCatchup(ctx context.Context) {
	defer close(s.warmingDone) // always close — unblocks WarmingMiddleware even on error

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

	log.Infof("PrefetchService: cache stale (end_date=%s, target=%s), starting catch-up",
		lastCached.Format("2006-01-02"), targetDate.Format("2006-01-02"))

	// Load all US securities once; the same map is reused for every missing day.
	allSecs, err := s.secRepo.GetAllUS(ctx)
	if err != nil {
		log.Warnf("PrefetchService: could not load securities for catch-up: %v", err)
		return
	}
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

// next4amET returns the next occurrence of 4:00 AM America/New_York. If it is currently
// before 4am, returns today at 4am; otherwise returns tomorrow at 4am.
func next4amET() time.Time {
	nyLoc, _ := time.LoadLocation("America/New_York")
	now := time.Now().In(nyLoc)
	t := time.Date(now.Year(), now.Month(), now.Day(), 4, 0, 0, 0, nyLoc)
	if !now.Before(t) {
		t = t.Add(24 * time.Hour)
	}
	return t
}

// nextTradingDay advances t by one calendar day, then keeps advancing until it lands on
// a weekday that is not a NYSE holiday. Returns midnight UTC on that day.
func nextTradingDay(t time.Time) time.Time {
	nyLoc, _ := time.LoadLocation("America/New_York")
	d := t.Add(24 * time.Hour)
	for {
		ny := d.In(nyLoc)
		if ny.Weekday() != time.Saturday && ny.Weekday() != time.Sunday && !IsUSMarketHoliday(ny) {
			return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
		}
		d = d.Add(24 * time.Hour)
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

package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

// PricingClients groups the external provider clients used by PricingService.
// Event and Bulk are nil-safe: the service skips those features when nil.
type PricingClients struct {
	Price    providers.StockPriceFetcher
	Event    providers.StockEventFetcher // nil-safe: splits/dividends skipped when nil
	Treasury providers.TreasuryRateFetcher
	Bulk     providers.BulkFetcher // nil-safe: bulk EOD fetching skipped when nil
}

// PricingService handles price fetching with PostgreSQL cache.
// priceClient is used for stock prices (e.g. AlphaVantage, EODHD).
// eventClient is used for corporate action events (splits + dividends).
// fredClient is used for US10Y treasury rates (FRED).
type PricingService struct {
	priceRepo   *repository.PriceRepository
	secRepo     *repository.SecurityRepository
	priceClient providers.StockPriceFetcher
	eventClient providers.StockEventFetcher
	fredClient  providers.TreasuryRateFetcher
	bulkClient  providers.BulkFetcher
	// fetchSem is a global semaphore that caps concurrent provider (EODHD/FRED) connections
	// across ALL callers. Default capacity is 1 (sequential). Use WithConcurrency to raise it.
	fetchSem chan struct{}
	// fetchGroup deduplicates concurrent in-flight fetches for the same security ID.
	// When multiple goroutines simultaneously see a cache miss for the same security,
	// only one fires a provider request; the others wait and share the result.
	fetchGroup singleflight.Group
}

// NewPricingService creates a new PricingService
func NewPricingService(
	priceRepo *repository.PriceRepository,
	secRepo *repository.SecurityRepository,
	clients PricingClients,
) *PricingService {
	return &PricingService{
		priceRepo:   priceRepo,
		secRepo:     secRepo,
		priceClient: clients.Price,
		eventClient: clients.Event,
		fredClient:  clients.Treasury,
		bulkClient:  clients.Bulk,
		fetchSem:    make(chan struct{}, 1), // default: sequential (safe for tests)
	}
}

// WithConcurrency configures the maximum number of concurrent provider fetches
// (EODHD/FRED connections). Values of 10–20 work well for production.
// This is the global cap across all callers; the EODHD token-bucket rate limiter
// provides the per-second throughput bound on top of this.
func (s *PricingService) WithConcurrency(n int) *PricingService {
	s.fetchSem = make(chan struct{}, n)
	return s
}

// GetDailyPrices fetches daily prices using PostgreSQL cache and EODHD (or the configured priceClient) or FRED for Treasury.
// It respects IPO/inception dates and uses intelligent caching via fact_price_range.
func (s *PricingService) GetDailyPrices(ctx context.Context, securityID int64, startDate, endDate time.Time) ([]models.PriceData, []models.EventData, error) {
	// Get security with exchange metadata for routing (FD client needs Country and ExchangeName)
	security, err := s.secRepo.GetByIDWithCountry(ctx, securityID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get security: %w", err)
	}

	// Use inception date from security to determine effective start date
	inception := security.Inception

	// Calculate effective start date (can't have prices before IPO)
	effectiveStart := startDate
	if inception != nil && startDate.Before(*inception) {
		log.Warnf("Start date %s selected before IPO/inception date for Ticker: %s, ID: %d", startDate, security.Ticker, security.ID)
		effectiveStart = *inception
	}

	// Check fact_price_range to determine caching status
	priceRange, err := s.priceRepo.GetPriceRange(ctx, securityID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get price range: %w", err)
	}

	currentDT := time.Now() //grab time once to use in a couple spots below

	needsFetch, adjStartDT, adjEndDT := DetermineFetch(priceRange, currentDT, effectiveStart, endDate)

	if needsFetch { //FIXME: fetchAndStore can return the records it just wrote. We don't need to re-fetch from Postgres
		// singleflight deduplicates concurrent fetches for the same security: when multiple
		// goroutines simultaneously see a cache miss (TOCTOU race), only one calls the provider;
		// the others wait and share the result, then all read from the now-populated Postgres cache.
		_, sfErr, _ := s.fetchGroup.Do(fmt.Sprintf("%d", securityID), func() (interface{}, error) {
			return nil, fetchAndStore(ctx, security, s, currentDT, adjStartDT, adjEndDT)
		})
		if sfErr != nil {
			log.Debugf("GetDailyPrices failed for %s (id=%d): %v", security.Ticker, security.ID, sfErr)
			return nil, nil, sfErr
		}
	}

	// Query fact_price for the requested range (using effective start for pre-IPO requests)
	// switch back to the original start/end dates. We only adjusted to ensure contiguous date ranges.
	prices, err := s.priceRepo.GetDailyPrices(ctx, securityID, effectiveStart, endDate)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get prices from DB: %w", err)
	}

	// Query fact_event for split events in the same range
	events, err := s.priceRepo.GetDailySplits(ctx, securityID, effectiveStart, endDate)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get splits from DB: %w", err)
	}

	return prices, events, nil
}

// fetchAndStore fetches prices from the appropriate provider and caches them.
// US10Y is fetched from FRED (incremental date range); all other securities from the configured priceClient (e.g. EODHD).
// EODHD prices are pre-adjusted; split/dividend events are fetched separately via eventClient when non-nil.
func fetchAndStore(ctx context.Context, security *models.SecurityWithCountry, s *PricingService, currentDT time.Time, startDT time.Time, endDT time.Time) error {
	// Acquire global concurrency slot to cap simultaneous provider connections.
	select {
	case s.fetchSem <- struct{}{}:
		defer func() { <-s.fetchSem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	var fetchedPrices []providers.ParsedPriceData
	var err error

	if isMoneyMarketFund(security) {
		log.Infof("Skipping EODHD for money market fund %s; using synthetic $1.00 prices", security.Ticker)
		fetchedPrices = generateMoneyMarketPrices(startDT, endDT)
	} else if security.Ticker == "US10Y" {
		// Fetch only the missing date range from FRED (incremental caching).
		// DGS10 historical data starts 1962-01-02.
		fetchedPrices, err = s.fredClient.GetTreasuryRate(ctx, startDT, endDT)
		if err != nil {
			return fmt.Errorf("failed to fetch Treasuries from FRED: %w", err)
		}
	} else {
		fetchedPrices, err = s.priceClient.GetDailyPrices(ctx, security, startDT, endDT)
		if err != nil {
			return fmt.Errorf("failed to fetch Daily prices from provider: %w", err)
		}
		if s.eventClient != nil {
			fetchedEvents, evErr := s.eventClient.GetStockEvents(ctx, security)
			if evErr != nil {
				// Context cancellation means the request was never sent — split data is unknown.
				// Propagate so the caller knows this fetch is incomplete.
				if errors.Is(evErr, context.Canceled) || errors.Is(evErr, context.DeadlineExceeded) {
					return evErr
				}
				// Any other provider error (API failure, malformed response, etc.) is non-fatal:
				// price data was fetched successfully and most securities have no splits.
				log.Warnf("Event fetch (Splits/Dividends) failed for %s (non-fatal): %v", security.Ticker, evErr)
			} else if len(fetchedEvents) > 0 {
				var eventsToStore []models.EventData
				for _, e := range fetchedEvents {
					eventsToStore = append(eventsToStore, models.EventData{
						SecurityID:       security.ID,
						Date:             e.Date,
						Dividend:         e.Dividend,
						SplitCoefficient: e.SplitCoefficient,
					})
				}
				if storeErr := s.priceRepo.StoreDailyEvents(ctx, eventsToStore); storeErr != nil { //StoreDaily #1
					log.Warnf("Event store (Split/Dividends) failed for %s (non-fatal): %v", security.Ticker, storeErr)
				}
			}
		}
	}

	// Convert all prices
	var allPrices []models.PriceData

	var maxDate time.Time
	for _, p := range fetchedPrices {
		priceData := models.PriceData{
			SecurityID: security.ID,
			Date:       p.Date,
			Open:       p.Open,
			High:       p.High,
			Low:        p.Low,
			Close:      p.Close,
			Volume:     p.Volume,
		}
		allPrices = append(allPrices, priceData)

		// Track the actual data end
		if maxDate.IsZero() || p.Date.After(maxDate) {
			maxDate = p.Date
		}
	}

	// Cache all prices in PostgreSQL
	if len(allPrices) > 0 {
		if err := s.priceRepo.StoreDailyPrices(ctx, allPrices); err != nil {
			log.Warnf("failed to store prices for %s (id=%d): %v", security.Ticker, security.ID, err)
		}
	}

	// Always record the price range, even when the provider returned no data for the requested
	// window (holiday, weekend, or lightly-traded start date).
	// Use startDT (not minDate) so that re-requests for non-data start dates don't trigger
	// another provider fetch. rangeEnd uses maxDate when data was returned; endDT otherwise.
	//
	// it is possible that endDT > currentDT. Normally this is not the case. We need to fetch
	// the earliest next-business-day, but not before that. if currentDT > endDT, use "endDT+1."
	earliest := endDT
	if endDT.After(currentDT) {
		earliest = currentDT
	}
	var nextUpdate time.Time
	if security.Ticker == "US10Y" {
		nextUpdate = NextTreasuryUpdateDate(earliest)
	} else {
		nextUpdate = NextMarketDate(earliest)
	}

	rangeEnd := endDT
	if len(allPrices) > 0 {
		rangeEnd = maxDate
	}

	// Update the price range (uses LEAST/GREATEST to expand)
	if err := s.priceRepo.UpsertPriceRange(ctx, security.ID, startDT, rangeEnd, nextUpdate); err != nil {
		log.Warnf("failed to update price range for %s (id=%d): %v", security.Ticker, security.ID, err)
	}

	return nil
}

// isMoneyMarketFund returns true if the security is a FUND type whose name
// contains "money market" or "cash rsrvs" (case-insensitive). These funds
// maintain a stable $1.00 NAV and don't have reliable EODHD price data.
func isMoneyMarketFund(security *models.SecurityWithCountry) bool {
	return security.Type == string(models.SecurityTypeFund) &&
		(strings.Contains(strings.ToLower(security.Name), "money market") ||
			strings.Contains(strings.ToLower(security.Name), "cash rsrvs"))
}

// generateMoneyMarketPrices returns synthetic $1.00 price entries for every
// trading day (weekdays excluding NYSE holidays) between startDT and endDT inclusive.
func generateMoneyMarketPrices(startDT time.Time, endDT time.Time) []providers.ParsedPriceData {
	var prices []providers.ParsedPriceData
	for d := startDT; !d.After(endDT); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		if IsUSMarketHoliday(d) {
			continue
		}
		prices = append(prices, providers.ParsedPriceData{
			Date:             d,
			Open:             1.0,
			High:             1.0,
			Low:              1.0,
			Close:            1.0,
			SplitCoefficient: 1.0,
		})
	}
	return prices
}

func DetermineFetch(priceRange *repository.PriceRange, currentDT time.Time, startDT time.Time, endDT time.Time) (bool, time.Time, time.Time) {
	if priceRange == nil {
		//		log.Debugf("DetermineFetch: PR NIL, Current Time %s, Start: %s, End: %s", currentDT, effectiveStart, endDate)
		// No cached data at all - need fetch
		return true, startDT, endDT
	}

	//we cannot have gaps in coverage. So if we were to pick a time range that is before or after what we have fetched, we have to extend that range to meet
	//the edge of the previous range. E.g. PostGres has Jan 1 2025-Jan 1 2026. We then request Feb 1-Mar 1 2026. Cannot leave a gap of Jan 2-31, so we extend the
	//new request start date to Jan 2, 2026.
	var adjStartDT, adjEndDT time.Time
	adjStartDT = startDT
	adjEndDT = endDT
	if endDT.Before(priceRange.StartDate) {
		adjEndDT = priceRange.StartDate.AddDate(0, 0, -1)
	}
	if startDT.After(priceRange.EndDate) {
		adjStartDT = priceRange.EndDate.AddDate(0, 0, 1)
	}

	//log.Debugf("DetermineFetch. PR Start: %s, PR End %s, PR Next %s, Current: %s, Start: %s, End: %s", priceRange.StartDate, priceRange.EndDate, priceRange.NextUpdate, currentDT, effectiveStart, endDate)

	// Historical data we've never fetched — must fetch regardless of NextUpdate.
	if startDT.Before(priceRange.StartDate) {
		return true, adjStartDT, adjEndDT
	}

	// Normalize to calendar day (midnight UTC) before comparing.
	// endDate may carry a 23:59:59 time while priceRange.EndDate is stored at midnight;
	// same calendar day should count as covered.
	normalizedEnd := time.Date(endDT.Year(), endDT.Month(), endDT.Day(), 0, 0, 0, 0, time.UTC)
	normalizedCacheEnd := time.Date(priceRange.EndDate.Year(), priceRange.EndDate.Month(), priceRange.EndDate.Day(), 0, 0, 0, 0, time.UTC)

	// Case A: requested end extends beyond cached end — re-fetch after nextUpdate (existing logic).
	if normalizedEnd.After(normalizedCacheEnd) && currentDT.After(priceRange.NextUpdate) {
		return true, adjStartDT, adjEndDT
	}

	// Case B: same calendar day as cached end. The cache may have been populated before
	// market close (pre-close snapshot). Re-fetch if nextUpdate has elapsed AND we are still
	// within the overnight data-settling window (before 4 AM ET on the next calendar day).
	// Once past 4 AM ET the data is considered settled; serve from cache until the end date
	// extends further.
	if normalizedEnd.Equal(normalizedCacheEnd) && currentDT.After(priceRange.NextUpdate) {
		nyLoc, _ := time.LoadLocation("America/New_York")
		// normalizedCacheEnd is midnight UTC on the correct calendar date (Year/Month/Day fields
		// are in UTC coordinates). Add 1 day to get the next calendar date, then build 4 AM ET
		// from those UTC year/month/day values to avoid the midnight-UTC → prior-evening-ET shift.
		nextDay := normalizedCacheEnd.AddDate(0, 0, 1)
		settleBefore := time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(), 4, 0, 0, 0, nyLoc)
		if currentDT.Before(settleBefore) {
			return true, adjStartDT, adjEndDT
		}
	}

	return false, adjStartDT, adjEndDT

}

// GetPriceAtDate returns the closing price for a security at a specific date
// FIXME - this code may return a price before or after the date in question.
// it does call GetDailyPrices with 7 days of data, so that probably handles the EODHD fetch - to ensure we at least have data.
// I think that performance_service relies on this logic, but this is pretty dangerous.
func (s *PricingService) GetPriceAtDate(ctx context.Context, securityID int64, date time.Time) (float64, error) {
	// Try to get from cache first
	price, err := s.priceRepo.GetPriceAtDate(ctx, securityID, date)
	if err != nil {
		return 0, err
	}
	if price != nil {
		return price.Close, nil
	}

	// Fetch a range around the date
	startDate := date.AddDate(0, 0, -7)
	// Callers: NormalizeIdealPortfolio, GetPricesAtDateBatch (fallback for cache misses).
	// Split adjustments use GetSplitAdjustmentsBatch which queries fact_event directly.
	prices, _, err := s.GetDailyPrices(ctx, securityID, startDate, date)
	if err != nil {
		return 0, err
	}

	// Find the closest price on or before the date
	var closestPrice float64
	var closestDate time.Time
	for _, p := range prices {
		if !p.Date.After(date) && p.Date.After(closestDate) {
			closestDate = p.Date
			closestPrice = p.Close
		}
	}

	if closestDate.IsZero() {
		return 0, fmt.Errorf("no price found for security %d at date %s", securityID, date.Format("2006-01-02"))
	}

	return closestPrice, nil
}

// GetPricesAtDateBatch returns closing prices for multiple securities at a date.
// Securities already cached in fact_price are returned from there in one query.
// For cache misses, falls back to per-item GetPriceAtDate (which fetches from
// the external provider and caches the result).
func (s *PricingService) GetPricesAtDateBatch(ctx context.Context, secIDs []int64, date time.Time) (map[int64]float64, error) {
	cached, err := s.priceRepo.GetPricesAtDateBatch(ctx, secIDs, date)
	if err != nil {
		return nil, err
	}
	for _, id := range secIDs {
		if _, found := cached[id]; !found {
			price, err := s.GetPriceAtDate(ctx, id, date)
			if err != nil {
				return nil, err
			}
			cached[id] = price
		}
	}
	return cached, nil
}

// GetSplitAdjustmentsBatch returns cumulative split coefficients for multiple
// securities between startDate and endDate. Securities without splits are absent
// from the returned map; callers should default absent entries to 1.0.
// Only queries the local cache (fact_event); does not trigger a provider fetch.
// This is safe because GetPricesAtDateBatch ensures prices (and associated events)
// are cached before splits are needed.
func (s *PricingService) GetSplitAdjustmentsBatch(ctx context.Context, secIDs []int64, startDate, endDate time.Time) (map[int64]float64, error) {
	return s.priceRepo.GetSplitCoefficientsBatch(ctx, secIDs, startDate, endDate)
}

// BulkFetchPrices fetches end-of-day prices (and events, if bulkEventClient is set)
// for all securities on an exchange, storing records for any security in secsByTicker.
// secsByTicker should be pre-loaded by the caller (e.g. from SecurityRepository.GetAllUS)
// to avoid a per-record database lookup across thousands of tickers.
// When bulkEventClient is non-nil, EOD and events are fetched concurrently.
// Event store failures are non-fatal and logged as warnings.
// minRequired is the minimum number of matched prices required before writing to the DB;
// pass models.MinBulkFetchPrices for scheduled fetches or 0 to bypass the check.
func (s *PricingService) BulkFetchPrices(ctx context.Context, exchange string, date time.Time, secsByTicker map[string]*models.Security, minRequired int) (*models.BulkFetchResult, error) {
	result := &models.BulkFetchResult{
		Exchange: exchange,
		Date:     date.Format("2006-01-02"),
	}

	var eodRecords []providers.BulkEODRecord
	var eventRecords []providers.BulkEventRecord
	var eodErr, eventErr error

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		eodRecords, eodErr = s.bulkClient.GetBulkEOD(ctx, exchange, date)
	}()
	go func() {
		defer wg.Done()
		eventRecords, eventErr = s.bulkClient.GetBulkEvents(ctx, exchange, date)
	}()
	wg.Wait()

	if eodErr != nil {
		return nil, fmt.Errorf("bulk fetch failed: %w", eodErr)
	}
	if eventErr != nil {
		log.Warnf("BulkFetchPrices: events fetch failed (non-fatal): %v", eventErr)
	}

	result.Fetched = len(eodRecords)

	// Index events by ticker for O(1) lookup during price loop.
	eventsByCode := make(map[string]providers.BulkEventRecord, len(eventRecords))
	for _, e := range eventRecords {
		eventsByCode[e.Code] = e
	}

	var prices []models.PriceData
	var events []models.EventData
	var skippedTickers []string
	for _, rec := range eodRecords {
		sec, ok := secsByTicker[rec.Code]
		if !ok {
			result.Skipped++
			if len(skippedTickers) < 20 {
				skippedTickers = append(skippedTickers, rec.Code)
			}
			continue
		}
		prices = append(prices, models.PriceData{
			SecurityID: sec.ID,
			Date:       rec.Date,
			Open:       rec.Open,
			High:       rec.High,
			Low:        rec.Low,
			Close:      rec.AdjClose,
			Volume:     rec.Volume,
		})
		if ev, ok := eventsByCode[rec.Code]; ok {
			events = append(events, models.EventData{
				SecurityID:       sec.ID,
				Date:             rec.Date,
				Dividend:         ev.Dividend,
				SplitCoefficient: ev.SplitCoefficient,
			})
		}
	}

	// Skipped tickers are not in secsByTicker (our DB). EODHD bulk data includes instruments
	// we don't track: mutual funds (X/Y suffix share classes), SPAC units/warrants (-UN/-WT/_old),
	// index symbols (^W5000), and Morningstar fund codes (0P...). Skip counts grow going further
	// back in time because more mutual funds existed historically (many have since been merged/liquidated).
	// This is expected and not data loss.
	log.Debugf("BulkFetchPrices %s %s: %d records, %d matched, %d skipped (not in secsByTicker); sample skipped: %v",
		exchange, date.Format("2006-01-02"), len(eodRecords), len(prices), result.Skipped, skippedTickers)

	// Reject incomplete responses before touching the DB. When minRequired > 0 and
	// matched prices fall below it, EODHD hasn't finished publishing (e.g. fetched
	// minutes after market close). Returning an error keeps fact_price / fact_price_range
	// unchanged so the scheduler's break-on-error retry fires on the next poll cycle.
	if minRequired > 0 && len(prices) < minRequired {
		return nil, fmt.Errorf("BulkFetchPrices: incomplete response for %s — %d prices matched (need ≥%d); EODHD data not yet fully published",
			date.Format("2006-01-02"), len(prices), minRequired)
	}

	dbStart := time.Now()
	//date may be in the past for backfills or catch-up-fills. Rely on price_repo to use a max function to prevent
	//setting nextUpdate to a "year ago"
	nextUpdate := NextMarketDate(date)

	// Initialize ranges for ALL known securities, including those absent from the EOD response
	// (lightly-traded, halted, or newly-listed). Without this, a missing security has no
	// fact_price_range row and every subsequent singleton GetDailyPrices triggers a re-fetch.
	type rangeAccum struct{ minDate, maxDate time.Time }
	rangeMap := make(map[int64]*rangeAccum, len(secsByTicker))
	for _, sec := range secsByTicker {
		rangeMap[sec.ID] = &rangeAccum{minDate: date, maxDate: date}
	}
	// Expand per-security range from actual price records (handles OOO multi-date returns).
	for _, p := range prices {
		if acc, ok := rangeMap[p.SecurityID]; ok {
			if p.Date.Before(acc.minDate) {
				acc.minDate = p.Date
			}
			if p.Date.After(acc.maxDate) {
				acc.maxDate = p.Date
			}
		}
	}

	if len(prices) > 0 {
		t := time.Now()
		if err := s.priceRepo.StoreDailyPrices(ctx, prices); err != nil {
			return nil, fmt.Errorf("failed to store bulk prices: %w", err)
		}
		log.Debugf("BulkFetchPrices: StoreDailyPrices %d rows: %.2fms", len(prices), float64(time.Since(t))/float64(time.Millisecond))
	}

	ranges := make([]models.PriceRangeData, 0, len(rangeMap))
	for secID, acc := range rangeMap {
		ranges = append(ranges, models.PriceRangeData{
			SecurityID: secID,
			StartDate:  acc.minDate,
			EndDate:    acc.maxDate,
			NextUpdate: nextUpdate,
		})
	}
	t := time.Now()
	if err := s.priceRepo.BatchUpsertPriceRange(ctx, ranges); err != nil {
		log.Warnf("BulkFetchPrices: failed to update price ranges (non-fatal): %v", err)
	}
	log.Debugf("BulkFetchPrices: BatchUpsertPriceRange %d rows: %.2fms", len(ranges), float64(time.Since(t))/float64(time.Millisecond))
	result.Stored = len(prices)

	if len(events) > 0 {
		t := time.Now()
		if err := s.priceRepo.StoreDailyEvents(ctx, events); err != nil {
			log.Warnf("BulkFetchPrices: failed to store bulk events (non-fatal): %v", err)
		}
		log.Debugf("BulkFetchPrices: StoreDailyEvents %d rows: %.2fms", len(events), float64(time.Since(t))/float64(time.Millisecond))
	}

	log.Debugf("BulkFetchPrices: total DB storage: %.2fms", float64(time.Since(dbStart))/float64(time.Millisecond))
	log.Infof("BulkFetchPrices: exchange=%s date=%s fetched=%d stored=%d skipped=%d events=%d",
		exchange, result.Date, result.Fetched, result.Stored, result.Skipped, len(events))

	// Reaching here means len(prices) >= minPricesForFullFetch (checked above); record the fetch.
	if err := s.priceRepo.LogBulkFetch(ctx, date); err != nil {
		log.Warnf("BulkFetchPrices: failed to log bulk fetch (non-fatal): %v", err)
	}

	return result, nil
}

func (s *PricingService) GetAggregatePortfolioDividends(ctx context.Context, portfolioID int64, startDate, endDate time.Time) ([]models.EventData, error) {
	return s.priceRepo.GetAggregatePortfolioDividends(ctx, portfolioID, startDate, endDate)
}

// GetFirstPriceDates returns the earliest price date for each security in secIDs.
// Securities with no price rows are absent from the returned map.
func (s *PricingService) GetFirstPriceDates(ctx context.Context, secIDs []int64) (map[int64]*time.Time, error) {
	return s.priceRepo.GetFirstPriceDates(ctx, secIDs)
}

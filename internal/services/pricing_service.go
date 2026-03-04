package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// PricingService handles price fetching with PostgreSQL cache.
// fdClient is used for stock prices (FinancialData.net).
// fdEventClient is used for corporate action events (splits + dividends) from FinancialData.net.
// fredClient is used for US10Y treasury rates (FRED).
type PricingService struct {
	priceRepo     *repository.PriceRepository
	secRepo       *repository.SecurityRepository
	fdClient      providers.StockPriceFetcher
	fdEventClient providers.StockEventFetcher
	fredClient    providers.TreasuryRateFetcher
}

// NewPricingService creates a new PricingService
func NewPricingService(
	priceRepo *repository.PriceRepository,
	secRepo *repository.SecurityRepository,
	fdClient providers.StockPriceFetcher,
	fdEventClient providers.StockEventFetcher,
	fredClient providers.TreasuryRateFetcher,
) *PricingService {
	return &PricingService{
		priceRepo:     priceRepo,
		secRepo:       secRepo,
		fdClient:      fdClient,
		fdEventClient: fdEventClient,
		fredClient:    fredClient,
	}
}

// GetDailyPrices fetches daily prices using PostgreSQL cache and FinancialData.net / AlphaVantage / EODHD or Fred for Treasury.
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
		log.Warnf("Start date %s selected before IPO/inception date for Ticker: %s, ID: %d", startDate, security.Symbol, security.ID)
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
		err := fetchAndStore(ctx, security, s, currentDT, adjStartDT, adjEndDT)
		if err != nil {
			log.Warnf("About to fail pricing_service:GetDailyPrices")
			return nil, nil, err
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
// US10Y is fetched from FRED (incremental date range); all other securities from a provider like (Alphavantage, FinancialData.net, EODHD)
// FD prices are pre-adjusted so hasSplits=false for FD; AV returns split/dividend events.
func fetchAndStore(ctx context.Context, security *models.SecurityWithCountry, s *PricingService, currentDT time.Time, startDT time.Time, endDT time.Time) error {
	var fetchedPrices []providers.ParsedPriceData
	var err error
	hasSplits := false

	if isMoneyMarketFund(security) {
		log.Infof("Skipping EODHD for money market fund %s; using synthetic $1.00 prices", security.Symbol)
		fetchedPrices = generateMoneyMarketPrices(startDT, endDT)
	} else if security.Symbol == "US10Y" {
		// Fetch only the missing date range from FRED (incremental caching).
		// DGS10 historical data starts 1962-01-02.
		fetchedPrices, err = s.fredClient.GetTreasuryRate(ctx, startDT, endDT)
		if err != nil {
			return fmt.Errorf("failed to fetch Treasuries from FRED: %w", err)
		}
	} else {
		fetchedPrices, err = s.fdClient.GetDailyPrices(ctx, security, startDT, endDT)
		if err != nil {
			return fmt.Errorf("failed to fetch Daily prices from provider: %w", err)
		}
		if s.fdEventClient != nil {
			fetchedEvents, evErr := s.fdEventClient.GetStockEvents(ctx, security)
			if evErr != nil {
				log.Warnf("Event fetch (Splits/Dividends) failed for %s (non-fatal): %v", security.Symbol, evErr)
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
					log.Warnf("Event store (Split/Dividends) failed for %s (non-fatal): %v", security.Symbol, storeErr)
				}
			}
		}
	}

	// Convert all prices
	var allPrices []models.PriceData
	var allEvents []models.EventData

	var minDate, maxDate time.Time
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

		if hasSplits {
			if (p.SplitCoefficient != 1.0 && p.SplitCoefficient != 0.0) || (p.Dividend != 0) {
				eventData := models.EventData{
					SecurityID:       security.ID,
					Date:             p.Date,
					Dividend:         p.Dividend,
					SplitCoefficient: p.SplitCoefficient,
				}
				allEvents = append(allEvents, eventData)
			}
		}

		// Track the actual data range
		if minDate.IsZero() || p.Date.Before(minDate) {
			minDate = p.Date
		}
		if maxDate.IsZero() || p.Date.After(maxDate) {
			maxDate = p.Date
		}
	}

	// Cache all prices in PostgreSQL
	if len(allPrices) > 0 {
		if err := s.priceRepo.StoreDailyPrices(ctx, allPrices); err != nil {
			log.Errorf("warning: failed to store prices: %v\n", err)
		}

		/* TODO: Think some more about this. Inception is not the same as earliest available data, and we may want to try to fetch more data.
		// Infer inception date from the earliest price when doing a full fetch
		// and the security has no inception date recorded.
		if fetchStyle == "full" && security.Symbol != "US10Y" && security.Inception == nil && !minDate.IsZero() {
			if err := s.secRepo.UpdateInceptionDate(ctx, securityID, &minDate); err != nil {
				log.Warnf("failed to infer inception date for %s: %v", security.Symbol, err)
			} else {
				log.Infof("inferred inception date %s for %s from earliest price", minDate.Format("2006-01-02"), security.Symbol)
			}
		}
		*/

		// it is possible that endDT > currentDT. Normally this is not the case. We need to fetch the earliest next-business-day, but not before that.
		// if currentDT > endDT, then we use "endDT+1.""
		earliest := endDT
		if endDT.After(currentDT) {
			earliest = currentDT
		}
		var nextUpdate time.Time
		if security.Symbol == "US10Y" {
			nextUpdate = NextTreasuryUpdateDate(earliest)
		} else {
			nextUpdate = NextMarketDate(earliest)
		}

		// Update the price range (uses LEAST/GREATEST to expand)
		if err := s.priceRepo.UpsertPriceRange(ctx, security.ID, minDate, maxDate, nextUpdate); err != nil {
			fmt.Printf("warning: failed to update price range: %v\n", err)
		}
	}

	if len(allEvents) != 0 { //StoreDaily #2
		if err := s.priceRepo.StoreDailyEvents(ctx, allEvents); err != nil {
			log.Errorf("warning: failed to store events: %v\n", err)
		}
	}

	return nil
}

// isMoneyMarketFund returns true if the security is a FUND type whose name
// contains "money market" (case-insensitive). These funds maintain a stable
// $1.00 NAV and don't have reliable EODHD price data.
func isMoneyMarketFund(security *models.SecurityWithCountry) bool {
	return security.Type == string(models.SecurityTypeFund) &&
		(strings.Contains(strings.ToLower(security.Name), "money market") ||
			strings.Contains(strings.ToLower(security.Name), "cash rsrvs"))
}

// generateMoneyMarketPrices returns synthetic $1.00 price entries for every
// trading day (weekdays excluding NYSE holidays) from start through currentDT.
// If priceRange is non-nil, starts the day after the last cached end date (incremental update).
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

// IsUSMarketHoliday reports whether d is an NYSE market holiday.
// NYSE observes: New Year's Day, MLK Day, Presidents' Day, Good Friday,
// Memorial Day, Juneteenth (since 2022), Independence Day, Labor Day,
// Thanksgiving, and Christmas — with fixed holidays shifted to the nearest
// weekday when they fall on a weekend (Saturday → Friday, Sunday → Monday).
func IsUSMarketHoliday(d time.Time) bool {
	year := d.Year()
	target := time.Date(year, d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)

	// observedDate returns the weekday a fixed holiday is observed on.
	observedDate := func(y int, month time.Month, day int) time.Time {
		h := time.Date(y, month, day, 0, 0, 0, 0, time.UTC)
		switch h.Weekday() {
		case time.Saturday:
			return h.AddDate(0, 0, -1) // observe Friday
		case time.Sunday:
			//FIXME - this is wrong for GoodFriday. That is "before"
			return h.AddDate(0, 0, 1) // observe Monday
		}
		return h
	}

	// nthWeekday returns the nth occurrence of weekday in the given month/year.
	nthWeekday := func(y int, month time.Month, weekday time.Weekday, n int) time.Time {
		t := time.Date(y, month, 1, 0, 0, 0, 0, time.UTC)
		for t.Weekday() != weekday {
			t = t.AddDate(0, 0, 1)
		}
		return t.AddDate(0, 0, 7*(n-1))
	}

	// lastWeekday returns the last occurrence of weekday in the given month/year.
	lastWeekday := func(y int, month time.Month, weekday time.Weekday) time.Time {
		t := time.Date(y, month+1, 0, 0, 0, 0, 0, time.UTC) // last day of month
		for t.Weekday() != weekday {
			t = t.AddDate(0, 0, -1)
		}
		return t
	}

	// FIXME. What on God's green earth is this shit? Just check if the holiday is a Sunday in late March/April
	// goodFriday returns Good Friday for year y using the anonymous Gregorian algorithm.
	goodFriday := func(y int) time.Time {
		a := y % 19
		b := y / 100
		c := y % 100
		bDiv4 := b / 4
		e := b % 4
		f := (b + 8) / 25
		g := (b - f + 1) / 3
		h := (19*a + b - bDiv4 - g + 15) % 30
		i := c / 4
		k := c % 4
		l := (32 + 2*e + 2*i - h - k) % 7
		m := (a + 11*h + 22*l) / 451
		month := time.Month((h + l - 7*m + 114) / 31)
		day := ((h + l - 7*m + 114) % 31) + 1
		easter := time.Date(y, month, day, 0, 0, 0, 0, time.UTC)
		return easter.AddDate(0, 0, -2)
	}

	holidays := []time.Time{
		observedDate(year, time.January, 1),             // New Year's Day
		observedDate(year+1, time.January, 1),           // New Year's Day next year (may fall Dec 31)
		nthWeekday(year, time.January, time.Monday, 3),  // MLK Day (3rd Monday of Jan)
		nthWeekday(year, time.February, time.Monday, 3), // Presidents' Day (3rd Monday of Feb)
		goodFriday(year),                                  // Good Friday
		lastWeekday(year, time.May, time.Monday),          // Memorial Day (last Monday of May)
		observedDate(year, time.July, 4),                  // Independence Day
		nthWeekday(year, time.September, time.Monday, 1),  // Labor Day (1st Monday of Sep)
		nthWeekday(year, time.November, time.Thursday, 4), // Thanksgiving (4th Thursday of Nov)
		observedDate(year, time.December, 25),             // Christmas
	}
	if year >= 2022 {
		holidays = append(holidays, observedDate(year, time.June, 19)) // Juneteenth (since 2022)
	}

	for _, h := range holidays {
		if target.Equal(h) {
			return true
		}
	}
	return false
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
	// Fetch only when the requested end strictly exceeds the cached end AND the stale
	// window has passed. If normalizedEnd == normalizedCacheEnd the cache already covers
	// the request — don't speculatively re-fetch just because NextUpdate has elapsed.
	if normalizedEnd.After(normalizedCacheEnd) && currentDT.After(priceRange.NextUpdate) {
		return true, adjStartDT, adjEndDT
	}

	return false, adjStartDT, adjEndDT

}

// NextMarketDate predicts the date of the next stock market update.
// It handles timezone conversion, business day logic.
// It returns the next target date, in New York time, 4:30pm.
func NextMarketDate(input time.Time) time.Time {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("Failed to load location: %v", err)
		return input.AddDate(0, 0, 1)
	}

	nyTime := input.In(nyLoc)
	cutoffHour, cutoffMinute := 16, 30

	// Create target at 4:30 PM today
	target := time.Date(nyTime.Year(), nyTime.Month(), nyTime.Day(),
		cutoffHour, cutoffMinute, 0, 0, nyLoc)

	isWeekday := nyTime.Weekday() >= time.Monday && nyTime.Weekday() <= time.Friday
	isBeforeCutoff := nyTime.Before(target)

	if !(isWeekday && isBeforeCutoff) {
		// Roll forward to next day
		target = target.AddDate(0, 0, 1)
		// Skip weekends
		for target.Weekday() == time.Saturday || target.Weekday() == time.Sunday {
			target = target.AddDate(0, 0, 1)
		}
	}

	return target
}

// NextTreasuryUpdateDate predicts the next time FRED DGS10 data will be updated.
// FRED publishes Friday treasury data on the following Monday at 4:30 PM ET,
// so Fridays are always treated as "after cutoff" regardless of the time of day.
func NextTreasuryUpdateDate(input time.Time) time.Time {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("Failed to load location: %v", err)
		return input.AddDate(0, 0, 1)
	}

	nyTime := input.In(nyLoc)
	target := time.Date(nyTime.Year(), nyTime.Month(), nyTime.Day(), 16, 30, 0, 0, nyLoc)

	// Monday–Thursday before 4:30 PM ET → return today at 4:30 PM
	// Friday (any time), weekends, or after 4:30 PM → roll to next business day
	isWeekdayNotFriday := nyTime.Weekday() >= time.Monday && nyTime.Weekday() <= time.Thursday
	isBeforeCutoff := nyTime.Before(target)

	if !(isWeekdayNotFriday && isBeforeCutoff) {
		target = target.AddDate(0, 0, 1)
		for target.Weekday() == time.Saturday || target.Weekday() == time.Sunday {
			target = target.AddDate(0, 0, 1)
		}
	}

	return target
}

// GetPriceAtDate returns the closing price for a security at a specific date
// FIXME - this code may return a price before or after the date in question.
// it does call GetDailyPrices with 7 days of data, so that probably handles the Alphavantage fetch - to ensure we at least have data.
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
	// Callers: NormalizeIdealPortfolio, ComputeMembership, ComputeDirectMembership.
	// Split adjustment is handled separately by callers via GetSplitAdjustment.
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

// GetSplitAdjustment returns the cumulative split coefficient for a security
// between startDate and endDate. For example, a 2-for-1 split returns 2.0.
// If no splits occurred, returns 1.0.
func (s *PricingService) GetSplitAdjustment(ctx context.Context, securityID int64, startDate, endDate time.Time) (float64, error) {
	_, events, err := s.GetDailyPrices(ctx, securityID, startDate, endDate)
	if err != nil {
		return 0, fmt.Errorf("failed to get split data for security %d: %w", securityID, err)
	}
	coefficient := 1.0
	for _, e := range events {
		coefficient *= e.SplitCoefficient
	}
	return coefficient, nil
}

package services

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// adHocClosures covers NYSE market closures that are not recurring statutory holidays.
// These are one-off closures for national emergencies, presidential mourning periods,
// and natural disasters. Add new entries here as they occur.
var adHocClosures = map[time.Time]bool{
	time.Date(2001, 9, 11, 0, 0, 0, 0, time.UTC): true, // 9/11 terrorist attacks
	time.Date(2001, 9, 12, 0, 0, 0, 0, time.UTC): true,
	time.Date(2001, 9, 13, 0, 0, 0, 0, time.UTC): true,
	time.Date(2001, 9, 14, 0, 0, 0, 0, time.UTC): true,
	time.Date(2004, 6, 11, 0, 0, 0, 0, time.UTC): true, // President Reagan mourning
	time.Date(2007, 1, 2, 0, 0, 0, 0, time.UTC):  true, // President Ford mourning
	time.Date(2012, 10, 29, 0, 0, 0, 0, time.UTC): true, // Hurricane Sandy
	time.Date(2012, 10, 30, 0, 0, 0, 0, time.UTC): true,
	time.Date(2018, 12, 5, 0, 0, 0, 0, time.UTC): true, // President Bush Sr. mourning
	time.Date(2025, 1, 9, 0, 0, 0, 0, time.UTC):  true, // President Carter mourning
}

// easterSundays maps year → Easter Sunday date (UTC midnight) for 1990–2060.
// Good Friday = Easter Sunday − 2 days (always a Friday).
// Dates sourced from the US Naval Observatory / ecclesiastical calendar.
// For years outside this table, buildHolidaySet falls back to the Meeus/Jones/Butcher algorithm.
var easterSundays = map[int]time.Time{
	1990: time.Date(1990, 4, 15, 0, 0, 0, 0, time.UTC),
	1991: time.Date(1991, 3, 31, 0, 0, 0, 0, time.UTC),
	1992: time.Date(1992, 4, 19, 0, 0, 0, 0, time.UTC),
	1993: time.Date(1993, 4, 11, 0, 0, 0, 0, time.UTC),
	1994: time.Date(1994, 4, 3, 0, 0, 0, 0, time.UTC),
	1995: time.Date(1995, 4, 16, 0, 0, 0, 0, time.UTC),
	1996: time.Date(1996, 4, 7, 0, 0, 0, 0, time.UTC),
	1997: time.Date(1997, 3, 30, 0, 0, 0, 0, time.UTC),
	1998: time.Date(1998, 4, 12, 0, 0, 0, 0, time.UTC),
	1999: time.Date(1999, 4, 4, 0, 0, 0, 0, time.UTC),
	2000: time.Date(2000, 4, 23, 0, 0, 0, 0, time.UTC),
	2001: time.Date(2001, 4, 15, 0, 0, 0, 0, time.UTC),
	2002: time.Date(2002, 3, 31, 0, 0, 0, 0, time.UTC),
	2003: time.Date(2003, 4, 20, 0, 0, 0, 0, time.UTC),
	2004: time.Date(2004, 4, 11, 0, 0, 0, 0, time.UTC),
	2005: time.Date(2005, 3, 27, 0, 0, 0, 0, time.UTC),
	2006: time.Date(2006, 4, 16, 0, 0, 0, 0, time.UTC),
	2007: time.Date(2007, 4, 8, 0, 0, 0, 0, time.UTC),
	2008: time.Date(2008, 3, 23, 0, 0, 0, 0, time.UTC),
	2009: time.Date(2009, 4, 12, 0, 0, 0, 0, time.UTC),
	2010: time.Date(2010, 4, 4, 0, 0, 0, 0, time.UTC),
	2011: time.Date(2011, 4, 24, 0, 0, 0, 0, time.UTC),
	2012: time.Date(2012, 4, 8, 0, 0, 0, 0, time.UTC),
	2013: time.Date(2013, 3, 31, 0, 0, 0, 0, time.UTC),
	2014: time.Date(2014, 4, 20, 0, 0, 0, 0, time.UTC),
	2015: time.Date(2015, 4, 5, 0, 0, 0, 0, time.UTC),
	2016: time.Date(2016, 3, 27, 0, 0, 0, 0, time.UTC),
	2017: time.Date(2017, 4, 16, 0, 0, 0, 0, time.UTC),
	2018: time.Date(2018, 4, 1, 0, 0, 0, 0, time.UTC),
	2019: time.Date(2019, 4, 21, 0, 0, 0, 0, time.UTC),
	2020: time.Date(2020, 4, 12, 0, 0, 0, 0, time.UTC),
	2021: time.Date(2021, 4, 4, 0, 0, 0, 0, time.UTC),
	2022: time.Date(2022, 4, 17, 0, 0, 0, 0, time.UTC),
	2023: time.Date(2023, 4, 9, 0, 0, 0, 0, time.UTC),
	2024: time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC),
	2025: time.Date(2025, 4, 20, 0, 0, 0, 0, time.UTC),
	2026: time.Date(2026, 4, 5, 0, 0, 0, 0, time.UTC),
	2027: time.Date(2027, 3, 28, 0, 0, 0, 0, time.UTC),
	2028: time.Date(2028, 4, 16, 0, 0, 0, 0, time.UTC),
	2029: time.Date(2029, 4, 1, 0, 0, 0, 0, time.UTC),
	2030: time.Date(2030, 4, 21, 0, 0, 0, 0, time.UTC),
	2031: time.Date(2031, 4, 13, 0, 0, 0, 0, time.UTC),
	2032: time.Date(2032, 3, 28, 0, 0, 0, 0, time.UTC),
	2033: time.Date(2033, 4, 17, 0, 0, 0, 0, time.UTC),
	2034: time.Date(2034, 4, 9, 0, 0, 0, 0, time.UTC),
	2035: time.Date(2035, 3, 25, 0, 0, 0, 0, time.UTC),
	2036: time.Date(2036, 4, 13, 0, 0, 0, 0, time.UTC),
	2037: time.Date(2037, 4, 5, 0, 0, 0, 0, time.UTC),
	2038: time.Date(2038, 4, 25, 0, 0, 0, 0, time.UTC),
	2039: time.Date(2039, 4, 10, 0, 0, 0, 0, time.UTC),
	2040: time.Date(2040, 4, 1, 0, 0, 0, 0, time.UTC),
	2041: time.Date(2041, 4, 21, 0, 0, 0, 0, time.UTC),
	2042: time.Date(2042, 4, 6, 0, 0, 0, 0, time.UTC),
	2043: time.Date(2043, 3, 29, 0, 0, 0, 0, time.UTC),
	2044: time.Date(2044, 4, 17, 0, 0, 0, 0, time.UTC),
	2045: time.Date(2045, 4, 9, 0, 0, 0, 0, time.UTC),
	2046: time.Date(2046, 3, 25, 0, 0, 0, 0, time.UTC),
	2047: time.Date(2047, 4, 14, 0, 0, 0, 0, time.UTC),
	2048: time.Date(2048, 4, 5, 0, 0, 0, 0, time.UTC),
	2049: time.Date(2049, 4, 18, 0, 0, 0, 0, time.UTC),
	2050: time.Date(2050, 4, 10, 0, 0, 0, 0, time.UTC),
	2051: time.Date(2051, 4, 2, 0, 0, 0, 0, time.UTC),
	2052: time.Date(2052, 4, 21, 0, 0, 0, 0, time.UTC),
	2053: time.Date(2053, 4, 6, 0, 0, 0, 0, time.UTC),
	2054: time.Date(2054, 3, 29, 0, 0, 0, 0, time.UTC),
	2055: time.Date(2055, 4, 18, 0, 0, 0, 0, time.UTC),
	2056: time.Date(2056, 4, 2, 0, 0, 0, 0, time.UTC),
	2057: time.Date(2057, 4, 22, 0, 0, 0, 0, time.UTC),
	2058: time.Date(2058, 4, 14, 0, 0, 0, 0, time.UTC),
	2059: time.Date(2059, 3, 30, 0, 0, 0, 0, time.UTC),
	2060: time.Date(2060, 4, 18, 0, 0, 0, 0, time.UTC),
}

// holidayCache stores per-year holiday sets so they are only computed once.
// Keys are int (year), values are map[time.Time]struct{}.
var holidayCache sync.Map

// PricingService handles price fetching with PostgreSQL cache.
// priceClient is used for stock prices (e.g. FinancialData.net, AlphaVantage, EODHD).
// eventClient is used for corporate action events (splits + dividends).
// fredClient is used for US10Y treasury rates (FRED).
type PricingService struct {
	priceRepo   *repository.PriceRepository
	secRepo     *repository.SecurityRepository
	priceClient providers.StockPriceFetcher
	eventClient providers.StockEventFetcher
	fredClient  providers.TreasuryRateFetcher
	bulkClient  providers.BulkPriceFetcher
	// fetchSem is a global semaphore that caps concurrent provider (EODHD/FRED) connections
	// across ALL callers. Default capacity is 1 (sequential). Use WithConcurrency to raise it.
	fetchSem chan struct{}
}

// NewPricingService creates a new PricingService
func NewPricingService(
	priceRepo *repository.PriceRepository,
	secRepo *repository.SecurityRepository,
	priceClient providers.StockPriceFetcher,
	eventClient providers.StockEventFetcher,
	fredClient providers.TreasuryRateFetcher,
	bulkClient providers.BulkPriceFetcher,
) *PricingService {
	return &PricingService{
		priceRepo:   priceRepo,
		secRepo:     secRepo,
		priceClient: priceClient,
		eventClient: eventClient,
		fredClient:  fredClient,
		bulkClient:  bulkClient,
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
	// Acquire global concurrency slot to cap simultaneous provider connections.
	select {
	case s.fetchSem <- struct{}{}:
		defer func() { <-s.fetchSem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	var fetchedPrices []providers.ParsedPriceData
	var err error
	hasSplits := false

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
		if fetchStyle == "full" && security.Ticker != "US10Y" && security.Inception == nil && !minDate.IsZero() {
			if err := s.secRepo.UpdateInceptionDate(ctx, securityID, &minDate); err != nil {
				log.Warnf("failed to infer inception date for %s: %v", security.Ticker, err)
			} else {
				log.Infof("inferred inception date %s for %s from earliest price", minDate.Format("2006-01-02"), security.Ticker)
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
		if security.Ticker == "US10Y" {
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

// IsUSMarketHoliday reports whether d is an NYSE market holiday or ad-hoc closure.
// NYSE observes: New Year's Day, MLK Day, Presidents' Day, Good Friday,
// Memorial Day, Juneteenth (since 2022), Independence Day, Labor Day,
// Thanksgiving, and Christmas — with fixed holidays shifted to the nearest
// weekday when they fall on a weekend (Saturday → Friday, Sunday → Monday).
// Ad-hoc closures (9/11, Hurricane Sandy, presidential mourning days) are also covered.
//
// Holiday sets are computed once per year and cached; repeated calls within the same
// year (e.g. from generateMoneyMarketPrices) pay only an O(1) map lookup.
func IsUSMarketHoliday(d time.Time) bool {
	target := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)

	if adHocClosures[target] {
		return true
	}

	year := d.Year()
	val, ok := holidayCache.Load(year)
	if !ok {
		val, _ = holidayCache.LoadOrStore(year, buildHolidaySet(year))
	}
	set := val.(map[time.Time]struct{})
	_, found := set[target]
	return found
}

// buildHolidaySet computes the full set of NYSE statutory holidays for the given year.
// Called at most once per year; result is stored in holidayCache.
func buildHolidaySet(year int) map[time.Time]struct{} {
	// observedDate returns the weekday a fixed holiday is observed on.
	// Saturday holidays are observed the preceding Friday; Sunday holidays the following Monday.
	observedDate := func(y int, month time.Month, day int) time.Time {
		h := time.Date(y, month, day, 0, 0, 0, 0, time.UTC)
		switch h.Weekday() {
		case time.Saturday:
			return h.AddDate(0, 0, -1)
		case time.Sunday:
			return h.AddDate(0, 0, 1)
		}
		return h
	}

	// nthWeekday returns the nth occurrence of weekday in month/year.
	nthWeekday := func(y int, month time.Month, weekday time.Weekday, n int) time.Time {
		t := time.Date(y, month, 1, 0, 0, 0, 0, time.UTC)
		for t.Weekday() != weekday {
			t = t.AddDate(0, 0, 1)
		}
		return t.AddDate(0, 0, 7*(n-1))
	}

	// lastWeekday returns the last occurrence of weekday in month/year.
	lastWeekday := func(y int, month time.Month, weekday time.Weekday) time.Time {
		t := time.Date(y, month+1, 0, 0, 0, 0, 0, time.UTC)
		for t.Weekday() != weekday {
			t = t.AddDate(0, 0, -1)
		}
		return t
	}

	// goodFriday returns Good Friday for the year. Easter Sunday dates are looked up
	// from the easterSundays table (1990–2060). For out-of-range years the
	// Meeus/Jones/Butcher algorithm is used as a fallback.
	goodFriday := func(y int) time.Time {
		if easter, ok := easterSundays[y]; ok {
			return easter.AddDate(0, 0, -2)
		}
		// Meeus/Jones/Butcher algorithm (fallback for years outside the table)
		a := y % 19
		b := y / 100
		c := y % 100
		d := b / 4
		e := b % 4
		f := (b + 8) / 25
		g := (b - f + 1) / 3
		h := (19*a + b - d - g + 15) % 30
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
		observedDate(year+1, time.January, 1),           // New Year's Day (observed Dec 31 when Jan 1 is Saturday)
		nthWeekday(year, time.January, time.Monday, 3),  // MLK Day
		nthWeekday(year, time.February, time.Monday, 3), // Presidents' Day
		goodFriday(year),                                 // Good Friday
		lastWeekday(year, time.May, time.Monday),         // Memorial Day
		observedDate(year, time.July, 4),                 // Independence Day
		nthWeekday(year, time.September, time.Monday, 1), // Labor Day
		nthWeekday(year, time.November, time.Thursday, 4), // Thanksgiving
		observedDate(year, time.December, 25),            // Christmas
	}
	if year >= 2022 {
		holidays = append(holidays, observedDate(year, time.June, 19)) // Juneteenth
	}

	set := make(map[time.Time]struct{}, len(holidays))
	for _, h := range holidays {
		set[h] = struct{}{}
	}
	return set
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
// It handles timezone conversion, business day logic, and NYSE holidays.
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
	isTradingDay := isWeekday && !IsUSMarketHoliday(nyTime)

	if !(isTradingDay && isBeforeCutoff) {
		// Roll forward to next trading day
		target = target.AddDate(0, 0, 1)
		for target.Weekday() == time.Saturday || target.Weekday() == time.Sunday || IsUSMarketHoliday(target) {
			target = target.AddDate(0, 0, 1)
		}
	}

	return target
}

// LastMarketClose returns the most recent time end-of-day market data was available.
// If now is on a trading day at or after 4:30 PM ET, that is the last close.
// Otherwise it rolls back to the previous trading day at 4:30 PM ET.
// Use this instead of time.Now() when selecting an endDate for price queries.
func LastMarketClose(now time.Time) time.Time {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("Failed to load location: %v", err)
		return now.AddDate(0, 0, -1)
	}

	nyTime := now.In(nyLoc)
	target := time.Date(nyTime.Year(), nyTime.Month(), nyTime.Day(), 16, 30, 0, 0, nyLoc)

	isWeekday := nyTime.Weekday() >= time.Monday && nyTime.Weekday() <= time.Friday
	isTradingDay := isWeekday && !IsUSMarketHoliday(nyTime)
	isAfterCutoff := !nyTime.Before(target)

	if isTradingDay && isAfterCutoff {
		return target
	}

	// Roll back to previous trading day
	target = target.AddDate(0, 0, -1)
	for target.Weekday() == time.Saturday || target.Weekday() == time.Sunday || IsUSMarketHoliday(target) {
		target = target.AddDate(0, 0, -1)
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
		for target.Weekday() == time.Saturday || target.Weekday() == time.Sunday || IsUSMarketHoliday(target) {
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

// BulkFetchEODHDPrices fetches end-of-day prices for all securities on an exchange
// from EODHD, then stores prices for any security in secsByTicker.
// secsByTicker should be pre-loaded by the caller (e.g. from SecurityRepository.GetAllUS)
// to avoid a per-record database lookup across thousands of tickers.
func (s *PricingService) BulkFetchEODHDPrices(ctx context.Context, exchange string, date time.Time, secsByTicker map[string]*models.Security) (*models.BulkFetchResult, error) {
	result := &models.BulkFetchResult{
		Exchange: exchange,
		Date:     date.Format("2006-01-02"),
	}

	records, err := s.bulkClient.GetBulkEOD(ctx, exchange, date)
	if err != nil {
		return nil, fmt.Errorf("EODHD bulk fetch failed: %w", err)
	}
	result.Fetched = len(records)

	var prices []models.PriceData
	for _, rec := range records {
		sec, ok := secsByTicker[rec.Code]
		if !ok {
			result.Skipped++
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
	}

	if len(prices) > 0 {
		if err := s.priceRepo.StoreDailyPrices(ctx, prices); err != nil {
			return nil, fmt.Errorf("failed to store bulk prices: %w", err)
		}
	}
	result.Stored = len(prices)

	log.Infof("BulkFetchEODHDPrices: exchange=%s date=%s fetched=%d stored=%d skipped=%d",
		exchange, result.Date, result.Fetched, result.Stored, result.Skipped)

	return result, nil
}

func (s *PricingService) GetAggregatePortfolioDividends(ctx context.Context, portfolioID int64, startDate, endDate time.Time) ([]models.EventData, error) {
	return s.priceRepo.GetAggregatePortfolioDividends(ctx, portfolioID, startDate, endDate)
}

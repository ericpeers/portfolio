package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// PricingService handles price fetching with PostgreSQL cache.
// fdClient is used for stock prices (FinancialData.net).
// fredClient is used for US10Y treasury rates (FRED).
type PricingService struct {
	priceRepo  *repository.PriceRepository
	secRepo    *repository.SecurityRepository
	fdClient   providers.StockPriceFetcher
	fredClient providers.TreasuryRateFetcher
}

// NewPricingService creates a new PricingService
func NewPricingService(
	priceRepo *repository.PriceRepository,
	secRepo *repository.SecurityRepository,
	fdClient providers.StockPriceFetcher,
	fredClient providers.TreasuryRateFetcher,
) *PricingService {
	return &PricingService{
		priceRepo:  priceRepo,
		secRepo:    secRepo,
		fdClient:   fdClient,
		fredClient: fredClient,
	}
}

// GetDailyPrices fetches daily prices using PostgreSQL cache and FinancialData.net / AlphaVantage.
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
		effectiveStart = *inception
	}

	// Check fact_price_range to determine caching status
	priceRange, err := s.priceRepo.GetPriceRange(ctx, securityID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get price range: %w", err)
	}

	currentDT := time.Now() //grab time once to use in a couple spots below

	//"full" for all data, "compact" for last 100
	needsFetch, fetchStyle := DetermineFetch(priceRange, currentDT, effectiveStart, endDate)

	if needsFetch {
		err := fetchAndStore(ctx, needsFetch, security, s, fetchStyle, securityID, currentDT, priceRange)
		if err != nil {
			return nil, nil, err
		}
	}

	// Query fact_price for the requested range (using effective start for pre-IPO requests)
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
// US10Y is fetched from FRED (incremental date range); all other securities from FinancialData.net.
// FD prices are pre-adjusted so hasSplits=false for FD; AV returns split/dividend events.
func fetchAndStore(ctx context.Context, needsFetch bool, security *models.SecurityWithCountry, s *PricingService, fetchStyle string, securityID int64, currentDT time.Time, priceRange *repository.PriceRange) error {
	var fetchedPrices []providers.ParsedPriceData
	var err error
	hasSplits := false

	if security.Symbol == "US10Y" {
		// Fetch only the missing date range from FRED (incremental caching).
		// DGS10 historical data starts 1962-01-02.
		fredStart := time.Date(1962, 1, 2, 0, 0, 0, 0, time.UTC)
		if priceRange != nil {
			fredStart = priceRange.EndDate.AddDate(0, 0, 1)
		}
		fredEnd := currentDT

		fetchedPrices, err = s.fredClient.GetTreasuryRate(ctx, fredStart, fredEnd)
		if err != nil {
			return fmt.Errorf("failed to fetch Treasuries from FRED: %w", err)
		}
	} else {
		fetchedPrices, err = s.fdClient.GetDailyPrices(ctx, security, fetchStyle)
		if err != nil {
			return fmt.Errorf("failed to fetch prices from FinancialData.net: %w", err)
		}
	}

	// Convert all prices
	var allPrices []models.PriceData
	var allEvents []models.EventData

	var minDate, maxDate time.Time
	for _, p := range fetchedPrices {
		priceData := models.PriceData{
			SecurityID: securityID,
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
					SecurityID:       securityID,
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

		var nextUpdate time.Time
		if security.Symbol == "US10Y" {
			nextUpdate = NextTreasuryUpdateDate(currentDT)
		} else {
			nextUpdate = NextMarketDate(currentDT)
		}

		// Update the price range (uses LEAST/GREATEST to expand)
		if err := s.priceRepo.UpsertPriceRange(ctx, securityID, minDate, maxDate, nextUpdate); err != nil {
			fmt.Printf("warning: failed to update price range: %v\n", err)
		}
	}

	if len(allEvents) != 0 {
		if err := s.priceRepo.StoreDailyEvents(ctx, allEvents); err != nil {
			log.Errorf("warning: failed to store events: %v\n", err)
		}
	}

	return nil
}

func DetermineFetch(priceRange *repository.PriceRange, currentDT time.Time, effectiveStart time.Time, endDate time.Time) (bool, string) {
	if priceRange == nil {
		// No cached data at all - need full fetch
		return true, "full"
	}

	startCovered := !effectiveStart.Before(priceRange.StartDate)

	if !startCovered {
		// Historical data we've never fetched — must fetch regardless of NextUpdate
		if currentDT.Sub(priceRange.EndDate).Hours()/24.0 < 100.0 {
			return true, "compact"
		}
		return true, "full"
	}

	// Start is covered. Use NextUpdate for refresh timing.
	// Handles both "fully covered" and "end gap" (data not yet available) correctly.
	if priceRange.NextUpdate.After(currentDT) {
		return false, ""
	}

	// NextUpdate has passed — time to refresh.
	if currentDT.Sub(priceRange.EndDate).Hours()/24.0 < 100.0 {
		return true, "compact"
	}
	return true, "full"
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

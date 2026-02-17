package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// PricingService handles price fetching with PostgreSQL cache and AlphaVantage
type PricingService struct {
	priceRepo *repository.PriceCacheRepository
	secRepo   *repository.SecurityRepository
	avClient  *alphavantage.Client
}

// NewPricingService creates a new PricingService
func NewPricingService(
	priceRepo *repository.PriceCacheRepository,
	secRepo *repository.SecurityRepository,
	avClient *alphavantage.Client,
) *PricingService {
	return &PricingService{
		priceRepo: priceRepo,
		secRepo:   secRepo,
		avClient:  avClient,
	}
}

// GetDailyPrices fetches daily prices using PostgreSQL cache and AlphaVantage
// It respects IPO/inception dates and uses intelligent caching via fact_price_range
func (s *PricingService) GetDailyPrices(ctx context.Context, securityID int64, startDate, endDate time.Time) ([]models.PriceData, []models.EventData, error) {
	// Get security for symbol lookup and inception date
	// FIXME: We already check for inception dates in comparison_service. Do we need to repeat it here
	security, err := s.secRepo.GetByID(ctx, securityID) //FIXME. Singleton fetch. Why are we not using the list of ID's?
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

	if needsFetch { //FIXME: This needsFetch logic should go to a separate subroutine for readability.
		// Fetch from AlphaVantage
		var avPrices []alphavantage.ParsedPriceData
		var err error

		if security.Symbol == "US10Y" {
			avPrices, err = s.avClient.GetTreasuryRate(ctx)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to fetch Treasuries from AlphaVantage: %w", err)
			}
		} else {
			avPrices, err = s.avClient.GetDailyPrices(ctx, security.Symbol, fetchStyle)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to fetch prices from AlphaVantage: %w", err)
			}
		}

		// Convert all prices
		var allPrices []models.PriceData
		var allEvents []models.EventData

		var minDate, maxDate time.Time
		for _, p := range avPrices {
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

			if (p.SplitCoefficient != 1.0 && p.SplitCoefficient != 0.0) || (p.Dividend != 0) {
				eventData := models.EventData{
					SecurityID:       securityID,
					Date:             p.Date,
					Dividend:         p.Dividend,
					SplitCoefficient: p.SplitCoefficient,
				}
				allEvents = append(allEvents, eventData)
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

			nextUpdate := NextMarketDate(currentDT)

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

// GetQuote fetches a real-time quote using PostgreSQL cache
func (s *PricingService) GetQuote(ctx context.Context, securityID int64) (*models.Quote, error) {
	// Check PostgreSQL cache (quotes valid for 5 minutes)
	quote, err := s.priceRepo.GetCachedQuote(ctx, securityID, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to get quote from DB: %w", err)
	}
	if quote != nil {
		return quote, nil
	}

	// L3: Fetch from AlphaVantage
	security, err := s.secRepo.GetByID(ctx, securityID)
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}

	avQuote, err := s.avClient.GetQuote(ctx, security.Symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch quote from AlphaVantage: %w", err)
	}

	quote = &models.Quote{
		SecurityID: securityID,
		Symbol:     avQuote.Symbol,
		Price:      avQuote.Price,
		FetchedAt:  time.Now(),
	}

	// Cache in PostgreSQL
	if err := s.priceRepo.CacheQuote(ctx, quote); err != nil {
		fmt.Printf("warning: failed to cache quote: %v\n", err)
	}

	return quote, nil
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

// ComputeInstantValue calculates the current value of a portfolio
func (s *PricingService) ComputeInstantValue(ctx context.Context, memberships []models.PortfolioMembership, portfolioType models.PortfolioType) (float64, error) {
	// This code may be optimized to fetch security prices just once...
	// Currently fetches prices for each membership individually

	var totalValue float64

	for _, m := range memberships {
		quote, err := s.GetQuote(ctx, m.SecurityID)
		if err != nil {
			return 0, fmt.Errorf("failed to get quote for security %d: %w", m.SecurityID, err)
		}

		if portfolioType == models.PortfolioTypeIdeal {
			// For ideal portfolios, percentage_or_shares is a percentage
			// We assume a base value for calculation purposes
			totalValue += m.PercentageOrShares // Will be normalized later
		} else {
			// For active portfolios, percentage_or_shares is share count
			totalValue += m.PercentageOrShares * quote.Price
		}
	}

	return totalValue, nil
}

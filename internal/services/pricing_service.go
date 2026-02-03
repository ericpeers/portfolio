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
func (s *PricingService) GetDailyPrices(ctx context.Context, securityID int64, startDate, endDate time.Time) ([]models.PriceData, error) {
	// Get security for symbol lookup and inception date
	security, err := s.secRepo.GetByID(ctx, securityID)
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
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
		return nil, fmt.Errorf("failed to get price range: %w", err)
	}

	currentDT := time.Now() //grab time once to use in a couple spots below

	//"full" for all data, "compact" for last 100
	needsFetch, fetchStyle := DetermineFetch(priceRange, currentDT, effectiveStart, endDate)

	if needsFetch {
		// Fetch from AlphaVantage
		var avPrices []alphavantage.ParsedPriceData
		var err error

		if security.Symbol == "US10Y" {
			avPrices, err = s.avClient.GetTreasuryRate(ctx, fetchStyle)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch Treasuries from AlphaVantage: %w", err)
			}
		} else {
			avPrices, err = s.avClient.GetDailyPrices(ctx, security.Symbol, fetchStyle)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch prices from AlphaVantage: %w", err)
			}
		}

		// Convert all prices
		var allPrices []models.PriceData
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
			if err := s.priceRepo.CacheDailyPrices(ctx, allPrices); err != nil {
				fmt.Printf("warning: failed to cache prices: %v\n", err)
			}

			nextUpdate := NextMarketDate(currentDT)

			// Update the price range (uses LEAST/GREATEST to expand)
			if err := s.priceRepo.UpsertPriceRange(ctx, securityID, minDate, maxDate, nextUpdate); err != nil {
				fmt.Printf("warning: failed to update price range: %v\n", err)
			}
		}
	}

	// Query fact_price for the requested range (using effective start for pre-IPO requests)
	prices, err := s.priceRepo.GetDailyPrices(ctx, securityID, effectiveStart, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get prices from DB: %w", err)
	}

	return prices, nil
}

func DetermineFetch(priceRange *repository.PriceRange, currentDT time.Time, effectiveStart time.Time, endDate time.Time) (bool, string) {
	needsFetch := false
	fetchStyle := "full"
	if priceRange == nil {
		// No cached data at all - need to fetch
		needsFetch = true
		fetchStyle = "full"
	} else {
		//On holidays, weekends, there is no update for pricing data. So don't keep re-fetching
		//if there won't be an update. Normally quotes are 15 minutes delayed, so we will push to 4:30
		//current time might be in

		if priceRange.NextUpdate.After(currentDT) {
			//it's too soon to ask again. Skip over it.
			//also let the time function handle timezone shifts. e.g. we are in mountain time, but nextDT was computed using
			// eastern time, and postgres stored it as UTC in a timestamptz
		} else {
			// Check if the requested range extends beyond previously cached range
			// But respect inception date - if request is before IPO but end is within cache, no refetch needed
			rangeCoversRequest := !(effectiveStart.Before(priceRange.StartDate) || endDate.After(priceRange.EndDate))
			if !rangeCoversRequest {
				// Check if we're requesting pre-IPO data that we can't get anyway
				needsFetch = true
				if currentDT.Sub(priceRange.EndDate).Hours()/24.0 < 100.0 {
					fetchStyle = "compact"
				}
			}
		}
	}
	return needsFetch, fetchStyle
}

// NextMarketDate predicts the date of the next stock market update.
// It handles timezone conversion, business day logic.
// it returns the next target date, in New York time, 4:30pm.
func NextMarketDate(input time.Time) time.Time {

	// 1. Load the target timezone: America/New_York
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("Failed to load location: %w", err)
		return input.AddDate(0, 0, 1)
	}

	// 2. Convert the input time to New York time for calculation
	nyTime := input.In(nyLoc)

	// 3. Define the Cutoff: 4:30 PM (16:30)
	cutoffHour, cutoffMinute := 16, 30

	isWeekday := nyTime.Weekday() >= time.Monday && nyTime.Weekday() <= time.Friday
	isBeforeCutoff := nyTime.Hour() < cutoffHour || (nyTime.Hour() == cutoffHour && nyTime.Minute() < cutoffMinute)

	// Determine the target date
	var targetDate time.Time

	if isWeekday && isBeforeCutoff {
		// Case A: Today is valid
		targetDate = nyTime
	} else {
		// Case B: Roll forward to the next day
		targetDate = nyTime.AddDate(0, 0, 1)
		// Skip weekends
		for targetDate.Weekday() == time.Saturday || targetDate.Weekday() == time.Sunday {
			targetDate = targetDate.AddDate(0, 0, 1)
		}
	}

	return targetDate
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
	prices, err := s.GetDailyPrices(ctx, securityID, startDate, date)
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

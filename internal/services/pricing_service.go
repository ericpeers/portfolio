package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/cache"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

// PricingService handles price fetching with a 3-tier cache
type PricingService struct {
	memCache   *cache.MemoryCache
	priceRepo  *repository.PriceCacheRepository
	secRepo    *repository.SecurityRepository
	avClient   *alphavantage.Client
}

// NewPricingService creates a new PricingService
func NewPricingService(
	memCache *cache.MemoryCache,
	priceRepo *repository.PriceCacheRepository,
	secRepo *repository.SecurityRepository,
	avClient *alphavantage.Client,
) *PricingService {
	return &PricingService{
		memCache:  memCache,
		priceRepo: priceRepo,
		secRepo:   secRepo,
		avClient:  avClient,
	}
}

// GetDailyPrices fetches daily prices using 3-tier cache: memory -> postgres -> AlphaVantage
// It respects IPO/inception dates and uses intelligent caching via fact_price_range
func (s *PricingService) GetDailyPrices(ctx context.Context, securityID int64, startDate, endDate time.Time) ([]models.PriceData, error) {
	// L1: Check memory cache
	if prices, found := s.memCache.GetPrices(securityID, startDate, endDate); found {
		return prices, nil
	}

	// Get security for symbol lookup
	security, err := s.secRepo.GetByID(ctx, securityID)
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}

	// Get inception date to determine effective start date
	inception, err := s.priceRepo.GetSecurityInception(ctx, securityID)
	if err != nil {
		return nil, fmt.Errorf("failed to get security inception: %w", err)
	}

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

	needsFetch := false
	if priceRange == nil {
		// No cached data at all - need to fetch
		needsFetch = true
	} else {
		// Check if the requested range extends beyond cached range
		// But respect inception date - if request is before IPO but end is within cache, no refetch needed
		rangeCoversRequest := !effectiveStart.Before(priceRange.StartDate) && !endDate.After(priceRange.EndDate)
		if !rangeCoversRequest {
			// Check if we're requesting pre-IPO data that we can't get anyway
			if inception != nil && startDate.Before(*inception) && !endDate.After(priceRange.EndDate) {
				// Request starts before IPO but ends within cached range - no refetch needed
				// We already have all available data
			} else {
				needsFetch = true
			}
		}
	}

	if needsFetch {
		// Fetch from AlphaVantage
		avPrices, err := s.avClient.GetDailyPrices(ctx, security.Symbol, "full")
		if err != nil {
			return nil, fmt.Errorf("failed to fetch prices from AlphaVantage: %w", err)
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

			// Update the price range (uses LEAST/GREATEST to expand)
			if err := s.priceRepo.UpsertPriceRange(ctx, securityID, minDate, maxDate); err != nil {
				fmt.Printf("warning: failed to update price range: %v\n", err)
			}
		}
	}

	// Query fact_price for the requested range (using effective start for pre-IPO requests)
	prices, err := s.priceRepo.GetDailyPrices(ctx, securityID, effectiveStart, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get prices from DB: %w", err)
	}

	// Cache filtered results in memory
	s.memCache.SetPrices(securityID, startDate, endDate, prices)

	return prices, nil
}

// GetQuote fetches a real-time quote using 3-tier cache
func (s *PricingService) GetQuote(ctx context.Context, securityID int64) (*models.Quote, error) {
	// L1: Check memory cache
	if quote, found := s.memCache.GetQuote(securityID); found {
		return quote, nil
	}

	// L2: Check PostgreSQL cache (quotes valid for 5 minutes)
	quote, err := s.priceRepo.GetCachedQuote(ctx, securityID, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to get quote from DB: %w", err)
	}
	if quote != nil {
		s.memCache.SetQuote(securityID, quote)
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

	// Cache in memory
	s.memCache.SetQuote(securityID, quote)

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

// GetTreasuryRates fetches US10Y treasury rates for a date range
func (s *PricingService) GetTreasuryRates(ctx context.Context, startDate, endDate time.Time) ([]repository.TreasuryRate, error) {
	// Check PostgreSQL cache first
	rates, err := s.priceRepo.GetTreasuryRates(ctx, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get treasury rates from DB: %w", err)
	}
	if len(rates) > 0 {
		return rates, nil
	}

	// Fetch from AlphaVantage
	avRates, err := s.avClient.GetTreasuryRate(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch treasury rates from AlphaVantage: %w", err)
	}

	// Convert and filter
	var allRates []repository.TreasuryRate
	var result []repository.TreasuryRate
	for _, r := range avRates {
		tr := repository.TreasuryRate{
			Date: r.Date,
			Rate: r.Rate,
		}
		allRates = append(allRates, tr)
		if !r.Date.Before(startDate) && !r.Date.After(endDate) {
			result = append(result, tr)
		}
	}

	// Cache all rates
	if err := s.priceRepo.CacheTreasuryRates(ctx, allRates); err != nil {
		fmt.Printf("warning: failed to cache treasury rates: %v\n", err)
	}

	return result, nil
}

package services

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

const tradingDaysPerYear = 252

// PerformanceService handles portfolio performance calculations
type PerformanceService struct {
	pricingSvc    *PricingService
	portfolioRepo *repository.PortfolioRepository
	secRepo       *repository.SecurityRepository
}

// NewPerformanceService creates a new PerformanceService
func NewPerformanceService(
	pricingSvc *PricingService,
	portfolioRepo *repository.PortfolioRepository,
	secRepo *repository.SecurityRepository,

) *PerformanceService {
	return &PerformanceService{
		pricingSvc:    pricingSvc,
		portfolioRepo: portfolioRepo,
		secRepo:       secRepo,
	}
}

// NormalizeIdealPortfolio converts an ideal portfolio's percentages to share-based holdings.
// Returns a new PortfolioWithMemberships where PercentageOrShares contains computed shares.
// For actual portfolios, use the original pointer directly (no normalization needed).
//
// NOTE: This code may be optimized by collapsing price fetches between NormalizeIdealPortfolio
// and ComputeDailyValues. Consider retaining an in-memory cache of date/price points for
// securities to minimize postgres fetches.
func (s *PerformanceService) NormalizeIdealPortfolio(ctx context.Context, portfolio *models.PortfolioWithMemberships, startDate time.Time, targetStartValue float64) (*models.PortfolioWithMemberships, error) {
	if portfolio.Portfolio.PortfolioType != models.PortfolioTypeIdeal {
		// Actual portfolios don't need normalization - use original pointer
		return portfolio, nil
	}

	// Calculate total percentage
	var totalPct float64
	for _, m := range portfolio.Memberships {
		totalPct += m.PercentageOrShares
	}

	// Create new portfolio with computed shares
	normalized := &models.PortfolioWithMemberships{
		Portfolio: portfolio.Portfolio,
	}

	// Convert percentages to shares based on start prices
	// FIXME: This should be a bulk fetch. GetPriceAtDate finds the price at that date,
	// or the preceding business day. Consider bulk fetching all prices from postgres
	// for the start date, with fallback to GetPriceAtOrBeforeDate for missing data.
	// This will be slow for large portfolios (e.g., 2000 securities).
	for _, m := range portfolio.Memberships {
		price, err := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, startDate)
		if err != nil {
			return nil, fmt.Errorf("failed to get price for security %d: %w", m.SecurityID, err)
		}

		allocationDollars := targetStartValue * (m.PercentageOrShares / totalPct)
		shares := allocationDollars / price

		normalized.Memberships = append(normalized.Memberships, models.PortfolioMembership{
			PortfolioID:        m.PortfolioID,
			SecurityID:         m.SecurityID,
			PercentageOrShares: shares,
		})
	}

	return normalized, nil
}

// GainResult contains gain calculations
type GainResult struct {
	StartValue  float64
	EndValue    float64
	GainDollar  float64
	GainPercent float64
}

// ComputeGain calculates dollar and percentage returns.
// PercentageOrShares is treated as shares (works for actual portfolios or normalized ideal portfolios).
func (s *PerformanceService) ComputeGain(ctx context.Context, portfolio *models.PortfolioWithMemberships, startValue float64, endDate time.Time) (*GainResult, error) {
	var endValue float64
	for _, m := range portfolio.Memberships {
		price, err := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, endDate)
		if err != nil {
			return nil, fmt.Errorf("failed to get price for security %d: %w", m.SecurityID, err)
		}
		endValue += m.PercentageOrShares * price
	}

	gainDollar := endValue - startValue
	gainPercent := 0.0
	if startValue > 0 {
		gainPercent = (gainDollar / startValue) * 100
	}

	return &GainResult{
		StartValue:  startValue,
		EndValue:    endValue,
		GainDollar:  gainDollar,
		GainPercent: gainPercent,
	}, nil
}

// ComputeSharpe calculates Sharpe ratios from pre-computed daily values
// Calculate risk-free values: (1+i/n)^n-1, n=252
// Return: day (1×), month (√20×), 3m (√60×), year (√252×)
func (s *PerformanceService) ComputeSharpe(ctx context.Context, dailyValues []DailyValue, startDate, endDate time.Time) (*models.SharpeRatios, error) {
	if len(dailyValues) < 2 {
		return &models.SharpeRatios{}, nil
	}

	// Get treasury rates for risk-free rate
	US10Y, err := s.secRepo.GetBySymbol(ctx, "US10Y")
	if err != nil {
		return nil, fmt.Errorf("failed to get US10Y security: %w", err)
	}

	treasuryRates, err := s.pricingSvc.GetDailyPrices(ctx, US10Y.ID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get treasury rates: %w", err)
	}

	// Calculate average risk-free rate
	var avgRiskFreeRate float64
	if len(treasuryRates) > 0 {
		var sum float64
		for _, tr := range treasuryRates {
			sum += tr.Close
		}
		avgRiskFreeRate = sum / float64(len(treasuryRates))
	}

	// Calculate daily risk-free rate: (1+i/n)^n - 1 where i is annual rate, n=252
	dailyRiskFreeRate := math.Pow(1+avgRiskFreeRate/100/tradingDaysPerYear, 1) - 1

	// Calculate daily returns and excess returns
	var excessReturns []float64
	for i := 1; i < len(dailyValues); i++ {
		dailyReturn := (dailyValues[i].Value - dailyValues[i-1].Value) / dailyValues[i-1].Value
		excessReturn := dailyReturn - dailyRiskFreeRate
		excessReturns = append(excessReturns, excessReturn)
	}

	// Calculate mean excess return
	var sumExcess float64
	for _, er := range excessReturns {
		sumExcess += er
	}
	meanExcessReturn := sumExcess / float64(len(excessReturns))

	// Calculate standard deviation of excess returns
	var sumSquaredDiff float64
	for _, er := range excessReturns {
		diff := er - meanExcessReturn
		sumSquaredDiff += diff * diff
	}
	stdDevExcessReturn := math.Sqrt(sumSquaredDiff / float64(len(excessReturns)))

	// Calculate daily Sharpe ratio
	dailySharpe := 0.0
	if stdDevExcessReturn > 0 {
		dailySharpe = meanExcessReturn / stdDevExcessReturn
	}

	// Annualize Sharpe ratios for different periods
	// day (1×), month (√20×), 3m (√60×), year (√252×)
	return &models.SharpeRatios{
		Daily:      dailySharpe,
		Monthly:    dailySharpe * math.Sqrt(20),
		ThreeMonth: dailySharpe * math.Sqrt(60),
		Yearly:     dailySharpe * math.Sqrt(tradingDaysPerYear),
	}, nil
}

// DailyValue represents portfolio value on a specific date
type DailyValue struct {
	Date  time.Time
	Value float64
}

// ToModelDailyValues converts internal DailyValue slice to model DailyValue slice
func ToModelDailyValues(values []DailyValue) []models.DailyValue {
	result := make([]models.DailyValue, len(values))
	for i, v := range values {
		result[i] = models.DailyValue{
			Date:  v.Date.Format("2006-01-02"),
			Value: v.Value,
		}
	}
	return result
}

// ComputeDailyValues calculates the portfolio value for each trading day in the period.
// PercentageOrShares is treated as shares (works for actual portfolios or normalized ideal portfolios).
// Only returns dates where all securities in the portfolio have price data.
func (s *PerformanceService) ComputeDailyValues(ctx context.Context, portfolio *models.PortfolioWithMemberships, startDate, endDate time.Time) ([]DailyValue, error) {
	// Collect all security IDs
	secIDs := make([]int64, len(portfolio.Memberships))
	for i, m := range portfolio.Memberships {
		secIDs[i] = m.SecurityID
	}

	// Get price data for all securities
	pricesBySecID := make(map[int64]map[time.Time]float64)
	for _, secID := range secIDs {
		prices, err := s.pricingSvc.GetDailyPrices(ctx, secID, startDate, endDate)
		if err != nil {
			return nil, fmt.Errorf("failed to get prices for security %d: %w", secID, err)
		}

		priceMap := make(map[time.Time]float64)
		for _, p := range prices {
			priceMap[p.Date] = p.Close
		}
		pricesBySecID[secID] = priceMap
	}

	// Find all dates where we have prices for all securities
	dateSet := make(map[time.Time]bool)
	for _, priceMap := range pricesBySecID {
		for date := range priceMap {
			dateSet[date] = true
		}
	}

	// Sort dates
	var dates []time.Time
	for date := range dateSet {
		dates = append(dates, date)
	}
	sort.Slice(dates, func(i, j int) bool {
		return dates[i].Before(dates[j])
	})

	// Calculate portfolio value for each date
	var dailyValues []DailyValue
	for _, date := range dates {
		var value float64
		valid := true
		for _, m := range portfolio.Memberships {
			price, exists := pricesBySecID[m.SecurityID][date]
			if !exists {
				valid = false
				break
			}
			value += m.PercentageOrShares * price
		}
		if valid {
			dailyValues = append(dailyValues, DailyValue{
				Date:  date,
				Value: value,
			})
		}
	}

	return dailyValues, nil
}

// ComputeDividends calculates dividends received during the period (stub)
func (s *PerformanceService) ComputeDividends(ctx context.Context, portfolio *models.PortfolioWithMemberships, startDate, endDate time.Time) (float64, error) {
	// Stub implementation - would need dividend data from a data provider
	return 0, nil
}

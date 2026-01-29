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
}

// NewPerformanceService creates a new PerformanceService
func NewPerformanceService(
	pricingSvc *PricingService,
	portfolioRepo *repository.PortfolioRepository,
) *PerformanceService {
	return &PerformanceService{
		pricingSvc:    pricingSvc,
		portfolioRepo: portfolioRepo,
	}
}

// NormalizedPortfolio represents a portfolio converted to share-based holdings
type NormalizedPortfolio struct {
	PortfolioID int64
	StartValue  float64
	Holdings    []NormalizedHolding
}

// NormalizedHolding represents a share-based holding
type NormalizedHolding struct {
	SecurityID int64
	Shares     float64
}

// NormalizePortfolios converts ideal portfolios to share-based for comparison
// Both ideal: assume $100 start value
// Otherwise: compute actual value via compute_instant_value
// Auto rebalancing of portfolios could be possible, but perhaps we want to handle that
// with a single portfolio. Reason: we would need portfolio shares held at start of period,
// not end of period
// how do we handle auto rebalancing? How do we handle divergence of an ideal portfolio over time?
func (s *PerformanceService) NormalizePortfolios(ctx context.Context, portfolioA, portfolioB *models.PortfolioWithMemberships, startDate time.Time) (*NormalizedPortfolio, *NormalizedPortfolio, error) {
	// This code may be optimized to fetch security prices just once...

	normA, err := s.normalizePortfolio(ctx, portfolioA, startDate)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to normalize portfolio A: %w", err)
	}

	normB, err := s.normalizePortfolio(ctx, portfolioB, startDate)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to normalize portfolio B: %w", err)
	}

	return normA, normB, nil
}

func (s *PerformanceService) normalizePortfolio(ctx context.Context, portfolio *models.PortfolioWithMemberships, startDate time.Time) (*NormalizedPortfolio, error) {
	norm := &NormalizedPortfolio{
		PortfolioID: portfolio.Portfolio.ID,
	}

	if portfolio.Portfolio.PortfolioType == models.PortfolioTypeIdeal {
		// For ideal portfolios, assume $100 start value
		norm.StartValue = 100.0

		// Calculate total percentage
		var totalPct float64
		for _, m := range portfolio.Memberships {
			totalPct += m.PercentageOrShares
		}

		// Convert percentages to shares based on start prices
		for _, m := range portfolio.Memberships {
			price, err := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, startDate)
			if err != nil {
				return nil, fmt.Errorf("failed to get price for security %d: %w", m.SecurityID, err)
			}

			// Allocation in dollars = $100 * (percentage / total)
			allocationDollars := norm.StartValue * (m.PercentageOrShares / totalPct)
			shares := allocationDollars / price

			norm.Holdings = append(norm.Holdings, NormalizedHolding{
				SecurityID: m.SecurityID,
				Shares:     shares,
			})
		}
	} else {
		// For active portfolios, use actual shares
		var totalValue float64
		for _, m := range portfolio.Memberships {
			price, err := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, startDate)
			if err != nil {
				return nil, fmt.Errorf("failed to get price for security %d: %w", m.SecurityID, err)
			}
			totalValue += m.PercentageOrShares * price

			norm.Holdings = append(norm.Holdings, NormalizedHolding{
				SecurityID: m.SecurityID,
				Shares:     m.PercentageOrShares,
			})
		}
		norm.StartValue = totalValue
	}

	return norm, nil
}

// GainResult contains gain calculations
type GainResult struct {
	StartValue  float64
	EndValue    float64
	GainDollar  float64
	GainPercent float64
}

// ComputeGain calculates dollar and percentage returns
func (s *PerformanceService) ComputeGain(ctx context.Context, norm *NormalizedPortfolio, endDate time.Time) (*GainResult, error) {
	var endValue float64
	for _, h := range norm.Holdings {
		price, err := s.pricingSvc.GetPriceAtDate(ctx, h.SecurityID, endDate)
		if err != nil {
			return nil, fmt.Errorf("failed to get price for security %d: %w", h.SecurityID, err)
		}
		endValue += h.Shares * price
	}

	gainDollar := endValue - norm.StartValue
	gainPercent := 0.0
	if norm.StartValue > 0 {
		gainPercent = (gainDollar / norm.StartValue) * 100
	}

	return &GainResult{
		StartValue:  norm.StartValue,
		EndValue:    endValue,
		GainDollar:  gainDollar,
		GainPercent: gainPercent,
	}, nil
}

// ComputeSharpe calculates Sharpe ratios for different time periods
// Calculate risk-free values: (1+i/n)^n-1, n=252
// Return: day (1×), month (√20×), 3m (√60×), year (√252×)
func (s *PerformanceService) ComputeSharpe(ctx context.Context, norm *NormalizedPortfolio, startDate, endDate time.Time) (*models.SharpeRatios, error) {
	// Get treasury rates for risk-free rate
	treasuryRates, err := s.pricingSvc.GetTreasuryRates(ctx, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get treasury rates: %w", err)
	}

	// Calculate average risk-free rate
	var avgRiskFreeRate float64
	if len(treasuryRates) > 0 {
		var sum float64
		for _, tr := range treasuryRates {
			sum += tr.Rate
		}
		avgRiskFreeRate = sum / float64(len(treasuryRates))
	}

	// Calculate daily risk-free rate: (1+i/n)^n - 1 where i is annual rate, n=252
	dailyRiskFreeRate := math.Pow(1+avgRiskFreeRate/100/tradingDaysPerYear, 1) - 1

	// Compute daily portfolio values
	dailyValues, err := s.computeDailyValues(ctx, norm, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to compute daily values: %w", err)
	}

	if len(dailyValues) < 2 {
		return &models.SharpeRatios{}, nil
	}

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

func (s *PerformanceService) computeDailyValues(ctx context.Context, norm *NormalizedPortfolio, startDate, endDate time.Time) ([]DailyValue, error) {
	// Collect all security IDs
	secIDs := make([]int64, len(norm.Holdings))
	for i, h := range norm.Holdings {
		secIDs[i] = h.SecurityID
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
		for _, h := range norm.Holdings {
			price, exists := pricesBySecID[h.SecurityID][date]
			if !exists {
				valid = false
				break
			}
			value += h.Shares * price
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
func (s *PerformanceService) ComputeDividends(ctx context.Context, norm *NormalizedPortfolio, startDate, endDate time.Time) (float64, error) {
	// Stub implementation - would need dividend data from a data provider
	return 0, nil
}

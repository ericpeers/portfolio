package services

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
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
		gainPercent = (gainDollar / startValue)
	}

	return &GainResult{
		StartValue:  startValue,
		EndValue:    endValue,
		GainDollar:  gainDollar,
		GainPercent: gainPercent,
	}, nil
}

// ComputeSharpe calculates Sharpe ratios from pre-computed daily values
// to convert a risk free value at an annual rate assuming n=interest rate, p=period
// daily_rate = (1+n)^(1/p)-1
// in this case, we would want p=252 for trading days in the year.
// also may need to divide n by 100 because it is represented as a percent, not as a decimal value: 4.52 (%) rather than 0.0452
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

	treasuryRates, _, err := s.pricingSvc.GetDailyPrices(ctx, US10Y.ID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get treasury rates: %w", err)
	}

	riskFree := make(map[time.Time]float64)

	// Calculate average risk-free rate
	var avgRiskFreeRate float64
	if len(treasuryRates) > 0 {
		var sum float64
		for _, tr := range treasuryRates {
			sum += tr.Close

			todayRiskFreeDailyRate := math.Pow(1.0+(float64(tr.Close)/100.0), 1.0/float64(tradingDaysPerYear)) - 1.0

			// Normalize date to UTC midnight to ensure consistent map key lookups
			normalizedDate := time.Date(tr.Date.Year(), tr.Date.Month(), tr.Date.Day(), 0, 0, 0, 0, time.UTC)
			riskFree[normalizedDate] = todayRiskFreeDailyRate
		}

		avgRiskFreeRate = sum / float64(len(treasuryRates))
	}

	dailyAvgRiskFreeRate := math.Pow(1.0+(avgRiskFreeRate/100.0), 1.0/float64(tradingDaysPerYear)) - 1.0

	// Calculate daily returns and excess returns
	var excessReturns []float64
	for i := 1; i < len(dailyValues); i++ {
		dailyReturn := (dailyValues[i].Value - dailyValues[i-1].Value) / dailyValues[i-1].Value

		// Normalize date to UTC midnight to match riskFree map keys
		normalizedDate := time.Date(dailyValues[i].Date.Year(), dailyValues[i].Date.Month(), dailyValues[i].Date.Day(), 0, 0, 0, 0, time.UTC)
		dailyRF, found := riskFree[normalizedDate]
		if !found {
			// Bond market closed but stock market open (Veterans Day, Columbus Day).
			// Interpolate from the surrounding trading days' risk-free rates.
			var prev, next float64
			var foundPrev, foundNext bool
			for offset := 1; offset <= 5; offset++ {
				if !foundPrev {
					if v, ok := riskFree[normalizedDate.AddDate(0, 0, -offset)]; ok {
						prev = v
						foundPrev = true
					}
				}
				if !foundNext {
					if v, ok := riskFree[normalizedDate.AddDate(0, 0, offset)]; ok {
						next = v
						foundNext = true
					}
				}
				if foundPrev && foundNext {
					break
				}
			}
			switch {
			case foundPrev && foundNext:
				dailyRF = (prev + next) / 2.0
			case foundPrev:
				dailyRF = prev
			case foundNext:
				dailyRF = next
			default:
				dailyRF = dailyAvgRiskFreeRate
			}
			log.Infof("Missing daily Risk Free Rate on day: %s, interpolated from neighbors", dailyValues[i].Date)
		}
		excessReturn := dailyReturn - dailyRF
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

	// Get price data and split events for all securities
	pricesBySecID := make(map[int64]map[time.Time]float64)
	splitsBySecID := make(map[int64]map[time.Time]float64)
	for _, secID := range secIDs {
		prices, splits, err := s.pricingSvc.GetDailyPrices(ctx, secID, startDate, endDate)
		if err != nil {
			return nil, fmt.Errorf("failed to get prices for security %d: %w", secID, err)
		}

		priceMap := make(map[time.Time]float64)
		for _, p := range prices {
			priceMap[p.Date] = p.Close
		}
		pricesBySecID[secID] = priceMap

		splitMap := make(map[time.Time]float64)
		for _, sp := range splits {
			splitMap[sp.Date] = sp.SplitCoefficient
		}
		splitsBySecID[secID] = splitMap
	}

	// Find all dates where we have prices for all securities
	// FIXME. This seems like we take any date for which we have a price for that security, contrary to the comment above.
	dateSet := make(map[time.Time]bool)
	for _, priceMap := range pricesBySecID {
		for date := range priceMap {
			dateSet[date] = true
		}
	}

	// FIXME. This seems inefficient. Are we really building a map, and then building a slice, and then sorting? why can't we just the the rows in order in the first place?
	// Sort dates
	var dates []time.Time
	for date := range dateSet {
		dates = append(dates, date)
	}
	sort.Slice(dates, func(i, j int) bool {
		return dates[i].Before(dates[j])
	})

	// Build mutable shares map — split adjustments accumulate over time
	sharesMap := make(map[int64]float64)
	for _, m := range portfolio.Memberships {
		sharesMap[m.SecurityID] = m.PercentageOrShares
	}

	// Calculate portfolio value for each date, adjusting shares on split dates
	var dailyValues []DailyValue
	for _, date := range dates {
		// Apply split adjustments before computing value.
		// On a 2-for-1 split, coefficient is 2: price halves, shares double.
		for _, m := range portfolio.Memberships {
			if coeff, ok := splitsBySecID[m.SecurityID][date]; ok {
				sharesMap[m.SecurityID] *= coeff
			}
		}

		var value float64
		valid := true
		for _, m := range portfolio.Memberships {
			price, exists := pricesBySecID[m.SecurityID][date]
			if !exists {
				valid = false
				break
			}
			value += sharesMap[m.SecurityID] * price
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

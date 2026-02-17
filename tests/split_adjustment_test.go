package tests

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/jackc/pgx/v5/pgxpool"
)

// insertSplitEvent inserts a split event into fact_event
func insertSplitEvent(pool *pgxpool.Pool, securityID int64, date time.Time, splitCoefficient float64) error {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO fact_event (security_id, date, dividend, split_coefficient)
		VALUES ($1, $2, 0, $3)
		ON CONFLICT (security_id, date) DO UPDATE
		SET split_coefficient = EXCLUDED.split_coefficient
	`, securityID, date, splitCoefficient)
	if err != nil {
		return fmt.Errorf("failed to insert split event: %w", err)
	}
	return nil
}

// insertPriceDataWithSplit inserts price data where price halves on the split date.
// Before split: basePrice. On/after split: basePrice / splitCoefficient.
func insertPriceDataWithSplit(pool *pgxpool.Pool, securityID int64, startDate, endDate time.Time, basePrice float64, splitDate time.Time, splitCoefficient float64) error {
	ctx := context.Background()

	price := basePrice
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		// On the split date, the price adjusts
		if d.Equal(splitDate) {
			price = basePrice / splitCoefficient
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO fact_price (security_id, date, open, high, low, close, volume)
			VALUES ($1, $2, $3, $4, $5, $6, 1000000)
			ON CONFLICT (security_id, date) DO NOTHING
		`, securityID, d, price, price+1, price-1, price)
		if err != nil {
			return fmt.Errorf("failed to insert price data: %w", err)
		}
	}

	// Set up price range
	futureNextUpdate := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := pool.Exec(ctx, `
		INSERT INTO fact_price_range (security_id, start_date, end_date, next_update)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (security_id) DO UPDATE SET start_date = $2, end_date = $3, next_update = $4
	`, securityID, startDate, endDate, futureNextUpdate)
	if err != nil {
		return fmt.Errorf("failed to insert price range: %w", err)
	}

	return nil
}

// TestSplitAdjustmentValueContinuity verifies that portfolio value is continuous across a stock split.
// On a 2-for-1 split, the price halves and shares double, so the portfolio value should stay the same.
func TestSplitAdjustmentValueContinuity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup test security
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := setupDailyValuesTestSecurity(pool, "SPLTST", "Split Test Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "SPLTST")

	// Date range: Mon Jan 6 through Fri Jan 17 (2 weeks)
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)
	splitDate := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC) // Monday of week 2
	splitCoefficient := 2.0                                      // 2-for-1 split
	basePrice := 200.0

	// Insert price data: $200 before split, $100 from split date onward
	if err := insertPriceDataWithSplit(pool, secID, startDate, endDate, basePrice, splitDate, splitCoefficient); err != nil {
		t.Fatalf("Failed to insert price data: %v", err)
	}

	// Insert the split event
	if err := insertSplitEvent(pool, secID, splitDate, splitCoefficient); err != nil {
		t.Fatalf("Failed to insert split event: %v", err)
	}

	// Create an active portfolio with 10 shares
	cleanupDailyValuesTestPortfolio(pool, "Split Test Portfolio", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "Split Test Portfolio", 1)

	portfolioID, err := createTestPortfolio(pool, "Split Test Portfolio", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID, PercentageOrShares: 10}, // 10 shares
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	// Create services
	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceCacheRepo := repository.NewPriceCacheRepository(pool)
	pricingSvc := services.NewPricingService(priceCacheRepo, securityRepo, avClient)
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo)

	// Fetch portfolio
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	portfolio, err := portfolioSvc.GetPortfolio(context.Background(), portfolioID)
	if err != nil {
		t.Fatalf("Failed to get portfolio: %v", err)
	}

	// Compute daily values
	dailyValues, err := performanceSvc.ComputeDailyValues(context.Background(), portfolio, startDate, endDate)
	if err != nil {
		t.Fatalf("Failed to compute daily values: %v", err)
	}

	if len(dailyValues) == 0 {
		t.Fatal("Expected daily values, got none")
	}

	// Pre-split value: 10 shares * $200 = $2000
	// Post-split value: 20 shares * $100 = $2000 (should be continuous)
	splitDateStr := splitDate.Format("2006-01-02")

	var preSplitValue, splitDayValue float64
	for _, dv := range dailyValues {
		dateStr := dv.Date.Format("2006-01-02")
		if dateStr == "2025-01-10" { // Friday before split
			preSplitValue = dv.Value
		}
		if dateStr == splitDateStr {
			splitDayValue = dv.Value
		}
	}

	if preSplitValue == 0 {
		t.Fatal("Did not find pre-split value (2025-01-10)")
	}
	if splitDayValue == 0 {
		t.Fatal("Did not find split day value")
	}

	// The portfolio value should be approximately the same before and after the split
	// Pre-split: 10 shares * $200 = $2000
	// Split day: 20 shares * $100 = $2000
	expectedValue := 2000.0
	epsilon := 0.01

	if math.Abs(preSplitValue-expectedValue) > epsilon {
		t.Errorf("Pre-split value = %.2f, expected %.2f", preSplitValue, expectedValue)
	}
	if math.Abs(splitDayValue-expectedValue) > epsilon {
		t.Errorf("Split day value = %.2f, expected %.2f (value should be continuous across split)", splitDayValue, expectedValue)
	}

	t.Logf("Split test: pre-split value = %.2f, split day value = %.2f (expected %.2f)", preSplitValue, splitDayValue, expectedValue)
}

// TestSplitAdjustmentNoSplit verifies that portfolios without splits compute correctly (no regression).
func TestSplitAdjustmentNoSplit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := setupDailyValuesTestSecurity(pool, "NOSPLIT", "No Split Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "NOSPLIT")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)

	// Insert constant price data (close = basePrice + 2 = 102 from insertPriceData)
	if err := insertPriceData(pool, secID, startDate, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data: %v", err)
	}

	cleanupDailyValuesTestPortfolio(pool, "No Split Portfolio", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "No Split Portfolio", 1)

	portfolioID, err := createTestPortfolio(pool, "No Split Portfolio", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID, PercentageOrShares: 10},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceCacheRepo := repository.NewPriceCacheRepository(pool)
	pricingSvc := services.NewPricingService(priceCacheRepo, securityRepo, avClient)
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo)

	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	portfolio, err := portfolioSvc.GetPortfolio(context.Background(), portfolioID)
	if err != nil {
		t.Fatalf("Failed to get portfolio: %v", err)
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(context.Background(), portfolio, startDate, endDate)
	if err != nil {
		t.Fatalf("Failed to compute daily values: %v", err)
	}

	// All days should have the same value: 10 shares * $102 (close) = $1020
	expectedValue := 10 * 102.0
	for _, dv := range dailyValues {
		if math.Abs(dv.Value-expectedValue) > 0.01 {
			t.Errorf("Date %s: value = %.2f, expected %.2f", dv.Date.Format("2006-01-02"), dv.Value, expectedValue)
		}
	}

	t.Logf("No-split test: %d daily values, all expected to be %.2f", len(dailyValues), expectedValue)
}

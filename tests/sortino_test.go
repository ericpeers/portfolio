package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// insertVaryingPrices inserts prices from the provided slice (cycling) so that
// daily returns are non-zero — needed for Sharpe/Sortino denominators to be defined.
func insertVaryingPrices(pool *pgxpool.Pool, securityID int64, startDate, endDate time.Time, prices []float64) error {
	ctx := context.Background()
	i := 0
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		p := prices[i%len(prices)]
		_, err := pool.Exec(ctx, `
			INSERT INTO fact_price (security_id, date, open, high, low, close, volume)
			VALUES ($1, $2, $3, $4, $5, $6, 1000000)
			ON CONFLICT (security_id, date) DO NOTHING
		`, securityID, d, p, p+1, p-1, p)
		if err != nil {
			return err
		}
		i++
	}
	futureNextUpdate := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := pool.Exec(ctx, `
		INSERT INTO fact_price_range (security_id, start_date, end_date, next_update)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (security_id) DO UPDATE SET start_date = $2, end_date = $3, next_update = $4
	`, securityID, startDate, endDate, futureNextUpdate)
	return err
}

// TestSortinoRatiosPresent verifies that Sortino ratios are returned by the compare
// endpoint alongside Sharpe ratios, and are correctly annualized.
func TestSortinoRatiosPresent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := createTestSecurity(pool, "SRTNTSTA", "Sortino Test Sec A", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create test security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "SRTNTSTA")

	secID2, err := createTestSecurity(pool, "SRTNTSTB", "Sortino Test Sec B", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create test security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "SRTNTSTB")

	// ~6 weeks of trading data gives enough daily returns for non-zero denominators.
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 2, 14, 0, 0, 0, 0, time.UTC)

	// Prices alternate up/down around a rising trend, producing both positive and
	// negative daily returns so downside deviation is defined and non-zero.
	prices := []float64{100, 103, 101, 105, 102, 107, 104, 109, 106, 111}

	if err := insertVaryingPrices(pool, secID1, startDate, endDate, prices); err != nil {
		t.Fatalf("Failed to insert price data for security 1: %v", err)
	}
	if err := insertVaryingPrices(pool, secID2, startDate, endDate, prices); err != nil {
		t.Fatalf("Failed to insert price data for security 2: %v", err)
	}
	defer func() {
		ctx := context.Background()
		pool.Exec(ctx, "DELETE FROM fact_price WHERE security_id = $1", secID1)
		pool.Exec(ctx, "DELETE FROM fact_price WHERE security_id = $1", secID2)
		pool.Exec(ctx, "DELETE FROM fact_price_range WHERE security_id = $1", secID1)
		pool.Exec(ctx, "DELETE FROM fact_price_range WHERE security_id = $1", secID2)
	}()

	cleanupTestPortfolio(pool, "Sortino Portfolio A", 1)
	cleanupTestPortfolio(pool, "Sortino Portfolio B", 1)
	defer cleanupTestPortfolio(pool, "Sortino Portfolio A", 1)
	defer cleanupTestPortfolio(pool, "Sortino Portfolio B", 1)

	portfolioAID, err := createTestPortfolio(pool, "Sortino Portfolio A", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 0.60},
		{SecurityID: secID2, PercentageOrShares: 0.40},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	portfolioBID, err := createTestPortfolio(pool, "Sortino Portfolio B", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 0.50},
		{SecurityID: secID2, PercentageOrShares: 0.50},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

	reqBody := models.CompareRequest{
		PortfolioA:  portfolioAID,
		PortfolioB:  portfolioBID,
		StartPeriod: models.FlexibleDate{Time: startDate},
		EndPeriod:   models.FlexibleDate{Time: endDate},
	}
	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	metricsA := response.PerformanceMetrics.PortfolioAMetrics

	t.Logf("Portfolio A — Sharpe: daily=%.4f yearly=%.4f | Sortino: daily=%.4f yearly=%.4f",
		metricsA.SharpeRatios.Daily, metricsA.SharpeRatios.Yearly,
		metricsA.SortinoRatios.Daily, metricsA.SortinoRatios.Yearly)

	// With mixed up/down prices and a positive overall trend, both ratios should be non-zero.
	if metricsA.SortinoRatios.Yearly == 0 && metricsA.SharpeRatios.Yearly != 0 {
		t.Error("Sortino yearly is 0 while Sharpe yearly is non-zero — expected Sortino to be defined")
	}

	// Verify annualization: monthly = daily × √20 (within 0.1% tolerance).
	if metricsA.SortinoRatios.Daily != 0 {
		expected := metricsA.SortinoRatios.Daily * math.Sqrt(20)
		if math.Abs(metricsA.SortinoRatios.Monthly-expected)/math.Abs(expected) > 0.001 {
			t.Errorf("Sortino monthly annualization wrong: got %.6f, expected %.6f",
				metricsA.SortinoRatios.Monthly, expected)
		}
	}
}

// TestSortinoRatiosNegativeReturn verifies Sortino behavior when the portfolio return
// is negative relative to the risk-free rate (mean excess return < 0).
//
// With a declining price sequence that has occasional bounces, downside deviation is
// smaller than the full standard deviation (because some excess returns are positive).
// Dividing a negative mean by a smaller denominator makes Sortino MORE negative than
// Sharpe, so Sortino yearly ≤ Sharpe yearly when mean excess return < 0.
func TestSortinoRatiosNegativeReturn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := createTestSecurity(pool, "SRTNNEGA", "Sortino Neg Test A", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create test security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "SRTNNEGA")

	secID2, err := createTestSecurity(pool, "SRTNNEGG", "Sortino Neg Test B", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create test security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "SRTNNEGG")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 2, 14, 0, 0, 0, 0, time.UTC)

	// Prices decline overall (avg ~-1.5%/day after bounces), clearly below any
	// reasonable risk-free daily rate (~0.016%). This forces mean excess return < 0.
	// The bounce days (up moves) create positive excess returns, so downside_dev < std_dev,
	// which causes Sortino to be more negative than Sharpe.
	prices := []float64{111, 108, 110, 105, 108, 102, 105, 99, 102, 96}

	if err := insertVaryingPrices(pool, secID1, startDate, endDate, prices); err != nil {
		t.Fatalf("Failed to insert price data for security 1: %v", err)
	}
	if err := insertVaryingPrices(pool, secID2, startDate, endDate, prices); err != nil {
		t.Fatalf("Failed to insert price data for security 2: %v", err)
	}
	defer func() {
		ctx := context.Background()
		pool.Exec(ctx, "DELETE FROM fact_price WHERE security_id = $1", secID1)
		pool.Exec(ctx, "DELETE FROM fact_price WHERE security_id = $1", secID2)
		pool.Exec(ctx, "DELETE FROM fact_price_range WHERE security_id = $1", secID1)
		pool.Exec(ctx, "DELETE FROM fact_price_range WHERE security_id = $1", secID2)
	}()

	cleanupTestPortfolio(pool, "Sortino Neg Portfolio A", 1)
	cleanupTestPortfolio(pool, "Sortino Neg Portfolio B", 1)
	defer cleanupTestPortfolio(pool, "Sortino Neg Portfolio A", 1)
	defer cleanupTestPortfolio(pool, "Sortino Neg Portfolio B", 1)

	portfolioAID, err := createTestPortfolio(pool, "Sortino Neg Portfolio A", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 0.60},
		{SecurityID: secID2, PercentageOrShares: 0.40},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	portfolioBID, err := createTestPortfolio(pool, "Sortino Neg Portfolio B", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 0.50},
		{SecurityID: secID2, PercentageOrShares: 0.50},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

	reqBody := models.CompareRequest{
		PortfolioA:  portfolioAID,
		PortfolioB:  portfolioBID,
		StartPeriod: models.FlexibleDate{Time: startDate},
		EndPeriod:   models.FlexibleDate{Time: endDate},
	}
	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	metricsA := response.PerformanceMetrics.PortfolioAMetrics

	t.Logf("Portfolio A — Sharpe: daily=%.4f yearly=%.4f | Sortino: daily=%.4f yearly=%.4f",
		metricsA.SharpeRatios.Daily, metricsA.SharpeRatios.Yearly,
		metricsA.SortinoRatios.Daily, metricsA.SortinoRatios.Yearly)

	// Both ratios must be negative — mean return was below the risk-free rate.
	if metricsA.SharpeRatios.Yearly >= 0 {
		t.Errorf("Expected Sharpe yearly < 0 for a declining portfolio, got %.4f", metricsA.SharpeRatios.Yearly)
	}
	if metricsA.SortinoRatios.Yearly >= 0 {
		t.Errorf("Expected Sortino yearly < 0 for a declining portfolio, got %.4f", metricsA.SortinoRatios.Yearly)
	}

	// With bounce days creating positive excess returns, downside_dev < std_dev.
	// Dividing a negative mean by a smaller denominator makes Sortino more negative.
	if metricsA.SortinoRatios.Yearly > metricsA.SharpeRatios.Yearly {
		t.Errorf("Expected Sortino yearly (%.4f) ≤ Sharpe yearly (%.4f) when mean excess return < 0",
			metricsA.SortinoRatios.Yearly, metricsA.SharpeRatios.Yearly)
	}
}

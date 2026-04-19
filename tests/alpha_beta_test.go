package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

func containsStr(s, substr string) bool { return strings.Contains(s, substr) }

// buildPerformanceSvcFromPool creates a PerformanceService backed by the test pool.
// Provider clients use dead URLs; tests must use date ranges where US10Y is cached in the DB.
func buildPerformanceSvcFromPool(t *testing.T) *services.PerformanceService {
	t.Helper()
	pool := getTestPool(t)
	secRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
	})
	return services.NewPerformanceService(pricingSvc, portfolioRepo, secRepo, 20)
}

// TestAlphaBetaEmptyBenchmarkReturnsZero verifies the guard condition: when benchmarkPrices
// is empty (benchmark unavailable), ComputeAlphaBeta returns AlphaBeta{0,0} without error.
// No DB access is needed because the function returns early before fetching any data.
func TestAlphaBetaEmptyBenchmarkReturnsZero(t *testing.T) {
	t.Parallel()

	// Nil service fields are safe here — the function exits before using them.
	svc := services.NewPerformanceService(nil, nil, nil, 0)

	dailyValues := []services.DailyValue{
		{Date: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC), Value: 100},
		{Date: time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC), Value: 105},
	}

	result, err := svc.ComputeAlphaBeta(context.Background(), dailyValues, nil,
		time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("expected no error with empty benchmark, got: %v", err)
	}
	if result.Alpha != 0 || result.Beta != 0 {
		t.Errorf("expected zero AlphaBeta for empty benchmark, got alpha=%.6f beta=%.6f", result.Alpha, result.Beta)
	}
}

// TestAlphaBetaSingleDayValueReturnsZero verifies the guard condition for dailyValues < 2.
func TestAlphaBetaSingleDayValueReturnsZero(t *testing.T) {
	t.Parallel()

	svc := services.NewPerformanceService(nil, nil, nil, 0)

	dailyValues := []services.DailyValue{
		{Date: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC), Value: 100},
	}
	benchPrices := []models.PriceData{
		{Date: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC), Close: 100},
	}

	result, err := svc.ComputeAlphaBeta(context.Background(), dailyValues, benchPrices,
		time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("expected no error with single day, got: %v", err)
	}
	if result.Alpha != 0 || result.Beta != 0 {
		t.Errorf("expected zero AlphaBeta for single-day input, got alpha=%.6f beta=%.6f", result.Alpha, result.Beta)
	}
}

// TestAlphaBetaPerfectCorrelation verifies that when portfolio and benchmark have
// identical daily returns, Beta ≈ 1.0 and Alpha ≈ 0 (after risk-free adjustment cancels).
//
// Because both excess-return sequences are identical, Cov/Var = 1 and mean(Rp) - mean(Rb) = 0.
// This holds regardless of what US10Y returns (the RF term cancels in the alpha formula).
func TestAlphaBetaPerfectCorrelation(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	svc := buildPerformanceSvcFromPool(t)

	// Use the same date range as the Sortino tests — US10Y should be cached in the DB.
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 2, 14, 0, 0, 0, 0, time.UTC)

	// Build a sequence of weekday dates with alternating prices (non-constant so returns are varied).
	priceSeq := []float64{100, 103, 101, 105, 102, 107, 104, 109, 106, 111}
	var dailyValues []services.DailyValue
	var benchPrices []models.PriceData
	i := 0
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		p := priceSeq[i%len(priceSeq)]
		dailyValues = append(dailyValues, services.DailyValue{Date: d, Value: p})
		benchPrices = append(benchPrices, models.PriceData{
			SecurityID: 0, // not used by ComputeAlphaBeta
			Date:       d,
			Close:      p, // identical to portfolio value → identical returns
		})
		i++
	}

	result, err := svc.ComputeAlphaBeta(context.Background(), dailyValues, benchPrices, startDate, endDate)
	if err != nil {
		t.Fatalf("ComputeAlphaBeta failed: %v", err)
	}

	t.Logf("Perfect correlation — Beta=%.6f Alpha=%.6f", result.Beta, result.Alpha)

	if math.Abs(result.Beta-1.0) > 0.001 {
		t.Errorf("expected Beta ≈ 1.0 for perfect correlation, got %.6f", result.Beta)
	}
	// Alpha = (meanRp_excess - beta*meanRb_excess)*252 = 0 since meanRp=meanRb and beta=1.
	// Allow small floating-point tolerance scaled by annualization factor (252).
	if math.Abs(result.Alpha) > 0.01 {
		t.Errorf("expected Alpha ≈ 0 for perfect correlation, got %.6f", result.Alpha)
	}
}

// TestAlphaBetaForwardFill verifies that a benchmark missing one trading day is
// forward-filled from the prior price, and the computation still completes without error.
func TestAlphaBetaForwardFill(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	svc := buildPerformanceSvcFromPool(t)

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)

	// Portfolio has 5 weekdays: Mon–Fri Jan 6–10.
	portfolioValues := []float64{100, 103, 101, 105, 102}
	var dailyValues []services.DailyValue
	d := startDate
	for _, v := range portfolioValues {
		dailyValues = append(dailyValues, services.DailyValue{Date: d, Value: v})
		d = d.AddDate(0, 0, 1)
	}

	// Benchmark skips Jan 8 (Wednesday) — it should be forward-filled from Jan 7.
	benchPrices := []models.PriceData{
		{Date: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC), Close: 1000},
		{Date: time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC), Close: 1030},
		// Jan 8 omitted (forward-filled as 1030)
		{Date: time.Date(2025, 1, 9, 0, 0, 0, 0, time.UTC), Close: 1010},
		{Date: time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC), Close: 1050},
	}

	result, err := svc.ComputeAlphaBeta(context.Background(), dailyValues, benchPrices, startDate, endDate)
	if err != nil {
		t.Fatalf("ComputeAlphaBeta failed with forward-fill gap: %v", err)
	}

	t.Logf("Forward-fill test — Beta=%.6f Alpha=%.6f", result.Beta, result.Alpha)

	if math.IsNaN(result.Beta) || math.IsInf(result.Beta, 0) {
		t.Errorf("Beta is NaN or Inf: %v", result.Beta)
	}
	if math.IsNaN(result.Alpha) || math.IsInf(result.Alpha, 0) {
		t.Errorf("Alpha is NaN or Inf: %v", result.Alpha)
	}
}

// TestAlphaBetaCompareEndpointIncludesBenchmarkMetrics is an end-to-end HTTP test verifying
// that the compare endpoint populates benchmark_metrics in the response. If ^GSPC and ^DJI
// are in the DB with price data for the test window, beta will be non-zero; otherwise
// W4002 warnings will be present and beta will be zero. Either outcome is valid.
func TestAlphaBetaCompareEndpointIncludesBenchmarkMetrics(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := createTestSecurity(pool, "ABETA1TST", "AlphaBeta Test Sec A", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("failed to create test security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "ABETA1TST")

	secID2, err := createTestSecurity(pool, "ABETA2TST", "AlphaBeta Test Sec B", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("failed to create test security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "ABETA2TST")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 2, 14, 0, 0, 0, 0, time.UTC)

	prices := []float64{100, 103, 101, 105, 102, 107, 104, 109, 106, 111}
	if err := insertVaryingPrices(pool, secID1, startDate, endDate, prices); err != nil {
		t.Fatalf("failed to insert prices for security 1: %v", err)
	}
	if err := insertVaryingPrices(pool, secID2, startDate, endDate, prices); err != nil {
		t.Fatalf("failed to insert prices for security 2: %v", err)
	}
	defer func() {
		ctx := context.Background()
		pool.Exec(ctx, "DELETE FROM fact_price WHERE security_id = $1", secID1)
		pool.Exec(ctx, "DELETE FROM fact_price WHERE security_id = $1", secID2)
		pool.Exec(ctx, "DELETE FROM fact_price_range WHERE security_id = $1", secID1)
		pool.Exec(ctx, "DELETE FROM fact_price_range WHERE security_id = $1", secID2)
	}()

	ptfNameA := nextPortfolioName()
	ptfNameB := nextPortfolioName()
	cleanupTestPortfolio(pool, ptfNameA, 1)
	cleanupTestPortfolio(pool, ptfNameB, 1)
	defer cleanupTestPortfolio(pool, ptfNameA, 1)
	defer cleanupTestPortfolio(pool, ptfNameB, 1)

	portfolioAID, err := createTestPortfolio(pool, ptfNameA, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 0.60},
		{SecurityID: secID2, PercentageOrShares: 0.40},
	})
	if err != nil {
		t.Fatalf("failed to create portfolio A: %v", err)
	}

	portfolioBID, err := createTestPortfolio(pool, ptfNameB, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 0.50},
		{SecurityID: secID2, PercentageOrShares: 0.50},
	})
	if err != nil {
		t.Fatalf("failed to create portfolio B: %v", err)
	}

	router := setupDailyValuesTestRouter(pool)

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
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	metricsA := response.PerformanceMetrics.PortfolioAMetrics
	bm := metricsA.BenchmarkMetrics

	t.Logf("SP500  — Alpha=%.4f Beta=%.4f", bm.SP500.Alpha, bm.SP500.Beta)
	t.Logf("DowJones — Alpha=%.4f Beta=%.4f", bm.DowJones.Alpha, bm.DowJones.Beta)

	// Check W4002 warnings per benchmark (each can be missing independently).
	gspcMissing, diaMissing := false, false
	for _, w := range response.Warnings {
		if w.Code == models.WarnBenchmarkDataUnavailable {
			t.Logf("W4002 warning: %s", w.Message)
			if containsStr(w.Message, "^GSPC") {
				gspcMissing = true
			}
			if containsStr(w.Message, "^DJI") {
				diaMissing = true
			}
		}
	}

	// When benchmark data is available: beta should be non-zero and finite.
	// When unavailable (W4002 issued for that benchmark): beta must be exactly zero.
	if gspcMissing {
		if bm.SP500.Beta != 0 || bm.SP500.Alpha != 0 {
			t.Errorf("expected zero SP500 AlphaBeta when W4002 issued for ^GSPC, got alpha=%.6f beta=%.6f", bm.SP500.Alpha, bm.SP500.Beta)
		}
	} else {
		if bm.SP500.Beta == 0 {
			t.Error("SP500 Beta is 0 but no W4002 warning for ^GSPC — expected non-zero beta when data is present")
		}
		if math.IsNaN(bm.SP500.Beta) || math.IsInf(bm.SP500.Beta, 0) {
			t.Errorf("SP500 Beta is NaN or Inf: %v", bm.SP500.Beta)
		}
		if math.IsNaN(bm.SP500.Alpha) || math.IsInf(bm.SP500.Alpha, 0) {
			t.Errorf("SP500 Alpha is NaN or Inf: %v", bm.SP500.Alpha)
		}
	}
	if diaMissing {
		if bm.DowJones.Beta != 0 || bm.DowJones.Alpha != 0 {
			t.Errorf("expected zero DowJones AlphaBeta when W4002 issued for ^DJI, got alpha=%.6f beta=%.6f", bm.DowJones.Alpha, bm.DowJones.Beta)
		}
	} else {
		if bm.DowJones.Beta == 0 {
			t.Error("DowJones Beta is 0 but no W4002 warning for ^DJI — expected non-zero beta when data is present")
		}
		if math.IsNaN(bm.DowJones.Beta) || math.IsInf(bm.DowJones.Beta, 0) {
			t.Errorf("DowJones Beta is NaN or Inf: %v", bm.DowJones.Beta)
		}
	}
}

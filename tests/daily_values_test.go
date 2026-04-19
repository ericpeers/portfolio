package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupDailyValuesTestRouter creates a router with compare endpoint for daily values tests.
// Prices are pre-seeded in tests; the provider clients are wired with dead URLs and never called.
func setupDailyValuesTestRouter(pool *pgxpool.Pool) *gin.Engine {
	gin.SetMode(gin.TestMode)

	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)

	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
	})
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc)
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo, 20)
	comparisonSvc := services.NewComparisonService(portfolioSvc, membershipSvc, performanceSvc, securityRepo)

	compareHandler := handlers.NewCompareHandler(comparisonSvc)

	router := gin.New()
	router.POST("/portfolios/compare", compareHandler.Compare)

	return router
}

// TestDailyValuesTwoIdealPortfolios tests daily values for two ideal portfolios
func TestDailyValuesTwoIdealPortfolios(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create test securities
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := createTestSecurity(pool, "DVTSTA", "Daily Values Test A", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVTSTA")

	secID2, err := createTestSecurity(pool, "DVTSTB", "Daily Values Test B", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVTSTB")

	// Insert price data - use trading days only
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC) // Monday
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)  // Friday

	if err := insertPriceData(pool, secID1, startDate, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for security 1: %v", err)
	}
	if err := insertPriceData(pool, secID2, startDate, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert price data for security 2: %v", err)
	}

	// Create two ideal portfolios
	cleanupTestPortfolio(pool, "DV Ideal Portfolio A", 1)
	cleanupTestPortfolio(pool, "DV Ideal Portfolio B", 1)
	defer cleanupTestPortfolio(pool, "DV Ideal Portfolio A", 1)
	defer cleanupTestPortfolio(pool, "DV Ideal Portfolio B", 1)

	portfolioAID, err := createTestPortfolio(pool, "DV Ideal Portfolio A", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 0.60},
		{SecurityID: secID2, PercentageOrShares: 0.40},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	portfolioBID, err := createTestPortfolio(pool, "DV Ideal Portfolio B", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 0.30},
		{SecurityID: secID2, PercentageOrShares: 0.70},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	// Create mock AV server (not used since data is cached)
	router := setupDailyValuesTestRouter(pool)

	// Make comparison request
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

	// Verify daily values are present for both portfolios
	dailyValuesA := response.PerformanceMetrics.PortfolioAMetrics.DailyValues
	dailyValuesB := response.PerformanceMetrics.PortfolioBMetrics.DailyValues

	if len(dailyValuesA) == 0 {
		t.Error("Expected daily values for portfolio A, got empty")
	}
	if len(dailyValuesB) == 0 {
		t.Error("Expected daily values for portfolio B, got empty")
	}

	// Verify start and end dates are present (both are trading days)
	startDateStr := startDate.Format("2006-01-02")
	endDateStr := endDate.Format("2006-01-02")

	hasStart := false
	hasEnd := false
	for _, dv := range dailyValuesA {
		if dv.Date == startDateStr {
			hasStart = true
		}
		if dv.Date == endDateStr {
			hasEnd = true
		}
	}

	if !hasStart {
		t.Errorf("Expected start date %s in daily values for portfolio A", startDateStr)
	}
	if !hasEnd {
		t.Errorf("Expected end date %s in daily values for portfolio A", endDateStr)
	}

	t.Logf("Two ideal portfolios: Portfolio A has %d daily values, Portfolio B has %d daily values",
		len(dailyValuesA), len(dailyValuesB))
}

// TestDailyValuesTwoActivePortfolios tests daily values for two active portfolios
func TestDailyValuesTwoActivePortfolios(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create test securities
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := createTestSecurity(pool, "DVACT1", "Daily Values Active 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVACT1")

	secID2, err := createTestSecurity(pool, "DVACT2", "Daily Values Active 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVACT2")

	// Insert price data
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC) // Monday
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)  // Friday

	if err := insertPriceData(pool, secID1, startDate, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for security 1: %v", err)
	}
	if err := insertPriceData(pool, secID2, startDate, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert price data for security 2: %v", err)
	}

	// Create two active portfolios (shares, not percentages)
	cleanupTestPortfolio(pool, "DV Active Portfolio A", 1)
	cleanupTestPortfolio(pool, "DV Active Portfolio B", 1)
	defer cleanupTestPortfolio(pool, "DV Active Portfolio A", 1)
	defer cleanupTestPortfolio(pool, "DV Active Portfolio B", 1)

	portfolioAID, err := createTestPortfolio(pool, "DV Active Portfolio A", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 10}, // 10 shares
		{SecurityID: secID2, PercentageOrShares: 20}, // 20 shares
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	portfolioBID, err := createTestPortfolio(pool, "DV Active Portfolio B", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 5},  // 5 shares
		{SecurityID: secID2, PercentageOrShares: 30}, // 30 shares
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
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
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	dailyValuesA := response.PerformanceMetrics.PortfolioAMetrics.DailyValues
	dailyValuesB := response.PerformanceMetrics.PortfolioBMetrics.DailyValues

	if len(dailyValuesA) == 0 {
		t.Error("Expected daily values for portfolio A, got empty")
	}
	if len(dailyValuesB) == 0 {
		t.Error("Expected daily values for portfolio B, got empty")
	}

	// Verify start and end dates are present
	startDateStr := startDate.Format("2006-01-02")
	endDateStr := endDate.Format("2006-01-02")

	hasStartA, hasEndA := false, false
	hasStartB, hasEndB := false, false
	for _, dv := range dailyValuesA {
		if dv.Date == startDateStr {
			hasStartA = true
		}
		if dv.Date == endDateStr {
			hasEndA = true
		}
	}
	for _, dv := range dailyValuesB {
		if dv.Date == startDateStr {
			hasStartB = true
		}
		if dv.Date == endDateStr {
			hasEndB = true
		}
	}

	if !hasStartA || !hasEndA {
		t.Errorf("Portfolio A missing start (%v) or end (%v) date", hasStartA, hasEndA)
	}
	if !hasStartB || !hasEndB {
		t.Errorf("Portfolio B missing start (%v) or end (%v) date", hasStartB, hasEndB)
	}

	t.Logf("Two active portfolios: Portfolio A has %d daily values, Portfolio B has %d daily values",
		len(dailyValuesA), len(dailyValuesB))
}

// TestDailyValuesIdealVsActive tests daily values for ideal vs active portfolio comparison
func TestDailyValuesIdealVsActive(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create test securities
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := createTestSecurity(pool, "DVMIX1", "Daily Values Mix 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVMIX1")

	secID2, err := createTestSecurity(pool, "DVMIX2", "Daily Values Mix 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVMIX2")

	// Insert price data
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)

	if err := insertPriceData(pool, secID1, startDate, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for security 1: %v", err)
	}
	if err := insertPriceData(pool, secID2, startDate, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert price data for security 2: %v", err)
	}

	// Create one ideal and one active portfolio
	cleanupTestPortfolio(pool, "DV Mix Ideal", 1)
	cleanupTestPortfolio(pool, "DV Mix Active", 1)
	defer cleanupTestPortfolio(pool, "DV Mix Ideal", 1)
	defer cleanupTestPortfolio(pool, "DV Mix Active", 1)

	portfolioIdealID, err := createTestPortfolio(pool, "DV Mix Ideal", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 0.50},
		{SecurityID: secID2, PercentageOrShares: 0.50},
	})
	if err != nil {
		t.Fatalf("Failed to create ideal portfolio: %v", err)
	}

	portfolioActiveID, err := createTestPortfolio(pool, "DV Mix Active", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 10},
		{SecurityID: secID2, PercentageOrShares: 20},
	})
	if err != nil {
		t.Fatalf("Failed to create active portfolio: %v", err)
	}

	router := setupDailyValuesTestRouter(pool)

	reqBody := models.CompareRequest{
		PortfolioA:  portfolioIdealID,
		PortfolioB:  portfolioActiveID,
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

	dailyValuesIdeal := response.PerformanceMetrics.PortfolioAMetrics.DailyValues
	dailyValuesActive := response.PerformanceMetrics.PortfolioBMetrics.DailyValues

	if len(dailyValuesIdeal) == 0 {
		t.Error("Expected daily values for ideal portfolio, got empty")
	}
	if len(dailyValuesActive) == 0 {
		t.Error("Expected daily values for active portfolio, got empty")
	}

	// Verify start and end dates
	startDateStr := startDate.Format("2006-01-02")
	endDateStr := endDate.Format("2006-01-02")

	hasStartIdeal, hasEndIdeal := false, false
	hasStartActive, hasEndActive := false, false
	for _, dv := range dailyValuesIdeal {
		if dv.Date == startDateStr {
			hasStartIdeal = true
		}
		if dv.Date == endDateStr {
			hasEndIdeal = true
		}
	}
	for _, dv := range dailyValuesActive {
		if dv.Date == startDateStr {
			hasStartActive = true
		}
		if dv.Date == endDateStr {
			hasEndActive = true
		}
	}

	if !hasStartIdeal || !hasEndIdeal {
		t.Errorf("Ideal portfolio missing start (%v) or end (%v) date", hasStartIdeal, hasEndIdeal)
	}
	if !hasStartActive || !hasEndActive {
		t.Errorf("Active portfolio missing start (%v) or end (%v) date", hasStartActive, hasEndActive)
	}

	t.Logf("Ideal vs Active: Ideal has %d daily values, Active has %d daily values",
		len(dailyValuesIdeal), len(dailyValuesActive))
}

// TestDailyValuesIPOMidPeriod tests daily values when a stock IPOs in the middle of the period
func TestDailyValuesIPOMidPeriod(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	earlyInception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	laterIPO := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC) // Wednesday

	secIDEarly, err := createTestSecurity(pool, "DVIPO1", "Daily Values Early", models.SecurityTypeStock, &earlyInception)
	if err != nil {
		t.Fatalf("Failed to setup early security: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVIPO1")

	secIDIPO, err := createTestSecurity(pool, "DVIPO2", "Daily Values Later IPO", models.SecurityTypeStock, &laterIPO)
	if err != nil {
		t.Fatalf("Failed to setup IPO security: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVIPO2")

	fullRangeStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)

	if err := insertPriceData(pool, secIDEarly, fullRangeStart, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for early security: %v", err)
	}
	if err := insertPriceData(pool, secIDIPO, laterIPO, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert price data for IPO security: %v", err)
	}

	cleanupTestPortfolio(pool, "DV IPO Portfolio A", 1)
	cleanupTestPortfolio(pool, "DV IPO Portfolio B", 1)
	defer cleanupTestPortfolio(pool, "DV IPO Portfolio A", 1)
	defer cleanupTestPortfolio(pool, "DV IPO Portfolio B", 1)

	portfolioAID, err := createTestPortfolio(pool, "DV IPO Portfolio A", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secIDEarly, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	portfolioBID, err := createTestPortfolio(pool, "DV IPO Portfolio B", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secIDEarly, PercentageOrShares: 0.50},
		{SecurityID: secIDIPO, PercentageOrShares: 0.50},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	router := setupDailyValuesTestRouter(pool)

	reqBody := models.CompareRequest{
		PortfolioA:  portfolioAID,
		PortfolioB:  portfolioBID,
		StartPeriod: models.FlexibleDate{Time: laterIPO},
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

	dailyValuesA := response.PerformanceMetrics.PortfolioAMetrics.DailyValues
	dailyValuesB := response.PerformanceMetrics.PortfolioBMetrics.DailyValues

	if len(dailyValuesA) == 0 {
		t.Error("Expected daily values for portfolio A, got empty")
	}
	if len(dailyValuesB) == 0 {
		t.Error("Expected daily values for portfolio B, got empty")
	}

	t.Logf("IPO test: Portfolio A (early only) has %d daily values, Portfolio B (with newer security) has %d daily values",
		len(dailyValuesA), len(dailyValuesB))

	ipoDateStr := laterIPO.Format("2006-01-02")
	endDateStr := endDate.Format("2006-01-02")

	hasStartA, hasEndA := false, false
	hasStartB, hasEndB := false, false

	for _, dv := range dailyValuesA {
		if dv.Date == ipoDateStr {
			hasStartA = true
		}
		if dv.Date == endDateStr {
			hasEndA = true
		}
	}
	for _, dv := range dailyValuesB {
		if dv.Date == ipoDateStr {
			hasStartB = true
		}
		if dv.Date == endDateStr {
			hasEndB = true
		}
	}

	if !hasStartA || !hasEndA {
		t.Errorf("Portfolio A missing start (%v) or end (%v) date", hasStartA, hasEndA)
	}
	if !hasStartB || !hasEndB {
		t.Errorf("Portfolio B missing start (%v) or end (%v) date", hasStartB, hasEndB)
	}

	for _, dv := range dailyValuesB {
		dvDate, _ := time.Parse("2006-01-02", dv.Date)
		if dvDate.Before(laterIPO) {
			t.Errorf("Portfolio B should not have daily values before IPO date %s, but found %s", ipoDateStr, dv.Date)
		}
	}

	if len(dailyValuesA) != len(dailyValuesB) {
		t.Logf("Note: Portfolio A has %d values, Portfolio B has %d values (may differ if price availability varies)",
			len(dailyValuesA), len(dailyValuesB))
	}
}

// TestDailyValuesStartEndTradingDays specifically tests that start and end dates are included
func TestDailyValuesStartEndTradingDays(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := createTestSecurity(pool, "DVTDAY", "Daily Values Trading Day", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVTDAY")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)

	if err := insertPriceData(pool, secID, startDate, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data: %v", err)
	}

	cleanupTestPortfolio(pool, "DV Trading Day Test", 1)
	defer cleanupTestPortfolio(pool, "DV Trading Day Test", 1)

	portfolioID, err := createTestPortfolio(pool, "DV Trading Day Test", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	router := setupDailyValuesTestRouter(pool)

	reqBody := models.CompareRequest{
		PortfolioA:  portfolioID,
		PortfolioB:  portfolioID,
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

	dailyValues := response.PerformanceMetrics.PortfolioAMetrics.DailyValues

	if len(dailyValues) == 0 {
		t.Fatal("Expected daily values, got empty")
	}

	startDateStr := startDate.Format("2006-01-02")
	endDateStr := endDate.Format("2006-01-02")

	hasStart := false
	hasEnd := false
	for _, dv := range dailyValues {
		if dv.Date == startDateStr {
			hasStart = true
		}
		if dv.Date == endDateStr {
			hasEnd = true
		}
	}

	if !hasStart {
		t.Errorf("Start date %s (Monday, trading day) should be in daily values", startDateStr)
	}
	if !hasEnd {
		t.Errorf("End date %s (Friday, trading day) should be in daily values", endDateStr)
	}

	for i := 1; i < len(dailyValues); i++ {
		prev, _ := time.Parse("2006-01-02", dailyValues[i-1].Date)
		curr, _ := time.Parse("2006-01-02", dailyValues[i].Date)
		if !prev.Before(curr) {
			t.Errorf("Daily values not sorted: %s should come before %s", dailyValues[i-1].Date, dailyValues[i].Date)
		}
	}

	for _, dv := range dailyValues {
		d, _ := time.Parse("2006-01-02", dv.Date)
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			t.Errorf("Found weekend date in daily values: %s (%s)", dv.Date, d.Weekday())
		}
	}

	t.Logf("Trading days test: Found %d daily values from %s to %s", len(dailyValues), startDateStr, endDateStr)
}

// TestDailyValuesForwardFillMissingData verifies that a day where one security has no
// price data is still included in results using the previous close (forward-fill),
// rather than being dropped entirely.
func TestDailyValuesForwardFillMissingData(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Week of 2025-02-03 (Mon) – 2025-02-07 (Fri), 5 clean trading days (no holidays).
	// Avoid the Jan 6-10 window: Jan 9 is a NYSE ad-hoc closure (Carter mourning day).
	// DVFFD2 will have no data on Wednesday the 5th, simulating a thinly-traded ADR.
	startDate := time.Date(2025, 2, 3, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 2, 7, 0, 0, 0, 0, time.UTC)
	gapDate := time.Date(2025, 2, 5, 0, 0, 0, 0, time.UTC) // Wednesday — no trade

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := createTestSecurity(pool, "DVFFD1", "Forward Fill Test 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVFFD1")

	secID2, err := createTestSecurity(pool, "DVFFD2", "Forward Fill Test 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "DVFFD2")

	if err := insertPriceData(pool, secID1, startDate, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for security 1: %v", err)
	}
	if err := insertPriceData(pool, secID2, startDate, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert price data for security 2: %v", err)
	}
	// Remove Wednesday's price for secID2 to simulate a no-trade day.
	if _, err := pool.Exec(ctx, `DELETE FROM fact_price WHERE security_id = $1 AND date = $2`, secID2, gapDate); err != nil {
		t.Fatalf("Failed to delete gap-day price: %v", err)
	}

	//clean up any preexisting data, and schedule cleanup for test completion
	cleanupTestPortfolio(pool, "DV Forward Fill A", 1)
	cleanupTestPortfolio(pool, "DV Forward Fill B", 1)
	defer cleanupTestPortfolio(pool, "DV Forward Fill A", 1)
	defer cleanupTestPortfolio(pool, "DV Forward Fill B", 1)

	// 10 shares of sec1 (@100) + 20 shares of sec2 (@50) = $2000/day normally.
	// On the gap day, sec2 forward-fills at $50 → still $2000 (constant price in test data).
	portAID, err := createTestPortfolio(pool, "DV Forward Fill A", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 10},
		{SecurityID: secID2, PercentageOrShares: 20},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	portBID, err := createTestPortfolio(pool, "DV Forward Fill B", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 5},
		{SecurityID: secID2, PercentageOrShares: 10},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	router := setupDailyValuesTestRouter(pool)

	reqBody := models.CompareRequest{
		PortfolioA:  portAID,
		PortfolioB:  portBID,
		StartPeriod: models.FlexibleDate{Time: startDate},
		EndPeriod:   models.FlexibleDate{Time: endDate},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	dailyValues := response.PerformanceMetrics.PortfolioAMetrics.DailyValues

	// Expect all 5 trading days — gap day must be forward-filled, not dropped.
	if len(dailyValues) != 5 {
		t.Errorf("Expected 5 daily values (gap day forward-filled), got %d: %v", len(dailyValues), dailyValues)
	}

	gapDateStr := gapDate.Format("2006-01-02")
	foundGap := false
	for _, dv := range dailyValues {
		if dv.Date == gapDateStr {
			foundGap = true
			// sec2 forward-fills at 50.0; value = 10*102 + 20*52 = 1020 + 1040 = 2060
			// (insertPriceData sets close = basePrice + 2, so 102 and 52)
			expected := 10.0*102.0 + 20.0*52.0
			if dv.Value != expected {
				t.Errorf("Gap day value: expected %.2f (forward-fill), got %.2f", expected, dv.Value)
			}
		}
	}
	if !foundGap {
		t.Errorf("Gap day %s was dropped instead of forward-filled", gapDateStr)
	}

	t.Logf("Forward-fill test: %d daily values, gap day %s present=%v", len(dailyValues), gapDateStr, foundGap)
}

// TestReverseSplitInSnapshotWindowComputesDailyValuesCorrectly verifies that a reverse
// split (coefficient < 1.0) occurring between startDate and snapshotted_at is handled
// correctly by ComputeDailyValues.
//
// Timeline:
//
//	Jan 6  — startDate / created_at
//	Jan 13 — 1-for-2 reverse split on TSRVSP1 (price $50→$100; shares 20→10)
//	Jan 17 — snapshotted_at; shares recorded as 10 (post-reverse-split count)
//	Jan 24 — endDate
//
// The fix: ComputeDailyValues must divide by the cumulative split coefficient
// when reversing splits in [startDate, snapshotted_at). For reverse splits the
// coefficient is < 1.0, so the guard must be `!= 0 && != 1.0` — not `> 1.0`.
//
// Without fix (guard `> 1.0`):
//
//	Reversal skipped; forward loop halves shares again on Jan 13.
//	Jan 10: 10 × $50 = $500 (wrong — one extra halving)
//	Jan 13: 5 × $100 = $500 (wrong — double-applied)
//
// With fix:
//
//	Reversal: 10 / 0.5 = 20 pre-split shares.
//	Jan 10: 20 × $50 = $1000 (correct)
//	Jan 13: forward loop ×0.5 → 10 shares × $100 = $1000 (continuous)
func TestReverseSplitInSnapshotWindowComputesDailyValuesCorrectly(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := createTestSecurity(pool, "TSRVSP1", "Test Reverse Split 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create TSRVSP1: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSRVSP1")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 24, 0, 0, 0, 0, time.UTC)
	splitDate := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)
	splitCoeff := 0.5 // 1-for-2 reverse split: 20 shares → 10, price $50 → $100

	// Prices: $50 before split, $100 on/after split (basePrice / coeff = 50 / 0.5 = 100).
	if err := insertPriceDataWithSplit(pool, secID, startDate, endDate, 50.0, splitDate, splitCoeff); err != nil {
		t.Fatalf("insert prices: %v", err)
	}
	if err := insertSplitEvent(pool, secID, splitDate, splitCoeff); err != nil {
		t.Fatalf("insert split event: %v", err)
	}

	// Portfolio records 10 shares — the post-reverse-split count as of snapshotted_at.
	cleanupTestPortfolio(pool, "Reverse Split Test Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Reverse Split Test Portfolio", 1)

	portfolioID, err := createTestPortfolio(pool, "Reverse Split Test Portfolio", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID, PercentageOrShares: 10}, // 10 post-reverse-split shares
	})
	if err != nil {
		t.Fatalf("create portfolio: %v", err)
	}

	// snapshotted_at = Jan 17: the reverse split on Jan 13 falls between startDate and here.
	snapDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)
	_, err = pool.Exec(ctx, `UPDATE portfolio SET snapshotted_at = $1 WHERE id = $2`, snapDate, portfolioID)
	if err != nil {
		t.Fatalf("set snapshotted_at: %v", err)
	}

	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
	})
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo, 20)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)

	portfolio, err := portfolioSvc.GetPortfolio(ctx, portfolioID)
	if err != nil {
		t.Fatalf("GetPortfolio: %v", err)
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(ctx, portfolio, startDate, endDate, nil)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}

	findValue := func(date string) float64 {
		for _, dv := range dailyValues {
			if dv.Date.Format("2006-01-02") == date {
				return dv.Value
			}
		}
		return -1
	}

	preSplit := findValue("2025-01-10") // Friday before the reverse split
	splitDay := findValue("2025-01-13") // The reverse split day

	if preSplit < 0 {
		t.Fatal("no value found for 2025-01-10")
	}
	if splitDay < 0 {
		t.Fatal("no value found for 2025-01-13")
	}

	// Pre-split: reversal gives 10 / 0.5 = 20 shares; 20 × $50 = $1000.
	// Split day: forward loop ×0.5 → 10 shares × $100 = $1000 (continuous).
	const want = 1000.0
	const epsilon = 0.01

	if math.Abs(preSplit-want) > epsilon {
		t.Errorf("Jan 10 (pre-reverse-split) = %.2f, want %.2f\n"+
			"  (if got 500.00, the reverse split was not reversed — guard was `> 1.0`)", preSplit, want)
	}
	if math.Abs(splitDay-want) > epsilon {
		t.Errorf("Jan 13 (reverse-split day) = %.2f, want %.2f", splitDay, want)
	}

	t.Logf("Reverse split test: Jan 10=%.2f  Jan 13=%.2f  (both want %.2f)", preSplit, splitDay, want)
}

// TestDailyValuesMixedCacheState verifies the bulk cache-classification path in
// ComputeDailyValues: warm securities (with a valid fact_price_range) must be served
// entirely from Postgres without hitting the provider, while the cold security (no
// fact_price_range row) triggers exactly one provider fetch that populates the cache.
func TestDailyValuesMixedCacheState(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)  // Monday
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)   // Friday (5 trading days)
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Two warm securities: prices and a fact_price_range entry with a far-future
	// next_update so DetermineFetch treats them as fully cached.
	warmTicker1 := nextTicker()
	warmTicker2 := nextTicker()
	coldTicker := nextTicker()

	secWarm1, err := createTestSecurity(pool, warmTicker1, "Mixed Cache Warm 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create warm1: %v", err)
	}
	defer cleanupTestSecurity(pool, warmTicker1)

	secWarm2, err := createTestSecurity(pool, warmTicker2, "Mixed Cache Warm 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create warm2: %v", err)
	}
	defer cleanupTestSecurity(pool, warmTicker2)

	// Cold security: no fact_price or fact_price_range — must be fetched from provider.
	secCold, err := createTestSecurity(pool, coldTicker, "Mixed Cache Cold", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create cold: %v", err)
	}
	defer cleanupTestSecurity(pool, coldTicker)

	if err := insertPriceData(pool, secWarm1, startDate, endDate, 100.0); err != nil {
		t.Fatalf("insert warm1 prices: %v", err)
	}
	if err := insertPriceData(pool, secWarm2, startDate, endDate, 50.0); err != nil {
		t.Fatalf("insert warm2 prices: %v", err)
	}

	portfolioName := nextPortfolioName()
	defer cleanupTestPortfolio(pool, portfolioName, 1)

	portfolioID, err := createTestPortfolio(pool, portfolioName, 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secWarm1, PercentageOrShares: 10},
		{SecurityID: secWarm2, PercentageOrShares: 5},
		{SecurityID: secCold, PercentageOrShares: 8},
	})
	if err != nil {
		t.Fatalf("create portfolio: %v", err)
	}

	// Mock server returns prices for the cold security and counts provider calls.
	// ComputeDailyValues (without Sharpe) never fetches US10Y, so every call here
	// must be for the cold security. Warm securities must not reach the provider.
	var providerCalls int32
	mockServer := createMockEODHDPriceServer(generatePriceData(startDate, endDate), &providerCalls)
	defer mockServer.Close()

	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", mockServer.URL),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
	})
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo, 20)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)

	portfolio, err := portfolioSvc.GetPortfolio(ctx, portfolioID)
	if err != nil {
		t.Fatalf("GetPortfolio: %v", err)
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(ctx, portfolio, startDate, endDate, nil)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}

	// Daily values should be present for all securities including the cold one.
	if len(dailyValues) == 0 {
		t.Fatalf("Expected daily values, got none — cold security may not have been fetched")
	}
	for _, dv := range dailyValues {
		if dv.Value <= 0 {
			t.Errorf("Daily value on %s is non-positive: %.2f", dv.Date.Format("2006-01-02"), dv.Value)
		}
	}

	// Exactly one provider call: the cold security fetch.
	// Warm securities are served from the Postgres cache without touching the provider.
	calls := atomic.LoadInt32(&providerCalls)
	if calls != 1 {
		t.Errorf("Expected exactly 1 provider call (cold security), got %d", calls)
	}

	// fact_price_range must now exist for the cold security (EnsurePricesCached wrote it).
	var rangeCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_price_range WHERE security_id = $1`, secCold,
	).Scan(&rangeCount); err != nil {
		t.Fatalf("query fact_price_range: %v", err)
	}
	if rangeCount == 0 {
		t.Error("Expected fact_price_range entry for cold security after fetch, got none")
	}

	// fact_price must have rows for the cold security (prices were stored by fetchAndStore).
	var priceCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_price WHERE security_id = $1 AND date >= $2 AND date <= $3`,
		secCold, startDate, endDate,
	).Scan(&priceCount); err != nil {
		t.Fatalf("query fact_price for cold security: %v", err)
	}
	if priceCount == 0 {
		t.Error("Expected prices in fact_price for cold security after fetch, got none")
	}

	t.Logf("Mixed cache test: %d daily values, %d provider call(s), cold security cached with %d price rows",
		len(dailyValues), calls, priceCount)
}

// TestDailyValuesPreSeedLastKnownPrice verifies that ComputeDailyValues includes the
// portfolio start date even when one security has no price on that exact date, by
// pre-seeding lastKnownPrice from the most recent price before startDate.
//
// Timeline:
//
//	Jan 27 – Jan 31: both securities have prices (pre-window)
//	Feb 3 (Mon, startDate): sec1 has price, sec2 does NOT (deleted after insert)
//	Feb 4 – Feb 7: both securities have prices
//
// Without the pre-seed: Feb 3 is dropped as "hard missing" because sec2 has no price
// and no prior lastKnownPrice when the loop starts.
// With the pre-seed: sec2 is seeded with its Jan 31 close (52.0); Feb 3 is forward-filled.
func TestDailyValuesPreSeedLastKnownPrice(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	startDate := time.Date(2025, 2, 3, 0, 0, 0, 0, time.UTC) // Monday
	endDate := time.Date(2025, 2, 7, 0, 0, 0, 0, time.UTC)   // Friday
	preWindow := time.Date(2025, 1, 27, 0, 0, 0, 0, time.UTC) // one week before startDate

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := createTestSecurity(pool, "TSTPSA1TST", "Pre-Seed Test 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTPSA1TST")

	secID2, err := createTestSecurity(pool, "TSTPSA2TST", "Pre-Seed Test 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTPSA2TST")

	// sec1: full coverage Jan 27 – Feb 7.
	if err := insertPriceData(pool, secID1, preWindow, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for security 1: %v", err)
	}
	// sec2: insert Jan 27 – Feb 7, then remove the Feb 3 row to simulate a security
	// that has prior history but skips the exact portfolio start date.
	if err := insertPriceData(pool, secID2, preWindow, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert price data for security 2: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM fact_price WHERE security_id = $1 AND date = $2`, secID2, startDate); err != nil {
		t.Fatalf("Failed to delete start-date price for security 2: %v", err)
	}

	cleanupTestPortfolio(pool, "DV PreSeed A", 1)
	cleanupTestPortfolio(pool, "DV PreSeed B", 1)
	defer cleanupTestPortfolio(pool, "DV PreSeed A", 1)
	defer cleanupTestPortfolio(pool, "DV PreSeed B", 1)

	portAID, err := createTestPortfolio(pool, "DV PreSeed A", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 10},
		{SecurityID: secID2, PercentageOrShares: 20},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}
	// portB holds only sec1 — no gap — used to satisfy the compare endpoint.
	portBID, err := createTestPortfolio(pool, "DV PreSeed B", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 5},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	router := setupDailyValuesTestRouter(pool)

	reqBody := models.CompareRequest{
		PortfolioA:  portAID,
		PortfolioB:  portBID,
		StartPeriod: models.FlexibleDate{Time: startDate},
		EndPeriod:   models.FlexibleDate{Time: endDate},
	}
	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	dailyValues := response.PerformanceMetrics.PortfolioAMetrics.DailyValues

	// All 5 trading days must be present — startDate must not be dropped as hard-missing.
	if len(dailyValues) != 5 {
		t.Errorf("Expected 5 daily values (startDate forward-filled via pre-seed), got %d: %v", len(dailyValues), dailyValues)
	}

	startDateStr := startDate.Format("2006-01-02")
	foundStart := false
	for _, dv := range dailyValues {
		if dv.Date == startDateStr {
			foundStart = true
			// sec2 has no price on Feb 3, forward-fills from Jan 31 close (basePrice+2 = 52.0)
			// sec1 has its real Feb 3 close (basePrice+2 = 102.0)
			expected := 10.0*102.0 + 20.0*52.0 // 1020 + 1040 = 2060
			if dv.Value != expected {
				t.Errorf("Start date value: expected %.2f (sec2 pre-seeded from Jan 31), got %.2f", expected, dv.Value)
			}
		}
	}
	if !foundStart {
		t.Errorf("startDate %s was dropped instead of forward-filled via pre-seed lastKnownPrice", startDateStr)
	}

	t.Logf("Pre-seed test: %d daily values, startDate %s present=%v", len(dailyValues), startDateStr, foundStart)
}

// TestDailyValuesPreSeedGapSplit verifies that a split occurring between the pre-seed date
// and startDate is correctly applied to the seeded price. splitsBySecID only covers
// [startDate, endDate], so gap splits must be fetched and applied separately.
//
// Timeline:
//
//	Jan 27 – Jan 31: both securities have prices (pre-window)
//	Feb 3 (Mon): sec2 has its last pre-gap price (close = 102.0)
//	Feb 4 (Tue): 2-for-1 split on sec2; no price row (gap day)
//	Feb 5 (Wed, startDate): sec2 still has no price — triggers pre-seed
//	Feb 6 – Feb 7: sec2 resumes at post-split price (close = 52.0)
//
// Without the gap-split fix: sec2 is seeded at 102.0 (pre-split) → wrong forward-fill.
// With the fix: seeded price is 102.0 / 2.0 = 51.0 (post-split) → correct.
func TestDailyValuesPreSeedGapSplit(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	startDate := time.Date(2025, 2, 5, 0, 0, 0, 0, time.UTC)  // Wednesday
	endDate := time.Date(2025, 2, 7, 0, 0, 0, 0, time.UTC)    // Friday
	preWindow := time.Date(2025, 1, 27, 0, 0, 0, 0, time.UTC) // Monday, one week back
	splitDate := time.Date(2025, 2, 4, 0, 0, 0, 0, time.UTC)  // Tuesday — in the gap

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := createTestSecurity(pool, "TSTPSGSA1", "Pre-Seed Gap Split 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTPSGSA1")

	secID2, err := createTestSecurity(pool, "TSTPSGSA2", "Pre-Seed Gap Split 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTPSGSA2")

	// sec1: full coverage Jan 27 – Feb 7, basePrice 100 (close = 102).
	if err := insertPriceData(pool, secID1, preWindow, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for security 1: %v", err)
	}

	// sec2 pre-split: Jan 27 – Feb 3 only. close = 102.0.
	preSplitEnd := time.Date(2025, 2, 3, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(pool, secID2, preWindow, preSplitEnd, 100.0); err != nil {
		t.Fatalf("Failed to insert pre-split price data for security 2: %v", err)
	}
	// sec2 post-split: Feb 6 – Feb 7 only, basePrice 50 (close = 52 ≈ 102/2).
	// No price on Feb 4 (split day) or Feb 5 (startDate) — simulates a no-trade gap.
	postSplitStart := time.Date(2025, 2, 6, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(pool, secID2, postSplitStart, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert post-split price data for security 2: %v", err)
	}
	// Update price range to show the cache covers the full requested window.
	if _, err := pool.Exec(ctx,
		`UPDATE fact_price_range SET start_date = $2, end_date = $3 WHERE security_id = $1`,
		secID2, preWindow, endDate,
	); err != nil {
		t.Fatalf("Failed to update price range for security 2: %v", err)
	}
	// 2-for-1 split on Feb 4 (in the gap, before startDate).
	if err := insertSplitEvent(pool, secID2, splitDate, 2.0); err != nil {
		t.Fatalf("Failed to insert gap split for security 2: %v", err)
	}

	cleanupTestPortfolio(pool, "DV PreSeedGapSplit A", 1)
	cleanupTestPortfolio(pool, "DV PreSeedGapSplit B", 1)
	defer cleanupTestPortfolio(pool, "DV PreSeedGapSplit A", 1)
	defer cleanupTestPortfolio(pool, "DV PreSeedGapSplit B", 1)

	// 10 shares of each. After the 2-for-1 split the investor holds 10 shares at post-split
	// price. The portfolio records the post-split share count (10 shares @ ~51).
	portAID, err := createTestPortfolio(pool, "DV PreSeedGapSplit A", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 10},
		{SecurityID: secID2, PercentageOrShares: 10},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}
	portBID, err := createTestPortfolio(pool, "DV PreSeedGapSplit B", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 5},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	router := setupDailyValuesTestRouter(pool)

	reqBody := models.CompareRequest{
		PortfolioA:  portAID,
		PortfolioB:  portBID,
		StartPeriod: models.FlexibleDate{Time: startDate},
		EndPeriod:   models.FlexibleDate{Time: endDate},
	}
	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	dailyValues := response.PerformanceMetrics.PortfolioAMetrics.DailyValues

	// startDate must be present (not dropped as hard-missing).
	startDateStr := startDate.Format("2006-01-02")
	foundStart := false
	for _, dv := range dailyValues {
		if dv.Date == startDateStr {
			foundStart = true
			// sec2 pre-seeded from Feb 3 (close = 102.0), gap split ÷2 → 51.0.
			// sec1 real price on Feb 5 (close = 102.0).
			expected := 10.0*102.0 + 10.0*51.0 // 1020 + 510 = 1530
			if dv.Value != expected {
				t.Errorf("startDate value: expected %.2f (sec2 pre-seeded post-split), got %.2f", expected, dv.Value)
			}
		}
	}
	if !foundStart {
		t.Errorf("startDate %s was dropped instead of forward-filled via pre-seed", startDateStr)
	}

	t.Logf("Pre-seed gap-split test: %d daily values, startDate %s present=%v", len(dailyValues), startDateStr, foundStart)
}

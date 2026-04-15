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

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupDailyValuesTestRouter creates a router with compare endpoint for daily values tests
func setupDailyValuesTestRouter(pool *pgxpool.Pool, avClient *alphavantage.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)

	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)

	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{Price: avClient, Treasury: avClient})
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
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
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

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

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

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

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

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

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

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

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

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

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{Price: avClient, Treasury: avClient})
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

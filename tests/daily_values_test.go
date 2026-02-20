package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
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

	pricingSvc := services.NewPricingService(priceRepo, securityRepo, avClient)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo)
	comparisonSvc := services.NewComparisonService(portfolioSvc, membershipSvc, performanceSvc)

	compareHandler := handlers.NewCompareHandler(comparisonSvc)

	router := gin.New()
	router.POST("/portfolios/compare", compareHandler.Compare)

	return router
}

// TestDailyValuesTwoIdealPortfolios tests daily values for two ideal portfolios
func TestDailyValuesTwoIdealPortfolios(t *testing.T) {
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
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)  // Monday
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)   // Friday

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
	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

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
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)  // Monday
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)   // Friday

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

	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

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

	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

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

	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

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

	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

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

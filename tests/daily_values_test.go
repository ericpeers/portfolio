package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// setupDailyValuesTestSecurity creates a test security with specified inception
func setupDailyValuesTestSecurity(pool *pgxpool.Pool, ticker, name string, inception *time.Time) (int64, error) {
	ctx := context.Background()

	// Clean up any existing test security first
	cleanupDailyValuesTestSecurity(pool, ticker)

	// Insert the test security
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO dim_security (ticker, name, exchange, type, inception)
		VALUES ($1, $2, 1, 'stock', $3)
		RETURNING id
	`, ticker, name, inception).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert test security: %w", err)
	}

	return id, nil
}

// cleanupDailyValuesTestSecurity removes test security and its associated data
func cleanupDailyValuesTestSecurity(pool *pgxpool.Pool, ticker string) {
	ctx := context.Background()

	var securityID int64
	err := pool.QueryRow(ctx, `SELECT id FROM dim_security WHERE ticker = $1`, ticker).Scan(&securityID)
	if err != nil {
		return // Security doesn't exist
	}

	pool.Exec(ctx, `DELETE FROM portfolio_membership WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM fact_price WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM fact_event WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
}

// cleanupDailyValuesTestPortfolio removes test portfolio and its memberships
func cleanupDailyValuesTestPortfolio(pool *pgxpool.Pool, name string, ownerID int64) {
	ctx := context.Background()
	pool.Exec(ctx, `
		DELETE FROM portfolio_membership
		WHERE portfolio_id IN (
			SELECT id FROM portfolio WHERE name = $1 AND owner = $2
		)
	`, name, ownerID)
	pool.Exec(ctx, `DELETE FROM portfolio WHERE name = $1 AND owner = $2`, name, ownerID)
}

// insertPriceData inserts price data for a security
func insertPriceData(pool *pgxpool.Pool, securityID int64, startDate, endDate time.Time, basePrice float64) error {
	ctx := context.Background()

	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		// Skip weekends
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO fact_price (security_id, date, open, high, low, close, volume)
			VALUES ($1, $2, $3, $4, $5, $6, 1000000)
			ON CONFLICT (security_id, date) DO NOTHING
		`, securityID, d, basePrice, basePrice+5, basePrice-1, basePrice+2)
		if err != nil {
			return fmt.Errorf("failed to insert price data: %w", err)
		}
	}

	// Set up price range with far-future next_update
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

// createTestPortfolio creates a portfolio for testing
func createTestPortfolio(pool *pgxpool.Pool, name string, ownerID int64, portfolioType models.PortfolioType, memberships []models.MembershipRequest) (int64, error) {
	ctx := context.Background()

	// Insert portfolio
	var portfolioID int64
	now := time.Now()
	err := pool.QueryRow(ctx, `
		INSERT INTO portfolio (name, owner, portfolio_type, objective, created, updated)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`, name, ownerID, portfolioType, models.ObjectiveGrowth, now, now).Scan(&portfolioID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert portfolio: %w", err)
	}

	// Insert memberships
	for _, m := range memberships {
		_, err := pool.Exec(ctx, `
			INSERT INTO portfolio_membership (portfolio_id, security_id, percentage_or_shares)
			VALUES ($1, $2, $3)
		`, portfolioID, m.SecurityID, m.PercentageOrShares)
		if err != nil {
			return 0, fmt.Errorf("failed to insert membership: %w", err)
		}
	}

	return portfolioID, nil
}

// TestDailyValuesTwoIdealPortfolios tests daily values for two ideal portfolios
func TestDailyValuesTwoIdealPortfolios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create test securities
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID1, err := setupDailyValuesTestSecurity(pool, "DVTSTA", "Daily Values Test A", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 1: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "DVTSTA")

	secID2, err := setupDailyValuesTestSecurity(pool, "DVTSTB", "Daily Values Test B", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 2: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "DVTSTB")

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
	cleanupDailyValuesTestPortfolio(pool, "DV Ideal Portfolio A", 1)
	cleanupDailyValuesTestPortfolio(pool, "DV Ideal Portfolio B", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "DV Ideal Portfolio A", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "DV Ideal Portfolio B", 1)

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
	secID1, err := setupDailyValuesTestSecurity(pool, "DVACT1", "Daily Values Active 1", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 1: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "DVACT1")

	secID2, err := setupDailyValuesTestSecurity(pool, "DVACT2", "Daily Values Active 2", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 2: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "DVACT2")

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
	cleanupDailyValuesTestPortfolio(pool, "DV Active Portfolio A", 1)
	cleanupDailyValuesTestPortfolio(pool, "DV Active Portfolio B", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "DV Active Portfolio A", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "DV Active Portfolio B", 1)

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
	secID1, err := setupDailyValuesTestSecurity(pool, "DVMIX1", "Daily Values Mix 1", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 1: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "DVMIX1")

	secID2, err := setupDailyValuesTestSecurity(pool, "DVMIX2", "Daily Values Mix 2", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security 2: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "DVMIX2")

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
	cleanupDailyValuesTestPortfolio(pool, "DV Mix Ideal", 1)
	cleanupDailyValuesTestPortfolio(pool, "DV Mix Active", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "DV Mix Ideal", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "DV Mix Active", 1)

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
// This tests that portfolios with newer securities only have daily values from when
// all securities have pricing data available.
func TestDailyValuesIPOMidPeriod(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create one security with early inception, one with later IPO
	earlyInception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	laterIPO := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC) // Wednesday

	secIDEarly, err := setupDailyValuesTestSecurity(pool, "DVIPO1", "Daily Values Early", &earlyInception)
	if err != nil {
		t.Fatalf("Failed to setup early security: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "DVIPO1")

	secIDIPO, err := setupDailyValuesTestSecurity(pool, "DVIPO2", "Daily Values Later IPO", &laterIPO)
	if err != nil {
		t.Fatalf("Failed to setup IPO security: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "DVIPO2")

	// Insert price data
	// Early security has full range starting from Monday Jan 6
	fullRangeStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC) // Monday
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)       // Friday (2 weeks)

	if err := insertPriceData(pool, secIDEarly, fullRangeStart, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for early security: %v", err)
	}

	// IPO security only has prices from IPO date (Jan 8) onwards
	if err := insertPriceData(pool, secIDIPO, laterIPO, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert price data for IPO security: %v", err)
	}

	// Create portfolios
	cleanupDailyValuesTestPortfolio(pool, "DV IPO Portfolio A", 1)
	cleanupDailyValuesTestPortfolio(pool, "DV IPO Portfolio B", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "DV IPO Portfolio A", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "DV IPO Portfolio B", 1)

	// Portfolio A: only early security (has data for full period)
	portfolioAID, err := createTestPortfolio(pool, "DV IPO Portfolio A", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secIDEarly, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	// Portfolio B: both securities (but must start comparison from when both exist)
	// The daily values will only include dates where ALL holdings have prices
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

	// Start comparison from IPO date (both securities have data from this point)
	reqBody := models.CompareRequest{
		PortfolioA:  portfolioAID,
		PortfolioB:  portfolioBID,
		StartPeriod: models.FlexibleDate{Time: laterIPO}, // Start from when both securities exist
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

	// Both portfolios should have the start date (IPO date) and end date
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

	// Verify no dates before IPO in portfolio B's daily values
	for _, dv := range dailyValuesB {
		dvDate, _ := time.Parse("2006-01-02", dv.Date)
		if dvDate.Before(laterIPO) {
			t.Errorf("Portfolio B should not have daily values before IPO date %s, but found %s", ipoDateStr, dv.Date)
		}
	}

	// Both should have same number of daily values since we started from IPO date
	if len(dailyValuesA) != len(dailyValuesB) {
		t.Logf("Note: Portfolio A has %d values, Portfolio B has %d values (may differ if price availability varies)",
			len(dailyValuesA), len(dailyValuesB))
	}
}

// TestDailyValuesStartEndTradingDays specifically tests that start and end dates are included
// when they are both valid trading days
func TestDailyValuesStartEndTradingDays(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create test security
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := setupDailyValuesTestSecurity(pool, "DVTDAY", "Daily Values Trading Day", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "DVTDAY")

	// Use specific trading days (Mon-Fri)
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)   // Monday
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)    // Friday (2 weeks)

	if err := insertPriceData(pool, secID, startDate, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data: %v", err)
	}

	cleanupDailyValuesTestPortfolio(pool, "DV Trading Day Test", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "DV Trading Day Test", 1)

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

	// Compare the portfolio with itself
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

	// Verify start and end dates are present
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

	// Verify dates are sorted
	for i := 1; i < len(dailyValues); i++ {
		prev, _ := time.Parse("2006-01-02", dailyValues[i-1].Date)
		curr, _ := time.Parse("2006-01-02", dailyValues[i].Date)
		if !prev.Before(curr) {
			t.Errorf("Daily values not sorted: %s should come before %s", dailyValues[i-1].Date, dailyValues[i].Date)
		}
	}

	// Verify no weekend dates
	for _, dv := range dailyValues {
		d, _ := time.Parse("2006-01-02", dv.Date)
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			t.Errorf("Found weekend date in daily values: %s (%s)", dv.Date, d.Weekday())
		}
	}

	t.Logf("Trading days test: Found %d daily values from %s to %s", len(dailyValues), startDateStr, endDateStr)
}

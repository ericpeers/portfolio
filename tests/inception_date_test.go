package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
)

// TestCompareAdjustsStartDateToInception tests that the comparison endpoint
// adjusts the start date to the latest inception date when the requested
// start date precedes a security's IPO, and returns a W4001 warning.
func TestCompareAdjustsStartDateToInception(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Security A: old inception, has price data for the full range
	earlyInception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	// Security B: newer IPO on Wed Jan 8 2025
	laterIPO := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC)

	secIDEarly, err := createTestSecurity(pool, "INCTST1", "Inception Test Early", models.SecurityTypeStock, &earlyInception)
	if err != nil {
		t.Fatalf("Failed to setup early security: %v", err)
	}
	defer cleanupTestSecurity(pool,"INCTST1")

	secIDLate, err := createTestSecurity(pool, "INCTST2", "Inception Test Late", models.SecurityTypeStock, &laterIPO)
	if err != nil {
		t.Fatalf("Failed to setup late security: %v", err)
	}
	defer cleanupTestSecurity(pool,"INCTST2")

	// Price data: early security has full range, late security only from IPO
	fullStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC) // Monday
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)  // Friday

	if err := insertPriceData(pool, secIDEarly, fullStart, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for early security: %v", err)
	}
	if err := insertPriceData(pool, secIDLate, laterIPO, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert price data for late security: %v", err)
	}

	// Create two ideal portfolios that both include the late-IPO security
	cleanupTestPortfolio(pool,"Inc Test Portfolio A", 1)
	cleanupTestPortfolio(pool,"Inc Test Portfolio B", 1)
	defer cleanupTestPortfolio(pool,"Inc Test Portfolio A", 1)
	defer cleanupTestPortfolio(pool,"Inc Test Portfolio B", 1)

	portfolioAID, err := createTestPortfolio(pool, "Inc Test Portfolio A", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secIDEarly, PercentageOrShares: 0.60},
		{SecurityID: secIDLate, PercentageOrShares: 0.40},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	portfolioBID, err := createTestPortfolio(pool, "Inc Test Portfolio B", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secIDEarly, PercentageOrShares: 0.50},
		{SecurityID: secIDLate, PercentageOrShares: 0.50},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	router := setupDailyValuesTestRouter(pool, avClient)

	// Request starts BEFORE the later IPO — this used to cause a 500 error
	reqBody := models.CompareRequest{
		PortfolioA:  portfolioAID,
		PortfolioB:  portfolioBID,
		StartPeriod: models.FlexibleDate{Time: fullStart}, // Mon Jan 6, before Jan 8 IPO
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

	// Verify W4001 warning was returned
	foundWarning := false
	for _, warn := range response.Warnings {
		if warn.Code == models.WarnStartDateAdjusted {
			foundWarning = true
			t.Logf("Got expected warning: %s", warn.Message)
		}
	}
	if !foundWarning {
		t.Error("Expected W4001 (start date adjusted) warning, but none found")
	}

	// Verify daily values don't contain dates before the IPO
	ipoDateStr := laterIPO.Format("2006-01-02")
	for _, dv := range response.PerformanceMetrics.PortfolioAMetrics.DailyValues {
		dvDate, _ := time.Parse("2006-01-02", dv.Date)
		if dvDate.Before(laterIPO) {
			t.Errorf("Portfolio A has daily value before IPO: %s (IPO is %s)", dv.Date, ipoDateStr)
		}
	}
	for _, dv := range response.PerformanceMetrics.PortfolioBMetrics.DailyValues {
		dvDate, _ := time.Parse("2006-01-02", dv.Date)
		if dvDate.Before(laterIPO) {
			t.Errorf("Portfolio B has daily value before IPO: %s (IPO is %s)", dv.Date, ipoDateStr)
		}
	}

	// Verify daily values are non-empty
	if len(response.PerformanceMetrics.PortfolioAMetrics.DailyValues) == 0 {
		t.Error("Expected daily values for portfolio A, got empty")
	}
	if len(response.PerformanceMetrics.PortfolioBMetrics.DailyValues) == 0 {
		t.Error("Expected daily values for portfolio B, got empty")
	}

	t.Logf("Inception adjustment: Portfolio A has %d daily values, Portfolio B has %d daily values",
		len(response.PerformanceMetrics.PortfolioAMetrics.DailyValues),
		len(response.PerformanceMetrics.PortfolioBMetrics.DailyValues))
}

// TestCompareNoAdjustmentWhenStartDateAfterInception verifies that no warning
// is produced when the requested start date is already after all inception dates.
func TestCompareNoAdjustmentWhenStartDateAfterInception(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := createTestSecurity(pool, "INCNOA1", "Inception NoAdj Test", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to setup security: %v", err)
	}
	defer cleanupTestSecurity(pool,"INCNOA1")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)

	if err := insertPriceData(pool, secID, startDate, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data: %v", err)
	}

	cleanupTestPortfolio(pool,"Inc NoAdj Portfolio", 1)
	defer cleanupTestPortfolio(pool,"Inc NoAdj Portfolio", 1)

	portfolioID, err := createTestPortfolio(pool, "Inc NoAdj Portfolio", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
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

	// No W4001 warning should be present
	for _, warn := range response.Warnings {
		if warn.Code == models.WarnStartDateAdjusted {
			t.Errorf("Did not expect W4001 warning when start date is after inception, but got: %s", warn.Message)
		}
	}
}

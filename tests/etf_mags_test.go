package tests

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
)

// createMockMAGSServer creates a mock AV server that returns MAGS ETF holdings.
func createMockMAGSServer(callCounter *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCounter != nil {
			atomic.AddInt32(callCounter, 1)
		}

		function := r.URL.Query().Get("function")

		if function == "ETF_PROFILE" {
			response := alphavantage.ETFProfileResponse{
				Holdings: []alphavantage.ETFHolding{
					{Symbol: "n/a", Name: "NVIDIA CORP SWAP", Weight: "0.0886"},
					{Symbol: "FGXXX", Name: "FIRST AMERICAN GOVERNMENT OBLIGS X", Weight: "0.0721"},
					{Symbol: "n/a", Name: "ALPHABET INC SWAP GS", Weight: "0.0589"},
					{Symbol: "n/a", Name: "AMAZON.COM INC SWAP", Weight: "0.0576"},
					{Symbol: "AMZN", Name: "AMAZON.COM INC", Weight: "0.0562"},
					{Symbol: "n/a", Name: "ALPHABET INC-CL A SWAP", Weight: "0.0534"},
					{Symbol: "NVDA", Name: "NVIDIA CORP", Weight: "0.0533"},
					{Symbol: "n/a", Name: "MICROSOFT CORP SWAP", Weight: "0.0532"},
					{Symbol: "MSFT", Name: "MICROSOFT CORP", Weight: "0.0526"},
					{Symbol: "TSLA", Name: "TESLA INC", Weight: "0.0522"},
					{Symbol: "META", Name: "META PLATFORMS INC CLASS A", Weight: "0.0513"},
					{Symbol: "n/a", Name: "META PLATFORMS INC-CLASS A SWAP", Weight: "0.0512"},
					{Symbol: "AAPL", Name: "APPLE INC", Weight: "0.0503"},
					{Symbol: "n/a", Name: "TESLA INC SWAP", Weight: "0.0445"},
					{Symbol: "n/a", Name: "APPLE INC SWAP", Weight: "0.0442"},
					{Symbol: "n/a", Name: "TESLA INC SWAP GS", Weight: "0.0423"},
					{Symbol: "GOOGL", Name: "ALPHABET INC CLASS A", Weight: "0.0415"},
					{Symbol: "n/a", Name: "APPLE INC SWAP GS", Weight: "0.0409"},
					{Symbol: "n/a", Name: "AMAZON INC SWAP GS", Weight: "0.0358"},
					{Symbol: "n/a", Name: "MICROSOFT CORP SWAP GS", Weight: "0.0344"},
					{Symbol: "n/a", Name: "META PLATFORMS INC SWAP GS", Weight: "0.0342"},
					{Symbol: "n/a", Name: "US DOLLARS", Weight: "0.0053"},
					{Symbol: "n/a", Name: "OTHER ASSETS AND LIABILITIES", Weight: "-0.0257"},
					{Symbol: "n/a", Name: "CASH OFFSET", Weight: "-0.5705"},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
			return
		}

		// Default: return empty response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
}

// TestMAGSSelfCompare tests a full portfolio comparison with a portfolio that
// holds 100% MAGS ETF, compared against itself. This exercises the full
// resolver chain (swap merging, symbol validation, normalization) and verifies
// correct ETF expansion into the Magnificent 7 stocks.
func TestMAGSSelfCompare(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create MAGS as ETF and 7 underlying equities
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Clean up MAGS ETF data from prior runs (portfolio_membership, prices, ETF data, security)
	cleanupDailyValuesTestSecurity(pool, "TSTMAGS")
	cleanupETFTestData(pool, "TSTMAGS")

	magsID, err := setupTestETF(pool, "TSTMAGS", "Roundhill Magnificent Seven ETF")
	if err != nil {
		t.Fatalf("Failed to setup MAGS ETF: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "TSTMAGS")
	defer cleanupETFTestData(pool, "TSTMAGS")

	type testSec struct {
		ticker string
		name   string
	}
	mag7 := []testSec{
		{"TSTNVDA", "NVIDIA CORP"},
		{"TSTAMZN", "AMAZON.COM INC"},
		{"TSTMSFT", "MICROSOFT CORP"},
		{"TSTAAPL", "APPLE INC"},
		{"TSTTSLA", "TESLA INC"},
		{"TSTGOOGL", "ALPHABET INC CLASS A"},
		{"TSTMETA", "META PLATFORMS INC CLASS A"},
	}

	mag7IDs := make(map[string]int64) // ticker → ID
	for _, s := range mag7 {
		id, err := setupDailyValuesTestSecurity(pool, s.ticker, s.name, &inception)
		if err != nil {
			t.Fatalf("Failed to setup security %s: %v", s.ticker, err)
		}
		mag7IDs[s.ticker] = id
		defer cleanupDailyValuesTestSecurity(pool, s.ticker)
	}

	// US10Y for Sharpe ratio
	us10yID, err := setupDailyValuesTestSecurity(pool, "TSTMG10Y", "Test Treasury Rate MAGS", &inception)
	if err != nil {
		t.Fatalf("Failed to setup US10Y: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "TSTMG10Y")

	// Insert price data for all securities
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC) // Monday
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)  // Friday

	basePrices := map[string]float64{
		"TSTNVDA":  130.0,
		"TSTAMZN":  185.0,
		"TSTMSFT":  420.0,
		"TSTAAPL":  195.0,
		"TSTTSLA":  250.0,
		"TSTGOOGL": 175.0,
		"TSTMETA":  590.0,
	}

	for ticker, id := range mag7IDs {
		if err := insertPriceData(pool, id, startDate, endDate, basePrices[ticker]); err != nil {
			t.Fatalf("Failed to insert price data for %s: %v", ticker, err)
		}
	}

	// Price data for MAGS ETF itself (needed for active portfolio pricing, though we use ideal)
	if err := insertPriceData(pool, magsID, startDate, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert MAGS ETF price data: %v", err)
	}
	// US10Y price data
	if err := insertPriceData(pool, us10yID, startDate, endDate, 4.5); err != nil {
		t.Fatalf("Failed to insert US10Y price data: %v", err)
	}

	// Create mock AV server returning MAGS holdings
	// The mock uses TST-prefixed tickers, but AV returns real-world symbols.
	// We need the mock AV to return the TST-prefixed symbols that match our dim_security entries.
	// Actually — the AV mock returns real-format holdings (NVDA, AMZN, etc.) because
	// that's how AV works. But our dim_security has TSTNVDA, etc.
	// To make this work, the mock must return our TST-prefixed symbols.
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		function := r.URL.Query().Get("function")

		if function == "ETF_PROFILE" {
			response := alphavantage.ETFProfileResponse{
				Holdings: []alphavantage.ETFHolding{
					{Symbol: "n/a", Name: "NVIDIA CORP SWAP", Weight: "0.0886"},
					{Symbol: "FGXXX", Name: "FIRST AMERICAN GOVERNMENT OBLIGS X", Weight: "0.0721"},
					{Symbol: "n/a", Name: "ALPHABET INC SWAP GS", Weight: "0.0589"},
					{Symbol: "n/a", Name: "AMAZON.COM INC SWAP", Weight: "0.0576"},
					{Symbol: "TSTAMZN", Name: "AMAZON.COM INC", Weight: "0.0562"},
					{Symbol: "n/a", Name: "ALPHABET INC-CL A SWAP", Weight: "0.0534"},
					{Symbol: "TSTNVDA", Name: "NVIDIA CORP", Weight: "0.0533"},
					{Symbol: "n/a", Name: "MICROSOFT CORP SWAP", Weight: "0.0532"},
					{Symbol: "TSTMSFT", Name: "MICROSOFT CORP", Weight: "0.0526"},
					{Symbol: "TSTTSLA", Name: "TESLA INC", Weight: "0.0522"},
					{Symbol: "TSTMETA", Name: "META PLATFORMS INC CLASS A", Weight: "0.0513"},
					{Symbol: "n/a", Name: "META PLATFORMS INC-CLASS A SWAP", Weight: "0.0512"},
					{Symbol: "TSTAAPL", Name: "APPLE INC", Weight: "0.0503"},
					{Symbol: "n/a", Name: "TESLA INC SWAP", Weight: "0.0445"},
					{Symbol: "n/a", Name: "APPLE INC SWAP", Weight: "0.0442"},
					{Symbol: "n/a", Name: "TESLA INC SWAP GS", Weight: "0.0423"},
					{Symbol: "TSTGOOGL", Name: "ALPHABET INC CLASS A", Weight: "0.0415"},
					{Symbol: "n/a", Name: "APPLE INC SWAP GS", Weight: "0.0409"},
					{Symbol: "n/a", Name: "AMAZON INC SWAP GS", Weight: "0.0358"},
					{Symbol: "n/a", Name: "MICROSOFT CORP SWAP GS", Weight: "0.0344"},
					{Symbol: "n/a", Name: "META PLATFORMS INC SWAP GS", Weight: "0.0342"},
					{Symbol: "n/a", Name: "US DOLLARS", Weight: "0.0053"},
					{Symbol: "n/a", Name: "OTHER ASSETS AND LIABILITIES", Weight: "-0.0257"},
					{Symbol: "n/a", Name: "CASH OFFSET", Weight: "-0.5705"},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	// Create Ideal portfolio holding 100% MAGS
	portfolioName := "MAGS Test Portfolio"
	cleanupDailyValuesTestPortfolio(pool, portfolioName, 1)
	defer cleanupDailyValuesTestPortfolio(pool, portfolioName, 1)

	portfolioID, err := createTestPortfolio(pool, portfolioName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: magsID, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("Failed to create MAGS portfolio: %v", err)
	}

	// Run self-compare
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

	// Assert: self-compare should have similarity score of 1.0
	if math.Abs(response.AbsoluteSimilarityScore-1.0) > 0.0001 {
		t.Errorf("Expected absolute_similarity_score 1.0 for self-compare, got %.6f",
			response.AbsoluteSimilarityScore)
	}

	// Assert: expanded memberships should have exactly 7 holdings (the Magnificent 7)
	expandedA := response.PortfolioA.ExpandedMemberships
	if len(expandedA) != 7 {
		t.Errorf("Expected 7 expanded memberships (Magnificent 7), got %d", len(expandedA))
		for _, m := range expandedA {
			t.Logf("  %s: %.6f", m.Symbol, m.Allocation)
		}
	}

	// Assert: expanded allocations sum to ~1.0
	var allocSum float64
	expandedSymbols := make(map[string]float64)
	for _, m := range expandedA {
		allocSum += m.Allocation
		expandedSymbols[m.Symbol] = m.Allocation
	}
	if math.Abs(allocSum-1.0) > 0.001 {
		t.Errorf("Expected expanded allocations to sum to ~1.0, got %.6f", allocSum)
	}

	// Assert: all 7 stocks are present
	expectedStocks := []string{"TSTNVDA", "TSTAMZN", "TSTMSFT", "TSTAAPL", "TSTTSLA", "TSTGOOGL", "TSTMETA"}
	for _, sym := range expectedStocks {
		if _, ok := expandedSymbols[sym]; !ok {
			t.Errorf("Expected %s in expanded memberships, but not found", sym)
		}
	}

	// Assert: FGXXX is NOT in expanded memberships (filtered by symbol validation)
	if _, ok := expandedSymbols["FGXXX"]; ok {
		t.Error("FGXXX should NOT appear in expanded memberships")
	}

	// Assert: check warnings
	var w1001Count, w1002Count int
	for _, warn := range response.Warnings {
		switch warn.Code {
		case models.WarnUnresolvedETFHolding:
			w1001Count++
		case models.WarnPartialETFExpansion:
			w1002Count++
		}
	}

	// W1001: expect warnings for unresolved holdings (US DOLLARS, OTHER ASSETS AND LIABILITIES, CASH OFFSET)
	// plus FGXXX (symbol not in database). The count may vary but should be > 0.
	if w1001Count == 0 {
		t.Error("Expected at least one W1001 warning for unresolved holdings")
	}

	// W1002: expect at most 1 per portfolio invocation. Since we compare a portfolio
	// against itself, ComputeMembership is called twice. The first call fetches from AV
	// and normalizes (emitting W1002). The second call reads from cache where data is
	// already normalized to ~1.0, so no W1002. Hence at most 1 W1002.
	if w1002Count > 1 {
		t.Errorf("Expected at most 1 W1002 warning, got %d", w1002Count)
		for _, warn := range response.Warnings {
			if warn.Code == models.WarnPartialETFExpansion {
				t.Logf("  W1002: %s", warn.Message)
			}
		}
	}

	// Assert: FGXXX should appear in W1001 warnings
	fgxxxWarned := false
	for _, warn := range response.Warnings {
		if warn.Code == models.WarnUnresolvedETFHolding && strings.Contains(warn.Message, "FGXXX") {
			fgxxxWarned = true
		}
	}
	if !fgxxxWarned {
		t.Error("Expected a W1001 warning for FGXXX (symbol not in database)")
	}

	t.Logf("MAGS self-compare: similarity=%.4f, %d expanded holdings, allocSum=%.6f, %d W1001, %d W1002",
		response.AbsoluteSimilarityScore, len(expandedA), allocSum, w1001Count, w1002Count)

	// Log individual allocations for debugging
	for _, m := range expandedA {
		t.Logf("  %s: %.4f (%.1f%%)", m.Symbol, m.Allocation, m.Allocation*100)
	}

	// Log all warnings
	for _, warn := range response.Warnings {
		t.Logf("  Warning %s: %s", warn.Code, warn.Message)
	}
}

// TestMAGSSelfCompareSecondCallUsesCache verifies that the second call to
// compare (which reads cached holdings) doesn't produce duplicate W1002 warnings.
func TestMAGSSelfCompareSecondCallUsesCache(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	cleanupDailyValuesTestSecurity(pool, "TSTMAGS2")
	cleanupETFTestData(pool, "TSTMAGS2")

	magsID, err := setupTestETF(pool, "TSTMAGS2", "Roundhill Magnificent Seven ETF 2")
	if err != nil {
		t.Fatalf("Failed to setup MAGS ETF: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "TSTMAGS2")
	defer cleanupETFTestData(pool, "TSTMAGS2")

	// Create the 7 stocks with TST2 prefix
	stocks := []struct {
		ticker string
		name   string
		price  float64
	}{
		{"TST2NVDA", "NVIDIA CORP", 130.0},
		{"TST2AMZN", "AMAZON.COM INC", 185.0},
		{"TST2MSFT", "MICROSOFT CORP", 420.0},
		{"TST2AAPL", "APPLE INC", 195.0},
		{"TST2TSLA", "TESLA INC", 250.0},
		{"TST2GOGL", "ALPHABET INC CLASS A", 175.0},
		{"TST2META", "META PLATFORMS INC CLASS A", 590.0},
	}

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)

	for _, s := range stocks {
		id, err := setupDailyValuesTestSecurity(pool, s.ticker, s.name, &inception)
		if err != nil {
			t.Fatalf("Failed to setup %s: %v", s.ticker, err)
		}
		defer cleanupDailyValuesTestSecurity(pool, s.ticker)
		if err := insertPriceData(pool, id, startDate, endDate, s.price); err != nil {
			t.Fatalf("Failed to insert price for %s: %v", s.ticker, err)
		}
	}

	us10yID, err := setupDailyValuesTestSecurity(pool, "TST2MG10", "Test Treasury 2", &inception)
	if err != nil {
		t.Fatalf("Failed to setup US10Y: %v", err)
	}
	defer cleanupDailyValuesTestSecurity(pool, "TST2MG10")
	if err := insertPriceData(pool, us10yID, startDate, endDate, 4.5); err != nil {
		t.Fatalf("Failed to insert US10Y price: %v", err)
	}
	if err := insertPriceData(pool, magsID, startDate, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert MAGS price: %v", err)
	}

	// Mock AV with TST2 prefixed symbols
	var avCallCount int32
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&avCallCount, 1)
		function := r.URL.Query().Get("function")
		if function == "ETF_PROFILE" {
			response := alphavantage.ETFProfileResponse{
				Holdings: []alphavantage.ETFHolding{
					{Symbol: "TST2NVDA", Name: "NVIDIA CORP", Weight: "0.1419"},
					{Symbol: "TST2AMZN", Name: "AMAZON.COM INC", Weight: "0.1496"},
					{Symbol: "TST2MSFT", Name: "MICROSOFT CORP", Weight: "0.1402"},
					{Symbol: "TST2AAPL", Name: "APPLE INC", Weight: "0.1354"},
					{Symbol: "TST2TSLA", Name: "TESLA INC", Weight: "0.1390"},
					{Symbol: "TST2GOGL", Name: "ALPHABET INC CLASS A", Weight: "0.1538"},
					{Symbol: "TST2META", Name: "META PLATFORMS INC CLASS A", Weight: "0.1367"},
					{Symbol: "n/a", Name: "US DOLLARS", Weight: "0.0053"},
					{Symbol: "n/a", Name: "CASH OFFSET", Weight: "-0.0019"},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	portfolioName := "MAGS Cache Test"
	cleanupDailyValuesTestPortfolio(pool, portfolioName, 1)
	defer cleanupDailyValuesTestPortfolio(pool, portfolioName, 1)

	portfolioID, err := createTestPortfolio(pool, portfolioName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: magsID, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	router := setupDailyValuesTestRouter(pool, avClient)

	// First compare call — fetches from AV
	reqBody := models.CompareRequest{
		PortfolioA:  portfolioID,
		PortfolioB:  portfolioID,
		StartPeriod: models.FlexibleDate{Time: startDate},
		EndPeriod:   models.FlexibleDate{Time: endDate},
	}
	body1, _ := json.Marshal(reqBody)
	req1, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body1))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("First call: expected 200, got %d: %s", w1.Code, w1.Body.String())
	}

	firstAVCalls := atomic.LoadInt32(&avCallCount)
	if firstAVCalls == 0 {
		t.Error("First call should have hit AV")
	}

	// Second compare call — should use cached data, no additional AV call
	body2, _ := json.Marshal(reqBody)
	req2, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body2))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Second call: expected 200, got %d: %s", w2.Code, w2.Body.String())
	}

	secondAVCalls := atomic.LoadInt32(&avCallCount)
	if secondAVCalls > firstAVCalls {
		t.Errorf("Second call should not hit AV again, but got %d additional calls",
			secondAVCalls-firstAVCalls)
	}

	// Parse second response and check warnings
	var response2 models.CompareResponse
	if err := json.Unmarshal(w2.Body.Bytes(), &response2); err != nil {
		t.Fatalf("Failed to unmarshal second response: %v", err)
	}

	// Cached data should already be normalized to ~1.0, so no W1002
	var w1002Count int
	for _, warn := range response2.Warnings {
		if warn.Code == models.WarnPartialETFExpansion {
			w1002Count++
			t.Logf("W1002 on second call: %s", warn.Message)
		}
	}
	if w1002Count > 0 {
		t.Errorf("Second call (cached data) should produce 0 W1002 warnings, got %d", w1002Count)
	}

	t.Logf("Cache test: first AV calls=%d, second AV calls=%d, second W1002=%d",
		firstAVCalls, secondAVCalls, w1002Count)
}

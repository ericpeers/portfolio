package tests

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
)

// TestMAGSSelfCompare tests a full portfolio comparison with a portfolio that
// holds 100% MAGS ETF, compared against itself. ETF holdings are pre-seeded
// directly into the database (as they would be via LoadETFHoldings in production).
func TestMAGSSelfCompare(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	cleanupTestSecurity(pool, "TSTMAGS")
	magsID, err := createTestETF(pool, "TSTMAGS", "Roundhill Magnificent Seven ETF")
	if err != nil {
		t.Fatalf("Failed to setup MAGS ETF: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTMAGS")

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

	mag7IDs := make(map[string]int64)
	for _, s := range mag7 {
		id, err := createTestSecurity(pool, s.ticker, s.name, models.SecurityTypeStock, &inception)
		if err != nil {
			t.Fatalf("Failed to setup security %s: %v", s.ticker, err)
		}
		mag7IDs[s.ticker] = id
		defer cleanupTestSecurity(pool, s.ticker)
	}

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)

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
	if err := insertPriceData(pool, magsID, startDate, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert MAGS ETF price data: %v", err)
	}

	// Pre-seed ETF holdings directly (as LoadETFHoldings would do in production).
	// Equal weights across 7 holdings.
	holdings := make(map[int64]float64, len(mag7IDs))
	for _, id := range mag7IDs {
		holdings[id] = 1.0 / float64(len(mag7IDs))
	}
	if err := insertETFHoldings(pool, magsID, holdings); err != nil {
		t.Fatalf("Failed to seed MAGS ETF holdings: %v", err)
	}

	portfolioName := "MAGS Test Portfolio"
	cleanupTestPortfolio(pool, portfolioName, 1)
	defer cleanupTestPortfolio(pool, portfolioName, 1)

	portfolioID, err := createTestPortfolio(pool, portfolioName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: magsID, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("Failed to create MAGS portfolio: %v", err)
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

	if math.Abs(response.AbsoluteSimilarityScore-1.0) > 0.0001 {
		t.Errorf("Expected absolute_similarity_score 1.0 for self-compare, got %.6f",
			response.AbsoluteSimilarityScore)
	}

	expandedA := response.PortfolioA.ExpandedMemberships
	if len(expandedA) != 7 {
		t.Errorf("Expected 7 expanded memberships (Magnificent 7), got %d", len(expandedA))
		for _, m := range expandedA {
			t.Logf("  %s: %.6f", m.Ticker, m.Allocation)
		}
	}

	var allocSum float64
	expandedSymbols := make(map[string]float64)
	for _, m := range expandedA {
		allocSum += m.Allocation
		expandedSymbols[m.Ticker] = m.Allocation
	}
	if math.Abs(allocSum-1.0) > 0.001 {
		t.Errorf("Expected expanded allocations to sum to ~1.0, got %.6f", allocSum)
	}

	for _, s := range mag7 {
		if _, ok := expandedSymbols[s.ticker]; !ok {
			t.Errorf("Expected %s in expanded memberships, but not found", s.ticker)
		}
	}

	t.Logf("MAGS self-compare: similarity=%.4f, %d expanded holdings, allocSum=%.6f",
		response.AbsoluteSimilarityScore, len(expandedA), allocSum)
	for _, m := range expandedA {
		t.Logf("  %s: %.4f (%.1f%%)", m.Ticker, m.Allocation, m.Allocation*100)
	}
}

// TestMAGSSelfCompareSecondCallUsesCache verifies that repeated comparison calls
// return consistent results using the pre-seeded holdings cache.
func TestMAGSSelfCompareSecondCallUsesCache(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	cleanupTestSecurity(pool, "TSTMAGS2")
	magsID, err := createTestETF(pool, "TSTMAGS2", "Roundhill Magnificent Seven ETF 2")
	if err != nil {
		t.Fatalf("Failed to setup MAGS ETF: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTMAGS2")

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

	stockHoldings := make(map[int64]float64, len(stocks))
	for _, s := range stocks {
		id, err := createTestSecurity(pool, s.ticker, s.name, models.SecurityTypeStock, &inception)
		if err != nil {
			t.Fatalf("Failed to setup %s: %v", s.ticker, err)
		}
		defer cleanupTestSecurity(pool, s.ticker)
		if err := insertPriceData(pool, id, startDate, endDate, s.price); err != nil {
			t.Fatalf("Failed to insert price for %s: %v", s.ticker, err)
		}
		stockHoldings[id] = 1.0 / float64(len(stocks))
	}

	if err := insertPriceData(pool, magsID, startDate, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert MAGS price: %v", err)
	}

	if err := insertETFHoldings(pool, magsID, stockHoldings); err != nil {
		t.Fatalf("Failed to seed MAGS2 ETF holdings: %v", err)
	}

	portfolioName := "MAGS Cache Test"
	cleanupTestPortfolio(pool, portfolioName, 1)
	defer cleanupTestPortfolio(pool, portfolioName, 1)

	portfolioID, err := createTestPortfolio(pool, portfolioName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: magsID, PercentageOrShares: 1.0},
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

	makeRequest := func(label string) models.CompareResponse {
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d: %s", label, w.Code, w.Body.String())
		}
		var resp models.CompareResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("%s: failed to unmarshal: %v", label, err)
		}
		return resp
	}

	resp1 := makeRequest("first call")
	resp2 := makeRequest("second call")

	// Both calls should produce identical results from the cached holdings.
	if math.Abs(resp1.AbsoluteSimilarityScore-resp2.AbsoluteSimilarityScore) > 0.0001 {
		t.Errorf("Repeated calls returned different similarity scores: %.6f vs %.6f",
			resp1.AbsoluteSimilarityScore, resp2.AbsoluteSimilarityScore)
	}
	if len(resp1.PortfolioA.ExpandedMemberships) != len(resp2.PortfolioA.ExpandedMemberships) {
		t.Errorf("Repeated calls returned different expansion counts: %d vs %d",
			len(resp1.PortfolioA.ExpandedMemberships), len(resp2.PortfolioA.ExpandedMemberships))
	}

	// No W1003 expected — holdings are pre-seeded at exactly 1.0 total weight.
	for _, warn := range resp2.Warnings {
		if warn.Code == models.WarnPartialETFExpansion {
			t.Errorf("Unexpected W1003 on second call: %s", warn.Message)
		}
	}

	t.Logf("Cache test: similarity=%.4f, %d expanded holdings (both calls consistent)",
		resp1.AbsoluteSimilarityScore, len(resp1.PortfolioA.ExpandedMemberships))
}

package tests

import (
	"bytes"
	"encoding/json"
	"math"
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

// setupBasketTestRouter creates a compare router for basket tests
func setupBasketTestRouter(pool *pgxpool.Pool, avClient *alphavantage.Client) *gin.Engine {
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

// findBasketHolding returns the BasketHolding for the given symbol in a BasketLevel
func findBasketHolding(level models.BasketLevel, symbol string) *models.BasketHolding {
	for i := range level.Holdings {
		if level.Holdings[i].Symbol == symbol {
			return &level.Holdings[i]
		}
	}
	return nil
}

// callCompare posts a compare request and returns the parsed response
func callCompare(t *testing.T, router *gin.Engine, aID, bID int64, start, end time.Time) *models.CompareResponse {
	t.Helper()
	reqBody := models.CompareRequest{
		PortfolioA:  aID,
		PortfolioB:  bID,
		StartPeriod: models.FlexibleDate{Time: start},
		EndPeriod:   models.FlexibleDate{Time: end},
	}
	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Compare endpoint returned %d: %s", w.Code, w.Body.String())
	}
	var resp models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal compare response: %v", err)
	}
	return &resp
}

// TestBasketAnalysisThresholds verifies basket fills and threshold behavior for an
// ideal portfolio A compared against an ideal portfolio B.
//
// Portfolio A (ideal): BSKTETF1 (60%), BSKTS4 (40%)
// ETF1 constituents: BSKTS1 (40%), BSKTS2 (30%), BSKTS3 (30%)
// Portfolio B (ideal): BSKTS1 (35%), BSKTS2 (25%), BSKTS4 (30%), BSKTETF1 (10%)
//
// Available constituent weight for ETF1 in B's stock pool = 0.70 (BSKTS1 + BSKTS2)
//
// Expected fills:
//   basket_20/40/60 (T ≤ 0.70): ETF1 redeems, redeemedFill=0.60, totalFill=1.00
//   basket_80/100  (T > 0.70):  ETF1 does not redeem,           totalFill=0.40
func TestBasketAnalysisThresholds(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// --- Tickers ---
	allTickers := []string{"BSKTETF1", "BSKTS1", "BSKTS2", "BSKTS3", "BSKTS4"}
	for _, tk := range allTickers {
		cleanupTestSecurity(pool, tk)
	}
	defer func() {
		for _, tk := range allTickers {
			cleanupTestSecurity(pool, tk)
		}
	}()
	cleanupTestPortfolio(pool, "BSKT Ideal A", 1)
	cleanupTestPortfolio(pool, "BSKT Ideal B", 1)
	defer cleanupTestPortfolio(pool, "BSKT Ideal A", 1)
	defer cleanupTestPortfolio(pool, "BSKT Ideal B", 1)

	// --- Create securities ---
	etfID, err := createTestETF(pool, "BSKTETF1", "Basket Test ETF 1")
	if err != nil {
		t.Fatalf("Failed to create ETF: %v", err)
	}
	stockID1, err := createTestStock(pool, "BSKTS1", "Basket Stock 1")
	if err != nil {
		t.Fatalf("Failed to create stock 1: %v", err)
	}
	stockID2, err := createTestStock(pool, "BSKTS2", "Basket Stock 2")
	if err != nil {
		t.Fatalf("Failed to create stock 2: %v", err)
	}
	stockID3, err := createTestStock(pool, "BSKTS3", "Basket Stock 3")
	if err != nil {
		t.Fatalf("Failed to create stock 3: %v", err)
	}
	stockID4, err := createTestStock(pool, "BSKTS4", "Basket Stock 4")
	if err != nil {
		t.Fatalf("Failed to create stock 4: %v", err)
	}

	// --- ETF1 holdings: BSKTS1 40%, BSKTS2 30%, BSKTS3 30% (total = 1.00) ---
	if err := insertETFHoldings(pool, etfID, map[int64]float64{
		stockID1: 0.40,
		stockID2: 0.30,
		stockID3: 0.30,
	}); err != nil {
		t.Fatalf("Failed to insert ETF holdings: %v", err)
	}

	// --- Price data ---
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	for _, id := range []int64{etfID, stockID1, stockID2, stockID3, stockID4} {
		if err := insertPriceData(pool, id, startDate, endDate, 100.0); err != nil {
			t.Fatalf("Failed to insert price data for security %d: %v", id, err)
		}
	}

	// --- Portfolio A (ideal): ETF1=0.60, BSKTS4=0.40 ---
	portfolioAID, err := createTestPortfolio(pool, "BSKT Ideal A", 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: etfID, PercentageOrShares: 0.60},
			{SecurityID: stockID4, PercentageOrShares: 0.40},
		})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	// --- Portfolio B (ideal): BSKTS1=0.35, BSKTS2=0.25, BSKTS4=0.30, ETF1=0.10 ---
	// Total = 1.00 so allocations == PercentageOrShares.
	portfolioBID, err := createTestPortfolio(pool, "BSKT Ideal B", 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: stockID1, PercentageOrShares: 0.35},
			{SecurityID: stockID2, PercentageOrShares: 0.25},
			{SecurityID: stockID4, PercentageOrShares: 0.30},
			{SecurityID: etfID, PercentageOrShares: 0.10},
		})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	// --- Router with mock AV (all data is already in DB) ---
	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupBasketTestRouter(pool, avClient)

	resp := callCompare(t, router, portfolioAID, portfolioBID, startDate, endDate)

	// --- Baskets must be present (A is ideal) ---
	if resp.Baskets == nil {
		t.Fatal("Expected baskets to be present when portfolio A is ideal, got nil")
	}

	const eps = 0.001

	// Helper: check a threshold level
	type wantLevel struct {
		threshold       float64
		level           models.BasketLevel
		wantTotalFill   float64
		wantETFDirect   float64
		wantETFRedeemed float64
		wantETFCoverage float64
		wantStockDirect float64
	}

	levels := []wantLevel{
		{0.20, resp.Baskets.Basket20, 1.00, 0.10, 0.60, 0.70, 0.30},
		{0.40, resp.Baskets.Basket40, 1.00, 0.10, 0.60, 0.70, 0.30},
		{0.60, resp.Baskets.Basket60, 1.00, 0.10, 0.60, 0.70, 0.30},
		{0.80, resp.Baskets.Basket80, 0.40, 0.10, 0.00, 0.70, 0.30},
		{1.00, resp.Baskets.Basket100, 0.40, 0.10, 0.00, 0.70, 0.30},
	}

	for _, wl := range levels {
		if wl.level.Threshold != wl.threshold {
			t.Errorf("basket threshold: got %.2f, want %.2f", wl.level.Threshold, wl.threshold)
		}
		if math.Abs(wl.level.TotalFill-wl.wantTotalFill) > eps {
			t.Errorf("basket_%.0f TotalFill: got %.4f, want %.4f", wl.threshold*100, wl.level.TotalFill, wl.wantTotalFill)
		}

		etfH := findBasketHolding(wl.level, "BSKTETF1")
		if etfH == nil {
			t.Errorf("basket_%.0f: BSKTETF1 holding missing", wl.threshold*100)
			continue
		}
		if math.Abs(etfH.DirectFill-wl.wantETFDirect) > eps {
			t.Errorf("basket_%.0f BSKTETF1.DirectFill: got %.4f, want %.4f", wl.threshold*100, etfH.DirectFill, wl.wantETFDirect)
		}
		if math.Abs(etfH.RedeemedFill-wl.wantETFRedeemed) > eps {
			t.Errorf("basket_%.0f BSKTETF1.RedeemedFill: got %.4f, want %.4f", wl.threshold*100, etfH.RedeemedFill, wl.wantETFRedeemed)
		}
		if math.Abs(etfH.CoverageWeight-wl.wantETFCoverage) > eps {
			t.Errorf("basket_%.0f BSKTETF1.CoverageWeight: got %.4f, want %.4f", wl.threshold*100, etfH.CoverageWeight, wl.wantETFCoverage)
		}
		if math.Abs(etfH.IdealAlloc-0.60) > eps {
			t.Errorf("basket_%.0f BSKTETF1.IdealAlloc: got %.4f, want 0.60", wl.threshold*100, etfH.IdealAlloc)
		}

		stockH := findBasketHolding(wl.level, "BSKTS4")
		if stockH == nil {
			t.Errorf("basket_%.0f: BSKTS4 holding missing", wl.threshold*100)
			continue
		}
		if math.Abs(stockH.DirectFill-wl.wantStockDirect) > eps {
			t.Errorf("basket_%.0f BSKTS4.DirectFill: got %.4f, want %.4f", wl.threshold*100, stockH.DirectFill, wl.wantStockDirect)
		}
		if stockH.RedeemedFill != 0 {
			t.Errorf("basket_%.0f BSKTS4 is a stock; expected RedeemedFill=0, got %.4f", wl.threshold*100, stockH.RedeemedFill)
		}
		if math.Abs(stockH.IdealAlloc-0.40) > eps {
			t.Errorf("basket_%.0f BSKTS4.IdealAlloc: got %.4f, want 0.40", wl.threshold*100, stockH.IdealAlloc)
		}
	}

	t.Logf("basket_60 TotalFill=%.4f (want 1.00), basket_80 TotalFill=%.4f (want 0.40)",
		resp.Baskets.Basket60.TotalFill, resp.Baskets.Basket80.TotalFill)
}

// TestBasketRoundRobin verifies that a constituent stock shared by two ETFs in portfolio A
// is only redeemed once per threshold (round-robin, first ETF wins).
//
// Portfolio A (ideal): BSKTRRETF1 (50%), BSKTRRETF2 (50%)
// Both ETFs hold BSKTRRS1 (100%)
// Portfolio B (ideal): BSKTRRS1 (100%)
//
// At any qualifying threshold, only one ETF can claim BSKTRRS1.
// Total redeemed across both = 1.00 (not 2.00).
func TestBasketRoundRobin(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	rrTickers := []string{"BSKTRRETF1", "BSKTRRETF2", "BSKTRRS1"}
	for _, tk := range rrTickers {
		cleanupTestSecurity(pool, tk)
	}
	defer func() {
		for _, tk := range rrTickers {
			cleanupTestSecurity(pool, tk)
		}
	}()
	cleanupTestPortfolio(pool, "BSKT RR Ideal A", 1)
	cleanupTestPortfolio(pool, "BSKT RR Ideal B", 1)
	defer cleanupTestPortfolio(pool, "BSKT RR Ideal A", 1)
	defer cleanupTestPortfolio(pool, "BSKT RR Ideal B", 1)

	// Create securities
	rrEtfID1, err := createTestETF(pool, "BSKTRRETF1", "Basket RR ETF 1")
	if err != nil {
		t.Fatalf("Failed to create ETF 1: %v", err)
	}
	rrEtfID2, err := createTestETF(pool, "BSKTRRETF2", "Basket RR ETF 2")
	if err != nil {
		t.Fatalf("Failed to create ETF 2: %v", err)
	}
	rrStockID, err := createTestStock(pool, "BSKTRRS1", "Basket RR Stock 1")
	if err != nil {
		t.Fatalf("Failed to create shared stock: %v", err)
	}

	// Both ETFs hold 100% of BSKTRRS1
	if err := insertETFHoldings(pool, rrEtfID1, map[int64]float64{rrStockID: 1.00}); err != nil {
		t.Fatalf("Failed to insert ETF1 holdings: %v", err)
	}
	if err := insertETFHoldings(pool, rrEtfID2, map[int64]float64{rrStockID: 1.00}); err != nil {
		t.Fatalf("Failed to insert ETF2 holdings: %v", err)
	}

	// Price data
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	for _, id := range []int64{rrEtfID1, rrEtfID2, rrStockID} {
		if err := insertPriceData(pool, id, startDate, endDate, 100.0); err != nil {
			t.Fatalf("Failed to insert price data: %v", err)
		}
	}

	// Portfolio A: both ETFs, equal weight
	portfolioAID, err := createTestPortfolio(pool, "BSKT RR Ideal A", 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: rrEtfID1, PercentageOrShares: 0.50},
			{SecurityID: rrEtfID2, PercentageOrShares: 0.50},
		})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	// Portfolio B: only the shared stock
	portfolioBID, err := createTestPortfolio(pool, "BSKT RR Ideal B", 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: rrStockID, PercentageOrShares: 1.00},
		})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupBasketTestRouter(pool, avClient)

	resp := callCompare(t, router, portfolioAID, portfolioBID, startDate, endDate)

	if resp.Baskets == nil {
		t.Fatal("Expected baskets to be present when portfolio A is ideal")
	}

	// At basket_20 both ETFs have availableWeight=1.00 >= 0.20, so both qualify.
	// But they share the only constituent: only one can claim the stock.
	// Total redeemed fill must be <= 1.00 (the stock's alloc in B), not 2.00.
	b20 := resp.Baskets.Basket20

	etf1H := findBasketHolding(b20, "BSKTRRETF1")
	etf2H := findBasketHolding(b20, "BSKTRRETF2")
	if etf1H == nil || etf2H == nil {
		t.Fatal("basket_20: one or both ETF holdings missing")
	}

	totalRedeemed := etf1H.RedeemedFill + etf2H.RedeemedFill
	if totalRedeemed > 1.00+0.001 {
		t.Errorf("Round-robin violated: total redeemed fill %.4f > 1.00 (double-counted)", totalRedeemed)
	}
	if math.Abs(totalRedeemed-1.00) > 0.001 {
		t.Errorf("Expected one ETF to claim the stock (totalRedeemed=1.00), got %.4f", totalRedeemed)
	}

	t.Logf("basket_20 RR: ETF1 redeemed=%.4f, ETF2 redeemed=%.4f, total=%.4f (expected 1.00)",
		etf1H.RedeemedFill, etf2H.RedeemedFill, totalRedeemed)
}

// TestBasketBETFExpansion verifies that B's non-direct-fill ETFs are expanded into constituent
// stocks and contribute to the redemption pool.
//
// Portfolio A (ideal): BSKXETFA (100%)
// Portfolio B (ideal): BSKXETFB (50%), BSKXSD (50%)
// BSKXETFA constituents: BSKXSC (60%), BSKXSD (40%)
// BSKXETFB constituents: BSKXSC (80%), BSKXSE (20%)
//
// B does NOT hold BSKXETFA → BSKXETFB is NOT a direct fill → it IS expanded.
// expandedBPool: {BSKXSC: 0.40, BSKXSD: 0.50, BSKXSE: 0.10}
//
// BSKXETFA's constituents BSKXSC and BSKXSD are both in the expanded pool,
// so availableWeight = 1.00 (all constituents covered).
// At all thresholds (T ≤ 1.00): proportional redemption:
//   BSKXSC: min(pool=0.40, cap=0.60) = 0.40
//   BSKXSD: min(pool=0.50, cap=0.40) = 0.40  ← B has excess BSKXSD; capped at ETF weight
//   RedeemedFill = 0.80, TotalFill = 0.80
// DirectFill = 0 (B doesn't hold BSKXETFA).
func TestBasketBETFExpansion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	allTickers := []string{"BSKXETFA", "BSKXETFB", "BSKXSC", "BSKXSD", "BSKXSE"}
	for _, tk := range allTickers {
		cleanupTestSecurity(pool, tk)
	}
	defer func() {
		for _, tk := range allTickers {
			cleanupTestSecurity(pool, tk)
		}
	}()
	cleanupTestPortfolio(pool, "BSKX Ideal A", 1)
	cleanupTestPortfolio(pool, "BSKX Ideal B", 1)
	defer cleanupTestPortfolio(pool, "BSKX Ideal A", 1)
	defer cleanupTestPortfolio(pool, "BSKX Ideal B", 1)

	// --- Create securities ---
	etfAID, err := createTestETF(pool, "BSKXETFA", "Basket X ETF A")
	if err != nil {
		t.Fatalf("Failed to create ETF A: %v", err)
	}
	etfBID, err := createTestETF(pool, "BSKXETFB", "Basket X ETF B")
	if err != nil {
		t.Fatalf("Failed to create ETF B: %v", err)
	}
	scID, err := createTestStock(pool, "BSKXSC", "Basket X Stock C")
	if err != nil {
		t.Fatalf("Failed to create stock C: %v", err)
	}
	sdID, err := createTestStock(pool, "BSKXSD", "Basket X Stock D")
	if err != nil {
		t.Fatalf("Failed to create stock D: %v", err)
	}
	seID, err := createTestStock(pool, "BSKXSE", "Basket X Stock E")
	if err != nil {
		t.Fatalf("Failed to create stock E: %v", err)
	}

	// --- ETF A constituents: BSKXSC 60%, BSKXSD 40% ---
	if err := insertETFHoldings(pool, etfAID, map[int64]float64{
		scID: 0.60,
		sdID: 0.40,
	}); err != nil {
		t.Fatalf("Failed to insert ETF A holdings: %v", err)
	}

	// --- ETF B constituents: BSKXSC 80%, BSKXSE 20% ---
	if err := insertETFHoldings(pool, etfBID, map[int64]float64{
		scID: 0.80,
		seID: 0.20,
	}); err != nil {
		t.Fatalf("Failed to insert ETF B holdings: %v", err)
	}

	// --- Price data ---
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	for _, id := range []int64{etfAID, etfBID, scID, sdID, seID} {
		if err := insertPriceData(pool, id, startDate, endDate, 100.0); err != nil {
			t.Fatalf("Failed to insert price data for security %d: %v", id, err)
		}
	}

	// --- Portfolio A (ideal): BSKXETFA 100% ---
	portfolioAID, err := createTestPortfolio(pool, "BSKX Ideal A", 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: etfAID, PercentageOrShares: 1.00},
		})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	// --- Portfolio B (ideal): BSKXETFB 50%, BSKXSD 50% ---
	portfolioBID, err := createTestPortfolio(pool, "BSKX Ideal B", 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: etfBID, PercentageOrShares: 0.50},
			{SecurityID: sdID, PercentageOrShares: 0.50},
		})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	// --- Router with mock AV ---
	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupBasketTestRouter(pool, avClient)

	resp := callCompare(t, router, portfolioAID, portfolioBID, startDate, endDate)

	if resp.Baskets == nil {
		t.Fatal("Expected baskets to be present when portfolio A is ideal")
	}

	const eps = 0.001

	// At all thresholds, BSKXETFA should be fully redeemed:
	// availableWeight=1.00 (both BSKXSC and BSKXSD in expandedBPool), DirectFill=0, RedeemedFill=0.90.
	levels := []struct {
		threshold float64
		level     models.BasketLevel
	}{
		{0.20, resp.Baskets.Basket20},
		{0.40, resp.Baskets.Basket40},
		{0.60, resp.Baskets.Basket60},
		{0.80, resp.Baskets.Basket80},
		{1.00, resp.Baskets.Basket100},
	}

	for _, wl := range levels {
		etfH := findBasketHolding(wl.level, "BSKXETFA")
		if etfH == nil {
			t.Errorf("basket_%.0f: BSKXETFA holding missing", wl.threshold*100)
			continue
		}
		if math.Abs(etfH.DirectFill-0.00) > eps {
			t.Errorf("basket_%.0f BSKXETFA.DirectFill: got %.4f, want 0.00", wl.threshold*100, etfH.DirectFill)
		}
		if math.Abs(etfH.CoverageWeight-1.00) > eps {
			t.Errorf("basket_%.0f BSKXETFA.CoverageWeight: got %.4f, want 1.00", wl.threshold*100, etfH.CoverageWeight)
		}
		if math.Abs(etfH.RedeemedFill-0.80) > eps {
			t.Errorf("basket_%.0f BSKXETFA.RedeemedFill: got %.4f, want 0.80", wl.threshold*100, etfH.RedeemedFill)
		}
		if math.Abs(wl.level.TotalFill-0.80) > eps {
			t.Errorf("basket_%.0f TotalFill: got %.4f, want 0.80", wl.threshold*100, wl.level.TotalFill)
		}
	}

	t.Logf("basket_60 BSKXETFA: DirectFill=%.4f, CoverageWeight=%.4f, RedeemedFill=%.4f (want 0.80), TotalFill=%.4f",
		findBasketHolding(resp.Baskets.Basket60, "BSKXETFA").DirectFill,
		findBasketHolding(resp.Baskets.Basket60, "BSKXETFA").CoverageWeight,
		findBasketHolding(resp.Baskets.Basket60, "BSKXETFA").RedeemedFill,
		resp.Baskets.Basket60.TotalFill)
}

// TestBasketProportionalRedemption verifies that RedeemedFill is capped at each
// constituent's ETF weight, not at B's full pool allocation.
//
// Portfolio A (ideal):
//   BSKPETF (60%), BSKPETF2 (40%)
//
// BSKPETF  constituents: BSKPA (40%), BSKPB (30%), BSKPC (30%)
// BSKPETF2 constituents: BSKPA (100%)
//
// Portfolio B (ideal): BSKPA (50%), BSKPB (50%)
//
// expandedBPool: {BSKPA: 0.50, BSKPB: 0.50}
//
// BSKPETF:  availableConst=[BSKPA,BSKPB], CoverageWeight=0.70 (BSKPC absent)
// BSKPETF2: availableConst=[BSKPA],       CoverageWeight=1.00
//
// At T=0.60 (round-robin: BSKPETF first, BSKPETF2 second):
//   BSKPETF  redeems: min(pool[BSKPA], 0.40)=0.40  + min(pool[BSKPB], 0.30)=0.30 = 0.70
//             remaining pool: BSKPA=0.10, BSKPB=0.20
//   BSKPETF2 redeems: min(pool[BSKPA], 1.00)=0.10  (only leftover BSKPA)
//
//   BSKPETF.RedeemedFill  = 0.70  (NOT 1.00 — the over-grab bug)
//   BSKPETF2.RedeemedFill = 0.10
//   TotalFill             = 0.70*0.60 + 0.10*0.40 ... wait, TotalFill = sum(DirectFill+RedeemedFill)
//                         = 0.00+0.70 + 0.00+0.10 = 0.80
//
// At T=0.80: BSKPETF CoverageWeight(0.70) < 0.80, no redemption.
//   BSKPETF.RedeemedFill  = 0.00
//   BSKPETF2 redeems min(pool[BSKPA], 1.00)=0.50
//   BSKPETF2.RedeemedFill = 0.50
//   TotalFill             = 0.50
func TestBasketProportionalRedemption(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	allTickers := []string{"BSKPETF", "BSKPETF2", "BSKPA", "BSKPB", "BSKPC"}
	for _, tk := range allTickers {
		cleanupTestSecurity(pool, tk)
	}
	defer func() {
		for _, tk := range allTickers {
			cleanupTestSecurity(pool, tk)
		}
	}()
	cleanupTestPortfolio(pool, "BSKP Ideal A", 1)
	cleanupTestPortfolio(pool, "BSKP Ideal B", 1)
	defer cleanupTestPortfolio(pool, "BSKP Ideal A", 1)
	defer cleanupTestPortfolio(pool, "BSKP Ideal B", 1)

	etfID, err := createTestETF(pool, "BSKPETF", "Basket Prop ETF")
	if err != nil {
		t.Fatalf("Failed to create BSKPETF: %v", err)
	}
	etf2ID, err := createTestETF(pool, "BSKPETF2", "Basket Prop ETF2")
	if err != nil {
		t.Fatalf("Failed to create BSKPETF2: %v", err)
	}
	aID, err := createTestStock(pool, "BSKPA", "Basket Prop Stock A")
	if err != nil {
		t.Fatalf("Failed to create BSKPA: %v", err)
	}
	bID, err := createTestStock(pool, "BSKPB", "Basket Prop Stock B")
	if err != nil {
		t.Fatalf("Failed to create BSKPB: %v", err)
	}
	cID, err := createTestStock(pool, "BSKPC", "Basket Prop Stock C")
	if err != nil {
		t.Fatalf("Failed to create BSKPC: %v", err)
	}

	// BSKPETF: BSKPA 40%, BSKPB 30%, BSKPC 30%
	if err := insertETFHoldings(pool, etfID, map[int64]float64{
		aID: 0.40, bID: 0.30, cID: 0.30,
	}); err != nil {
		t.Fatalf("Failed to insert BSKPETF holdings: %v", err)
	}
	// BSKPETF2: BSKPA 100%
	if err := insertETFHoldings(pool, etf2ID, map[int64]float64{
		aID: 1.00,
	}); err != nil {
		t.Fatalf("Failed to insert BSKPETF2 holdings: %v", err)
	}

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	for _, id := range []int64{etfID, etf2ID, aID, bID, cID} {
		if err := insertPriceData(pool, id, startDate, endDate, 100.0); err != nil {
			t.Fatalf("Failed to insert price data: %v", err)
		}
	}

	// A: BSKPETF 60%, BSKPETF2 40%
	portfolioAID, err := createTestPortfolio(pool, "BSKP Ideal A", 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: etfID, PercentageOrShares: 0.60},
			{SecurityID: etf2ID, PercentageOrShares: 0.40},
		})
	if err != nil {
		t.Fatalf("Failed to create portfolio A: %v", err)
	}

	// B: BSKPA 50%, BSKPB 50%
	portfolioBID, err := createTestPortfolio(pool, "BSKP Ideal B", 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: aID, PercentageOrShares: 0.50},
			{SecurityID: bID, PercentageOrShares: 0.50},
		})
	if err != nil {
		t.Fatalf("Failed to create portfolio B: %v", err)
	}

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupBasketTestRouter(pool, avClient)

	resp := callCompare(t, router, portfolioAID, portfolioBID, startDate, endDate)
	if resp.Baskets == nil {
		t.Fatal("Expected baskets to be present")
	}

	const eps = 0.001

	// --- basket_60: BSKPETF redeems proportionally, BSKPETF2 gets leftover ---
	b60 := resp.Baskets.Basket60
	etfH60 := findBasketHolding(b60, "BSKPETF")
	etf2H60 := findBasketHolding(b60, "BSKPETF2")
	if etfH60 == nil || etf2H60 == nil {
		t.Fatal("basket_60: missing ETF holding(s)")
	}
	if math.Abs(etfH60.CoverageWeight-0.70) > eps {
		t.Errorf("basket_60 BSKPETF.CoverageWeight: got %.4f, want 0.70", etfH60.CoverageWeight)
	}
	// Key assertion: must NOT grab 1.00 (B's full BSKPA+BSKPB pool)
	if math.Abs(etfH60.RedeemedFill-0.70) > eps {
		t.Errorf("basket_60 BSKPETF.RedeemedFill: got %.4f, want 0.70 (over-grab bug: consumed B's full pool instead of proportional ETF weights)", etfH60.RedeemedFill)
	}
	// BSKPETF2 gets only the leftover BSKPA (0.50 - 0.40 = 0.10)
	if math.Abs(etf2H60.RedeemedFill-0.10) > eps {
		t.Errorf("basket_60 BSKPETF2.RedeemedFill: got %.4f, want 0.10 (leftover after BSKPETF proportional redemption)", etf2H60.RedeemedFill)
	}
	if math.Abs(b60.TotalFill-0.80) > eps {
		t.Errorf("basket_60 TotalFill: got %.4f, want 0.80", b60.TotalFill)
	}

	// --- basket_80: BSKPETF blocked (0.70 < 0.80), BSKPETF2 gets full BSKPA pool ---
	b80 := resp.Baskets.Basket80
	etfH80 := findBasketHolding(b80, "BSKPETF")
	etf2H80 := findBasketHolding(b80, "BSKPETF2")
	if etfH80 == nil || etf2H80 == nil {
		t.Fatal("basket_80: missing ETF holding(s)")
	}
	if etfH80.RedeemedFill != 0 {
		t.Errorf("basket_80 BSKPETF.RedeemedFill: got %.4f, want 0.00 (coverage 0.70 < threshold 0.80)", etfH80.RedeemedFill)
	}
	if math.Abs(etf2H80.RedeemedFill-0.50) > eps {
		t.Errorf("basket_80 BSKPETF2.RedeemedFill: got %.4f, want 0.50 (full BSKPA pool available)", etf2H80.RedeemedFill)
	}
	if math.Abs(b80.TotalFill-0.50) > eps {
		t.Errorf("basket_80 TotalFill: got %.4f, want 0.50", b80.TotalFill)
	}

	t.Logf("basket_60: BSKPETF redeemed=%.4f (want 0.70), BSKPETF2 redeemed=%.4f (want 0.10), total=%.4f (want 0.80)",
		etfH60.RedeemedFill, etf2H60.RedeemedFill, b60.TotalFill)
	t.Logf("basket_80: BSKPETF redeemed=%.4f (want 0.00), BSKPETF2 redeemed=%.4f (want 0.50), total=%.4f (want 0.50)",
		etfH80.RedeemedFill, etf2H80.RedeemedFill, b80.TotalFill)
}

// TestBasketNotIdealA verifies that the baskets field is omitted when portfolio A is not ideal.
func TestBasketNotIdealA(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	for _, tk := range []string{"BSKNOSTA", "BSKNOSTB"} {
		cleanupTestSecurity(pool, tk)
	}
	defer func() {
		for _, tk := range []string{"BSKNOSTA", "BSKNOSTB"} {
			cleanupTestSecurity(pool, tk)
		}
	}()
	cleanupTestPortfolio(pool, "BSKT Active A", 1)
	cleanupTestPortfolio(pool, "BSKT Ideal B2", 1)
	defer cleanupTestPortfolio(pool, "BSKT Active A", 1)
	defer cleanupTestPortfolio(pool, "BSKT Ideal B2", 1)

	// Create securities
	nostaID, err := createTestStock(pool, "BSKNOSTA", "Basket Non-Ideal Stock A")
	if err != nil {
		t.Fatalf("Failed to create stock A: %v", err)
	}
	nostbID, err := createTestStock(pool, "BSKNOSTB", "Basket Non-Ideal Stock B")
	if err != nil {
		t.Fatalf("Failed to create stock B: %v", err)
	}

	// Price data for both
	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(pool, nostaID, startDate, endDate, 50.0); err != nil {
		t.Fatalf("Failed to insert price data A: %v", err)
	}
	if err := insertPriceData(pool, nostbID, startDate, endDate, 100.0); err != nil {
		t.Fatalf("Failed to insert price data B: %v", err)
	}

	// Portfolio A is Active (not ideal)
	portfolioAID, err := createTestPortfolio(pool, "BSKT Active A", 1, models.PortfolioTypeActive,
		[]models.MembershipRequest{
			{SecurityID: nostaID, PercentageOrShares: 10},
		})
	if err != nil {
		t.Fatalf("Failed to create active portfolio A: %v", err)
	}

	// Portfolio B is Ideal
	portfolioBID, err := createTestPortfolio(pool, "BSKT Ideal B2", 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: nostbID, PercentageOrShares: 1.00},
		})
	if err != nil {
		t.Fatalf("Failed to create ideal portfolio B: %v", err)
	}

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupBasketTestRouter(pool, avClient)

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
		t.Fatalf("Compare endpoint returned %d: %s", w.Code, w.Body.String())
	}

	// Parse as raw map to check that "baskets" key is absent (omitempty)
	var rawResp map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &rawResp); err != nil {
		t.Fatalf("Failed to unmarshal raw response: %v", err)
	}

	if _, exists := rawResp["baskets"]; exists {
		t.Error("Expected 'baskets' field to be absent when portfolio A is not ideal")
	} else {
		t.Log("Confirmed: 'baskets' field absent when portfolio A is Active")
	}
}

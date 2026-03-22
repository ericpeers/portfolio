package tests

import (
	"math"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
)

// approxEq returns true if |a - b| < epsilon. Used to compare float64 dividend totals.
func approxEq(a, b float64) bool {
	return math.Abs(a-b) < 1e-6
}

// TestDividendsIdealVsActualNormalizedTo200 compares a 3-member ideal portfolio (A)
// against a 3-member actual portfolio (B). The actual portfolio starts at $200, so
// the ideal is normalized to $200. Dividend computation must use the normalized share
// counts — not the raw percentage values — to produce the correct total.
//
// Setup:
//   - DTID1 close=$10, DTID2 close=$20, DTID3 close=$40
//   - Actual B: 10 sh DTID1 + 3 sh DTID2 + 1 sh DTID3 → start value = $200
//   - Ideal A: 60% DTID1 + 25% DTID2 + 15% DTID3
//     → normalized to $200 → 12 sh / 2.5 sh / 0.75 sh
//   - Dividends within range: DTID1=$1.00/sh, DTID2=$2.00/sh, DTID3=$0.50/sh
//
// Expected dividends:
//   - A (ideal, normalized): 12×$1.00 + 2.5×$2.00 + 0.75×$0.50 = $17.375
//   - B (actual):           10×$1.00 + 3×$2.00  + 1×$0.50   = $16.50
func TestDividendsIdealVsActualNormalizedTo200(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// close = basePrice + 2, so use base 8/18/38 for close 10/20/40.
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dtid1, err := createTestSecurity(pool, "DTID1", "Dividend Test Ideal 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTID1: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTID1")

	dtid2, err := createTestSecurity(pool, "DTID2", "Dividend Test Ideal 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTID2: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTID2")

	dtid3, err := createTestSecurity(pool, "DTID3", "Dividend Test Ideal 3", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTID3: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTID3")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC) // Monday
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)  // Friday
	divDate := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC)   // Wednesday — dividend ex-date

	// basePrice 8 → close 10, basePrice 18 → close 20, basePrice 38 → close 40
	if err := insertPriceData(pool, dtid1, startDate, endDate, 8.0); err != nil {
		t.Fatalf("insertPriceData DTID1: %v", err)
	}
	if err := insertPriceData(pool, dtid2, startDate, endDate, 18.0); err != nil {
		t.Fatalf("insertPriceData DTID2: %v", err)
	}
	if err := insertPriceData(pool, dtid3, startDate, endDate, 38.0); err != nil {
		t.Fatalf("insertPriceData DTID3: %v", err)
	}

	// Insert one dividend event per security within the query range.
	if err := insertDividendEvent(pool, dtid1, divDate, 1.00); err != nil {
		t.Fatalf("insertDividendEvent DTID1: %v", err)
	}
	if err := insertDividendEvent(pool, dtid2, divDate, 2.00); err != nil {
		t.Fatalf("insertDividendEvent DTID2: %v", err)
	}
	if err := insertDividendEvent(pool, dtid3, divDate, 0.50); err != nil {
		t.Fatalf("insertDividendEvent DTID3: %v", err)
	}

	// Actual portfolio B: 10 sh×$10 + 3 sh×$20 + 1 sh×$40 = $200 at start.
	cleanupTestPortfolio(pool, "DT Ideal A", 1)
	cleanupTestPortfolio(pool, "DT Actual B", 1)
	defer cleanupTestPortfolio(pool, "DT Ideal A", 1)
	defer cleanupTestPortfolio(pool, "DT Actual B", 1)

	actualID, err := createTestPortfolio(pool, "DT Actual B", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: dtid1, PercentageOrShares: 10},
		{SecurityID: dtid2, PercentageOrShares: 3},
		{SecurityID: dtid3, PercentageOrShares: 1},
	})
	if err != nil {
		t.Fatalf("create actual portfolio: %v", err)
	}

	// Ideal portfolio A: 60/25/15 percentages.
	// Normalized to $200: DTID1→12sh, DTID2→2.5sh, DTID3→0.75sh.
	idealID, err := createTestPortfolio(pool, "DT Ideal A", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: dtid1, PercentageOrShares: 0.60},
		{SecurityID: dtid2, PercentageOrShares: 0.25},
		{SecurityID: dtid3, PercentageOrShares: 0.15},
	})
	if err != nil {
		t.Fatalf("create ideal portfolio: %v", err)
	}

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

	resp := callCompare(t, router, idealID, actualID, startDate, endDate)

	aMetrics := resp.PerformanceMetrics.PortfolioAMetrics
	bMetrics := resp.PerformanceMetrics.PortfolioBMetrics

	// The ideal portfolio must be normalized to the actual's start value ($200).
	if !approxEq(aMetrics.StartValue, 200.0) {
		t.Errorf("ideal start value: expected $200.00, got $%.4f — normalization may be broken", aMetrics.StartValue)
	}

	// Dividends for the normalized ideal: 12×1.00 + 2.5×2.00 + 0.75×0.50 = $17.375
	expectedDivA := 12.0*1.00 + 2.5*2.00 + 0.75*0.50
	if !approxEq(aMetrics.Dividends, expectedDivA) {
		t.Errorf("ideal dividends: expected $%.4f (share-based), got $%.4f — raw-percentage multiplication would give $%.4f",
			expectedDivA, aMetrics.Dividends, 0.60*1.00+0.25*2.00+0.15*0.50)
	}

	// Dividends for the actual portfolio: 10×1.00 + 3×2.00 + 1×0.50 = $16.50
	expectedDivB := 10.0*1.00 + 3.0*2.00 + 1.0*0.50
	if !approxEq(bMetrics.Dividends, expectedDivB) {
		t.Errorf("actual dividends: expected $%.4f, got $%.4f", expectedDivB, bMetrics.Dividends)
	}

	t.Logf("ideal dividends=$%.4f, actual dividends=$%.4f, ideal start value=$%.2f",
		aMetrics.Dividends, bMetrics.Dividends, aMetrics.StartValue)
}

// TestDividendsActualVsActualMultipleEvents compares two actual portfolios that each
// have two separate dividend ex-dates within the query range. The test verifies that
// both events per security are summed before being multiplied by share count.
//
// Portfolio A holds DTAA1 and DTAA2; portfolio B holds DTAA3 and DTAA4.
//
// Dividends:
//   - DTAA1 (10 sh): $0.50 on Jan 8 + $0.75 on Jan 15 → $12.50
//   - DTAA2 ( 5 sh): $1.00 on Jan 8 + $1.50 on Jan 15 → $12.50
//   - A total: $25.00
//   - DTAA3 ( 8 sh): $0.25 on Jan 8 + $0.35 on Jan 15 →  $4.80
//   - DTAA4 ( 3 sh): $2.00 on Jan 8 + $1.00 on Jan 15 →  $9.00
//   - B total: $13.80
func TestDividendsActualVsActualMultipleEvents(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	// close = base + 2: DTAA1→$10 (base 8), DTAA2→$20 (base 18), DTAA3→$15 (base 13), DTAA4→$30 (base 28)
	dtaa1, err := createTestSecurity(pool, "DTAA1", "Dividend Test AA 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTAA1: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTAA1")

	dtaa2, err := createTestSecurity(pool, "DTAA2", "Dividend Test AA 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTAA2: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTAA2")

	dtaa3, err := createTestSecurity(pool, "DTAA3", "Dividend Test AA 3", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTAA3: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTAA3")

	dtaa4, err := createTestSecurity(pool, "DTAA4", "Dividend Test AA 4", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTAA4: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTAA4")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)   // Monday
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)    // Friday (2 weeks)
	divDate1 := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC)    // Wednesday week 1
	divDate2 := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)   // Wednesday week 2

	for _, id := range []int64{dtaa1, dtaa2, dtaa3, dtaa4} {
		if err := insertPriceData(pool, id, startDate, endDate, 8.0); err != nil {
			t.Fatalf("insertPriceData id=%d: %v", id, err)
		}
	}

	// Two dividend events per security, two weeks apart.
	dividendSetup := []struct {
		id   int64
		div1 float64
		div2 float64
	}{
		{dtaa1, 0.50, 0.75},
		{dtaa2, 1.00, 1.50},
		{dtaa3, 0.25, 0.35},
		{dtaa4, 2.00, 1.00},
	}
	for _, d := range dividendSetup {
		if err := insertDividendEvent(pool, d.id, divDate1, d.div1); err != nil {
			t.Fatalf("insertDividendEvent id=%d date1: %v", d.id, err)
		}
		if err := insertDividendEvent(pool, d.id, divDate2, d.div2); err != nil {
			t.Fatalf("insertDividendEvent id=%d date2: %v", d.id, err)
		}
	}

	cleanupTestPortfolio(pool, "DT AA Portfolio A", 1)
	cleanupTestPortfolio(pool, "DT AA Portfolio B", 1)
	defer cleanupTestPortfolio(pool, "DT AA Portfolio A", 1)
	defer cleanupTestPortfolio(pool, "DT AA Portfolio B", 1)

	portAID, err := createTestPortfolio(pool, "DT AA Portfolio A", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: dtaa1, PercentageOrShares: 10},
		{SecurityID: dtaa2, PercentageOrShares: 5},
	})
	if err != nil {
		t.Fatalf("create portfolio A: %v", err)
	}

	portBID, err := createTestPortfolio(pool, "DT AA Portfolio B", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: dtaa3, PercentageOrShares: 8},
		{SecurityID: dtaa4, PercentageOrShares: 3},
	})
	if err != nil {
		t.Fatalf("create portfolio B: %v", err)
	}

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

	resp := callCompare(t, router, portAID, portBID, startDate, endDate)

	// A: 10×(0.50+0.75) + 5×(1.00+1.50) = 12.50 + 12.50 = $25.00
	expectedDivA := 10.0*(0.50+0.75) + 5.0*(1.00+1.50)
	if !approxEq(resp.PerformanceMetrics.PortfolioAMetrics.Dividends, expectedDivA) {
		t.Errorf("portfolio A dividends: expected $%.2f, got $%.4f", expectedDivA,
			resp.PerformanceMetrics.PortfolioAMetrics.Dividends)
	}

	// B: 8×(0.25+0.35) + 3×(2.00+1.00) = 4.80 + 9.00 = $13.80
	expectedDivB := 8.0*(0.25+0.35) + 3.0*(2.00+1.00)
	if !approxEq(resp.PerformanceMetrics.PortfolioBMetrics.Dividends, expectedDivB) {
		t.Errorf("portfolio B dividends: expected $%.2f, got $%.4f", expectedDivB,
			resp.PerformanceMetrics.PortfolioBMetrics.Dividends)
	}

	t.Logf("A dividends=$%.2f (expected $%.2f), B dividends=$%.2f (expected $%.2f)",
		resp.PerformanceMetrics.PortfolioAMetrics.Dividends, expectedDivA,
		resp.PerformanceMetrics.PortfolioBMetrics.Dividends, expectedDivB)
}

// TestDividendsNoDividendEvents verifies that portfolios with no entries in fact_event
// report zero dividends.
func TestDividendsNoDividendEvents(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dtnd1, err := createTestSecurity(pool, "DTND1", "Dividend Test No-Div 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTND1: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTND1")

	dtnd2, err := createTestSecurity(pool, "DTND2", "Dividend Test No-Div 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTND2: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTND2")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)

	if err := insertPriceData(pool, dtnd1, startDate, endDate, 50.0); err != nil {
		t.Fatalf("insertPriceData DTND1: %v", err)
	}
	if err := insertPriceData(pool, dtnd2, startDate, endDate, 50.0); err != nil {
		t.Fatalf("insertPriceData DTND2: %v", err)
	}
	// No dividend events inserted for either security.

	cleanupTestPortfolio(pool, "DT NoDivs A", 1)
	cleanupTestPortfolio(pool, "DT NoDivs B", 1)
	defer cleanupTestPortfolio(pool, "DT NoDivs A", 1)
	defer cleanupTestPortfolio(pool, "DT NoDivs B", 1)

	portAID, err := createTestPortfolio(pool, "DT NoDivs A", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: dtnd1, PercentageOrShares: 10},
	})
	if err != nil {
		t.Fatalf("create portfolio A: %v", err)
	}

	portBID, err := createTestPortfolio(pool, "DT NoDivs B", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: dtnd2, PercentageOrShares: 5},
	})
	if err != nil {
		t.Fatalf("create portfolio B: %v", err)
	}

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

	resp := callCompare(t, router, portAID, portBID, startDate, endDate)

	if resp.PerformanceMetrics.PortfolioAMetrics.Dividends != 0.0 {
		t.Errorf("expected $0.00 dividends for A (no events), got $%.4f",
			resp.PerformanceMetrics.PortfolioAMetrics.Dividends)
	}
	if resp.PerformanceMetrics.PortfolioBMetrics.Dividends != 0.0 {
		t.Errorf("expected $0.00 dividends for B (no events), got $%.4f",
			resp.PerformanceMetrics.PortfolioBMetrics.Dividends)
	}

	t.Log("No-dividend portfolios correctly report $0.00")
}

// TestDividendsOutsideQueryRange verifies that dividend events which fall before the
// start date or after the end date are excluded from the computed total.
//
// Events are inserted only on Dec 31 (before range) and Jan 13 (after range).
// The query window is Jan 6–10, so both events must be excluded → dividends = $0.
func TestDividendsOutsideQueryRange(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dtor1, err := createTestSecurity(pool, "DTOR1", "Dividend Test OOR 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTOR1: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTOR1")

	dtor2, err := createTestSecurity(pool, "DTOR2", "Dividend Test OOR 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTOR2: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTOR2")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)

	// Price data spans Dec 31 through Jan 17 so that the OOR event dates have DB coverage.
	priceStart := time.Date(2024, 12, 30, 0, 0, 0, 0, time.UTC)
	priceEnd := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)

	if err := insertPriceData(pool, dtor1, priceStart, priceEnd, 50.0); err != nil {
		t.Fatalf("insertPriceData DTOR1: %v", err)
	}
	if err := insertPriceData(pool, dtor2, priceStart, priceEnd, 50.0); err != nil {
		t.Fatalf("insertPriceData DTOR2: %v", err)
	}

	// Dividend before the query window.
	beforeRange := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	// Dividend after the query window.
	afterRange := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)

	if err := insertDividendEvent(pool, dtor1, beforeRange, 5.00); err != nil {
		t.Fatalf("insertDividendEvent DTOR1 before: %v", err)
	}
	if err := insertDividendEvent(pool, dtor1, afterRange, 5.00); err != nil {
		t.Fatalf("insertDividendEvent DTOR1 after: %v", err)
	}
	if err := insertDividendEvent(pool, dtor2, beforeRange, 3.00); err != nil {
		t.Fatalf("insertDividendEvent DTOR2 before: %v", err)
	}
	if err := insertDividendEvent(pool, dtor2, afterRange, 3.00); err != nil {
		t.Fatalf("insertDividendEvent DTOR2 after: %v", err)
	}

	cleanupTestPortfolio(pool, "DT OOR A", 1)
	cleanupTestPortfolio(pool, "DT OOR B", 1)
	defer cleanupTestPortfolio(pool, "DT OOR A", 1)
	defer cleanupTestPortfolio(pool, "DT OOR B", 1)

	portAID, err := createTestPortfolio(pool, "DT OOR A", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: dtor1, PercentageOrShares: 10},
	})
	if err != nil {
		t.Fatalf("create portfolio A: %v", err)
	}

	portBID, err := createTestPortfolio(pool, "DT OOR B", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: dtor2, PercentageOrShares: 10},
	})
	if err != nil {
		t.Fatalf("create portfolio B: %v", err)
	}

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

	resp := callCompare(t, router, portAID, portBID, startDate, endDate)

	if resp.PerformanceMetrics.PortfolioAMetrics.Dividends != 0.0 {
		t.Errorf("expected $0.00 dividends for A (events outside range), got $%.4f",
			resp.PerformanceMetrics.PortfolioAMetrics.Dividends)
	}
	if resp.PerformanceMetrics.PortfolioBMetrics.Dividends != 0.0 {
		t.Errorf("expected $0.00 dividends for B (events outside range), got $%.4f",
			resp.PerformanceMetrics.PortfolioBMetrics.Dividends)
	}

	t.Logf("Out-of-range dividend test: A=$%.2f, B=$%.2f (both expected $0.00)",
		resp.PerformanceMetrics.PortfolioAMetrics.Dividends,
		resp.PerformanceMetrics.PortfolioBMetrics.Dividends)
}

// TestDividendsIdealVsIdealBothNormalized verifies dividend computation when both
// portfolios are ideal (both normalized to $100). Despite having the same two securities,
// the portfolios differ in weight so their normalized share counts — and therefore their
// dividend totals — differ.
//
//   - DTII1 close=$10, DTII2 close=$20
//   - Ideal A: 70% DTII1 + 30% DTII2 → normalized to $100 →  7.0 sh / 1.5 sh
//   - Ideal B: 40% DTII1 + 60% DTII2 → normalized to $100 →  4.0 sh / 3.0 sh
//   - Dividend: DTII1=$1.00/sh, DTII2=$2.00/sh
//   - A dividends: 7.0×$1.00 + 1.5×$2.00 = $10.00
//   - B dividends: 4.0×$1.00 + 3.0×$2.00 =  $10.00
//
// Both total $10 in this case, which is a useful sanity check: different weights can
// yield equal totals, confirming the per-security arithmetic is being applied, not just
// the portfolio value multiplied by a flat rate.
func TestDividendsIdealVsIdealBothNormalized(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// close = base + 2: DTII1→$10 (base 8), DTII2→$20 (base 18)
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dtii1, err := createTestSecurity(pool, "DTII1", "Dividend Test II 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTII1: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTII1")

	dtii2, err := createTestSecurity(pool, "DTII2", "Dividend Test II 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create DTII2: %v", err)
	}
	defer cleanupTestSecurity(pool, "DTII2")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	divDate := time.Date(2025, 1, 8, 0, 0, 0, 0, time.UTC)

	if err := insertPriceData(pool, dtii1, startDate, endDate, 8.0); err != nil {
		t.Fatalf("insertPriceData DTII1: %v", err)
	}
	if err := insertPriceData(pool, dtii2, startDate, endDate, 18.0); err != nil {
		t.Fatalf("insertPriceData DTII2: %v", err)
	}

	if err := insertDividendEvent(pool, dtii1, divDate, 1.00); err != nil {
		t.Fatalf("insertDividendEvent DTII1: %v", err)
	}
	if err := insertDividendEvent(pool, dtii2, divDate, 2.00); err != nil {
		t.Fatalf("insertDividendEvent DTII2: %v", err)
	}

	cleanupTestPortfolio(pool, "DT II Ideal A", 1)
	cleanupTestPortfolio(pool, "DT II Ideal B", 1)
	defer cleanupTestPortfolio(pool, "DT II Ideal A", 1)
	defer cleanupTestPortfolio(pool, "DT II Ideal B", 1)

	// A: 70% DTII1 + 30% DTII2 → $70/$10=7sh, $30/$20=1.5sh
	idealAID, err := createTestPortfolio(pool, "DT II Ideal A", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: dtii1, PercentageOrShares: 0.70},
		{SecurityID: dtii2, PercentageOrShares: 0.30},
	})
	if err != nil {
		t.Fatalf("create ideal A: %v", err)
	}

	// B: 40% DTII1 + 60% DTII2 → $40/$10=4sh, $60/$20=3sh
	idealBID, err := createTestPortfolio(pool, "DT II Ideal B", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: dtii1, PercentageOrShares: 0.40},
		{SecurityID: dtii2, PercentageOrShares: 0.60},
	})
	if err != nil {
		t.Fatalf("create ideal B: %v", err)
	}

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

	resp := callCompare(t, router, idealAID, idealBID, startDate, endDate)

	// Two ideal portfolios → both normalized to $100.
	aMetrics := resp.PerformanceMetrics.PortfolioAMetrics
	bMetrics := resp.PerformanceMetrics.PortfolioBMetrics

	if !approxEq(aMetrics.StartValue, 100.0) {
		t.Errorf("ideal A start value: expected $100.00, got $%.4f", aMetrics.StartValue)
	}
	if !approxEq(bMetrics.StartValue, 100.0) {
		t.Errorf("ideal B start value: expected $100.00, got $%.4f", bMetrics.StartValue)
	}

	// A: 7.0×$1.00 + 1.5×$2.00 = $10.00
	expectedDivA := 7.0*1.00 + 1.5*2.00
	if !approxEq(aMetrics.Dividends, expectedDivA) {
		t.Errorf("ideal A dividends: expected $%.2f, got $%.4f", expectedDivA, aMetrics.Dividends)
	}

	// B: 4.0×$1.00 + 3.0×$2.00 = $10.00
	expectedDivB := 4.0*1.00 + 3.0*2.00
	if !approxEq(bMetrics.Dividends, expectedDivB) {
		t.Errorf("ideal B dividends: expected $%.2f, got $%.4f", expectedDivB, bMetrics.Dividends)
	}

	t.Logf("ideal-vs-ideal: A dividends=$%.2f, B dividends=$%.2f, both normalized to $100",
		aMetrics.Dividends, bMetrics.Dividends)
}

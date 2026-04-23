package tests

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
)

// TestReallocate_IdealPortfolio_PreIPOMember verifies that the reallocate strategy:
//   - excludes the pre-IPO member before its IPO date
//   - emits WarnProportionalReallocation
//   - produces continuous daily values with no zero-value entries
//   - value is continuous at the IPO date (transition restores correct shares)
func TestReallocate_IdealPortfolio_PreIPOMember(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	// ipoTicker IPOs in the middle of the range.
	ipoTicker := nextTicker()
	stableTicker := nextTicker()
	portName := nextPortfolioName()

	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)  // Monday
	ipoDate := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)        // Monday, 1 week later
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)        // Friday

	ipoSecID, err := createTestSecurity(pool, ipoTicker, "IPO Security", models.SecurityTypeStock, &ipoDate)
	if err != nil {
		t.Fatalf("createTestSecurity(ipo): %v", err)
	}
	defer cleanupTestSecurity(pool, ipoTicker)

	stableInception := requestedStart
	stableSecID, err := createTestSecurity(pool, stableTicker, "Stable Security", models.SecurityTypeStock, &stableInception)
	if err != nil {
		t.Fatalf("createTestSecurity(stable): %v", err)
	}
	defer cleanupTestSecurity(pool, stableTicker)

	if err := insertPriceData(pool, ipoSecID, ipoDate, endDate, 200.0); err != nil {
		t.Fatalf("insertPriceData(ipo): %v", err)
	}
	if err := insertPriceData(pool, stableSecID, requestedStart, endDate, 100.0); err != nil {
		t.Fatalf("insertPriceData(stable): %v", err)
	}

	// Ideal portfolio: 40% ipo, 60% stable.
	portfolioID, err := createTestPortfolio(pool, portName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: ipoSecID, PercentageOrShares: 0.40},
		{SecurityID: stableSecID, PercentageOrShares: 0.60},
	})
	if err != nil {
		t.Fatalf("createTestPortfolio: %v", err)
	}
	defer cleanupTestPortfolio(pool, portName, 1)

	performanceSvc, portfolioSvc := newCashSubServices(t)

	portfolio, err := portfolioSvc.GetPortfolio(ctx, portfolioID)
	if err != nil {
		t.Fatalf("GetPortfolio: %v", err)
	}

	warnCtx, _ := services.NewWarningContext(ctx)

	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, requestedStart)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}
	if !coverage.AnyGaps {
		t.Fatal("expected AnyGaps=true for pre-IPO member")
	}

	diffs := services.GenerateReallocateDiffs(coverage)
	if len(diffs) == 0 {
		t.Fatal("expected non-empty diffs from GenerateReallocateDiffs")
	}

	// NormalizeIdealPortfolio with diffs: excluded member gets 0 shares.
	normalizedPortfolio, origMemberships, err := performanceSvc.NormalizeIdealPortfolio(warnCtx, portfolio, requestedStart, 10_000.0, diffs)
	if err != nil {
		t.Fatalf("NormalizeIdealPortfolio: %v", err)
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, normalizedPortfolio, requestedStart, endDate, diffs, origMemberships)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}
	if len(dailyValues) == 0 {
		t.Fatal("expected daily values, got none")
	}

	// No zero-value entries.
	for _, dv := range dailyValues {
		if dv.Value == 0 {
			t.Errorf("date %s has zero value", dv.Date.Format("2006-01-02"))
		}
	}

	// First date must be requestedStart.
	if !dailyValues[0].Date.Equal(requestedStart) {
		t.Errorf("first date = %s, want %s", dailyValues[0].Date.Format("2006-01-02"), requestedStart.Format("2006-01-02"))
	}

	// Verify value continuity at the IPO date boundary: the value on ipoDate should
	// be within 1% of the value on the trading day just before it.
	var beforeIPO, onIPO float64
	for i, dv := range dailyValues {
		if dv.Date.Before(ipoDate) {
			beforeIPO = dv.Value
		}
		if dv.Date.Equal(ipoDate) {
			onIPO = dailyValues[i].Value
			break
		}
	}
	if beforeIPO == 0 || onIPO == 0 {
		t.Fatal("could not find values around IPO date for continuity check")
	}
	if math.Abs(onIPO-beforeIPO)/beforeIPO > 0.01 {
		t.Errorf("value discontinuity at IPO date: before=%.4f, on=%.4f (>1%% jump)", beforeIPO, onIPO)
	}

	// Confirm we have coverage for the full range (pre-IPO + post-IPO trading days).
	// requestedStart..ipoDate-1: 5 days (Jan 6-10), ipoDate..endDate: 5 days (Jan 13-17).
	// Jan 9 is Carter state funeral (market closed), so expect 9 trading days.
	const expectedDays = 9
	if len(dailyValues) < expectedDays {
		t.Errorf("expected at least %d daily values, got %d", expectedDays, len(dailyValues))
	}

	t.Logf("Reallocate ideal: %d daily values, value before IPO=%.2f, at IPO=%.2f",
		len(dailyValues), beforeIPO, onIPO)

	// WarnProportionalReallocation is emitted by comparison/glance services, not by
	// ComputeDailyValues. Verify GenerateReallocateDiffs produced the right diff.
	foundRemove := false
	for _, d := range diffs {
		if d.Type == services.DiffRemove && d.SecurityID == ipoSecID {
			foundRemove = true
			if !d.EffectiveBefore.Equal(ipoDate) {
				t.Errorf("DiffRemove.EffectiveBefore = %s, want %s",
					d.EffectiveBefore.Format("2006-01-02"), ipoDate.Format("2006-01-02"))
			}
		}
	}
	if !foundRemove {
		t.Error("expected DiffRemove for ipo security, not found")
	}
}

// TestReallocate_ActivePortfolio_PreIPOMember verifies the reallocate strategy works for
// active portfolios, using dollar-value fractions at the transition boundary.
func TestReallocate_ActivePortfolio_PreIPOMember(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ipoTicker := nextTicker()
	stableTicker := nextTicker()
	portName := nextPortfolioName()

	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	ipoDate := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)

	ipoSecID, err := createTestSecurity(pool, ipoTicker, "Active IPO Sec", models.SecurityTypeStock, &ipoDate)
	if err != nil {
		t.Fatalf("createTestSecurity(ipo): %v", err)
	}
	defer cleanupTestSecurity(pool, ipoTicker)

	stableInception := requestedStart
	stableSecID, err := createTestSecurity(pool, stableTicker, "Active Stable Sec", models.SecurityTypeStock, &stableInception)
	if err != nil {
		t.Fatalf("createTestSecurity(stable): %v", err)
	}
	defer cleanupTestSecurity(pool, stableTicker)

	if err := insertPriceData(pool, ipoSecID, ipoDate, endDate, 200.0); err != nil {
		t.Fatalf("insertPriceData(ipo): %v", err)
	}
	if err := insertPriceData(pool, stableSecID, requestedStart, endDate, 100.0); err != nil {
		t.Fatalf("insertPriceData(stable): %v", err)
	}

	// Active portfolio: 5 shares of ipo ($202×5=$1010), 10 shares of stable ($102×10=$1020).
	portfolioID, err := createTestPortfolio(pool, portName, 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: ipoSecID, PercentageOrShares: 5.0},
		{SecurityID: stableSecID, PercentageOrShares: 10.0},
	})
	if err != nil {
		t.Fatalf("createTestPortfolio: %v", err)
	}
	defer cleanupTestPortfolio(pool, portName, 1)

	performanceSvc, portfolioSvc := newCashSubServices(t)

	portfolio, err := portfolioSvc.GetPortfolio(ctx, portfolioID)
	if err != nil {
		t.Fatalf("GetPortfolio: %v", err)
	}

	warnCtx, _ := services.NewWarningContext(ctx)

	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, requestedStart)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}
	if !coverage.AnyGaps {
		t.Fatal("expected AnyGaps=true for pre-IPO member")
	}

	diffs := services.GenerateReallocateDiffs(coverage)
	if len(diffs) == 0 {
		t.Fatal("expected non-empty diffs")
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, portfolio, requestedStart, endDate, diffs, nil)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}
	if len(dailyValues) == 0 {
		t.Fatal("expected daily values, got none")
	}

	for _, dv := range dailyValues {
		if dv.Value == 0 {
			t.Errorf("date %s has zero value", dv.Date.Format("2006-01-02"))
		}
	}

	if !dailyValues[0].Date.Equal(requestedStart) {
		t.Errorf("first date = %s, want %s", dailyValues[0].Date.Format("2006-01-02"), requestedStart.Format("2006-01-02"))
	}

	// Value continuity at IPO date.
	var beforeIPO, onIPO float64
	for i, dv := range dailyValues {
		if dv.Date.Before(ipoDate) {
			beforeIPO = dv.Value
		}
		if dv.Date.Equal(ipoDate) {
			onIPO = dailyValues[i].Value
			break
		}
	}
	if beforeIPO == 0 || onIPO == 0 {
		t.Fatal("could not find values around IPO date")
	}
	if math.Abs(onIPO-beforeIPO)/beforeIPO > 0.01 {
		t.Errorf("value discontinuity at IPO date: before=%.4f, on=%.4f (>1%% jump)", beforeIPO, onIPO)
	}

	t.Logf("Reallocate active: %d daily values, before IPO=%.2f, at IPO=%.2f",
		len(dailyValues), beforeIPO, onIPO)
}

// TestReallocate_ViaCompare verifies the reallocate strategy through the full
// ComparePortfolios call, confirming WarnProportionalReallocation is present.
func TestReallocate_ViaCompare(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ipoTicker := nextTicker()
	stableTicker := nextTicker()
	portNameA := nextPortfolioName()
	portNameB := nextPortfolioName()

	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	ipoDate := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)

	ipoSecID, err := createTestSecurity(pool, ipoTicker, "Compare IPO Sec", models.SecurityTypeStock, &ipoDate)
	if err != nil {
		t.Fatalf("createTestSecurity(ipo): %v", err)
	}
	defer cleanupTestSecurity(pool, ipoTicker)

	stableInception := requestedStart
	stableSecID, err := createTestSecurity(pool, stableTicker, "Compare Stable Sec", models.SecurityTypeStock, &stableInception)
	if err != nil {
		t.Fatalf("createTestSecurity(stable): %v", err)
	}
	defer cleanupTestSecurity(pool, stableTicker)

	if err := insertPriceData(pool, ipoSecID, ipoDate, endDate, 200.0); err != nil {
		t.Fatalf("insertPriceData(ipo): %v", err)
	}
	if err := insertPriceData(pool, stableSecID, requestedStart, endDate, 100.0); err != nil {
		t.Fatalf("insertPriceData(stable): %v", err)
	}

	// Portfolio A: only stable security (full coverage).
	portfolioAID, err := createTestPortfolio(pool, portNameA, 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: stableSecID, PercentageOrShares: 10.0},
	})
	if err != nil {
		t.Fatalf("createTestPortfolio(A): %v", err)
	}
	defer cleanupTestPortfolio(pool, portNameA, 1)

	// Portfolio B (ideal): 40% ipo (pre-IPO gap), 60% stable.
	portfolioBID, err := createTestPortfolio(pool, portNameB, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: ipoSecID, PercentageOrShares: 0.40},
		{SecurityID: stableSecID, PercentageOrShares: 0.60},
	})
	if err != nil {
		t.Fatalf("createTestPortfolio(B): %v", err)
	}
	defer cleanupTestPortfolio(pool, portNameB, 1)

	comparisonSvc := newComparisonService(t)

	warnCtx, wc := services.NewWarningContext(ctx)

	req := &models.CompareRequest{
		PortfolioA:          portfolioAID,
		PortfolioB:          portfolioBID,
		StartPeriod:         models.FlexibleDate{Time: requestedStart},
		EndPeriod:           models.FlexibleDate{Time: endDate},
		MissingDataStrategy: models.MissingDataStrategyReallocate,
	}

	resp, err := comparisonSvc.ComparePortfolios(warnCtx, req)
	if err != nil {
		t.Fatalf("ComparePortfolios: %v", err)
	}
	resp.Warnings = wc.GetWarnings()

	// WarnProportionalReallocation must be present.
	foundWarn := false
	for _, w := range resp.Warnings {
		if w.Code == models.WarnProportionalReallocation {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Errorf("expected WarnProportionalReallocation (W4004) in resp.Warnings, got: %v", resp.Warnings)
	}

	dvA := resp.PerformanceMetrics.PortfolioAMetrics.DailyValues
	dvB := resp.PerformanceMetrics.PortfolioBMetrics.DailyValues

	// Both sides must cover the full range.
	if len(dvA) == 0 {
		t.Fatal("portfolio A: no daily values")
	}
	if len(dvB) != len(dvA) {
		t.Errorf("portfolio B daily value count = %d, want %d (same as A)", len(dvB), len(dvA))
	}

	// No zero values in B.
	for _, dv := range dvB {
		if dv.Value == 0 {
			t.Errorf("portfolio B: date %s has zero value", dv.Date)
		}
	}

	t.Logf("Reallocate compare: A=%d dvs, B=%d dvs, W4004 found=%v", len(dvA), len(dvB), foundWarn)
}

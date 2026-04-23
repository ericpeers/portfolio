package tests

// TestCashAppreciating_SnapshotSplit_CorrectSharesUsed mirrors
// TestReallocate_SnapshotSplit_ExistingMemberInflation but uses the
// cash_appreciating strategy. The scenario is identical:
//
//   Feb  2  startDate; snapshotted_at = April 1 (post-split)
//   Mar  2  TST1 IPOs — synthetic prices switch to real for cash_appreciating
//   Mar 16  TST2 2:1 forward split ($100 → $50)
//   Mar 19  TST2 price doubles ($50 → $100)
//   Apr  1  snapshotted_at (DB records post-split shares: TST2 = 10)
//
// Portfolio DB membership:
//
//	TST1: 2 shares (pre-IPO, no splits between IPO and snapshotted_at)
//	TST2: 10 shares (post 2:1 split recorded at snapshotted_at)
//	TST3: 5 shares  (no splits)
//
// Expected behavior for cash_appreciating (no recomputeSharesOnTransition):
//
//	Split reversal at init: sharesMap[TST2] = 10/2 = 5 (correct pre-split count)
//	TST1 has synthetic prices before March 2; 2 shares throughout.
//	Mar  2: TST1 switches to real price $200 — value-continuous, total ≈ $1,400
//	Mar 16: TST2 splits → 10 shares × $50; value-neutral, total ≈ $1,400
//	Mar 19: TST2 doubles to $100 → 10 × $100 = $1,000; total = $1,900
//
// If the split reversal bug affected cash_appreciating (sharesMap[TST2] not reversed):
//
//	Pre-split: TST2 = 10 × $100 = $1,000 (WRONG — should be 5 × $100 = $500)
//	After split forward loop: 20 × $50 = $1,000 (still inflated shares)
//	Mar 19: 20 × $100 = $2,000 (WRONG — should be $1,000)
//	Total: $400 + $2,000 + $500 = $2,900 instead of $1,900

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
)

func TestCashAppreciating_SnapshotSplit_CorrectSharesUsed(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	// --- Date constants (same as the reallocate regression test) ---
	startDate := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	ipoDate := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	splitDate := time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC)
	priceDouble := time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC)
	snapDate := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	// --- Securities ---
	tst1 := nextTicker()
	tst2 := nextTicker()
	tst3 := nextTicker()
	portName := nextPortfolioName()

	tst1ID, err := createTestSecurity(pool, tst1, "CashApp Pre-IPO 1", models.SecurityTypeStock, &ipoDate)
	if err != nil {
		t.Fatalf("createTestSecurity(tst1): %v", err)
	}
	defer cleanupTestSecurity(pool, tst1)

	inception2 := startDate
	tst2ID, err := createTestSecurity(pool, tst2, "CashApp Stable 2", models.SecurityTypeStock, &inception2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst2): %v", err)
	}
	defer cleanupTestSecurity(pool, tst2)

	inception3 := startDate
	tst3ID, err := createTestSecurity(pool, tst3, "CashApp Stable 3", models.SecurityTypeStock, &inception3)
	if err != nil {
		t.Fatalf("createTestSecurity(tst3): %v", err)
	}
	defer cleanupTestSecurity(pool, tst3)

	// --- Prices (identical to the reallocate test) ---
	insertPriceRows(t, pool, tst1ID, weekdayPrices(ipoDate, endDate, 200.0))

	tst2Prices := mergePrices(
		weekdayPrices(startDate, time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC), 100.0),
		weekdayPrices(splitDate, time.Date(2026, 3, 18, 0, 0, 0, 0, time.UTC), 50.0),
		weekdayPrices(priceDouble, endDate, 100.0),
	)
	insertPriceRows(t, pool, tst2ID, tst2Prices)
	insertPriceRows(t, pool, tst3ID, weekdayPrices(startDate, endDate, 100.0))

	if err := insertSplitEvent(pool, tst2ID, splitDate, 2.0); err != nil {
		t.Fatalf("insertSplitEvent(tst2): %v", err)
	}

	// --- Portfolio (same DB membership as reallocate test) ---
	cleanupTestPortfolio(pool, portName, 1)
	defer cleanupTestPortfolio(pool, portName, 1)

	portfolioID, err := createTestPortfolio(pool, portName, 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: tst1ID, PercentageOrShares: 2.0},
		{SecurityID: tst2ID, PercentageOrShares: 10.0},
		{SecurityID: tst3ID, PercentageOrShares: 5.0},
	})
	if err != nil {
		t.Fatalf("createTestPortfolio: %v", err)
	}

	_, err = pool.Exec(ctx, `UPDATE portfolio SET snapshotted_at = $1 WHERE id = $2`, snapDate, portfolioID)
	if err != nil {
		t.Fatalf("set snapshotted_at: %v", err)
	}

	// --- Services ---
	performanceSvc, portfolioSvc := newCashSubServices(t)
	portfolio, err := portfolioSvc.GetPortfolio(ctx, portfolioID)
	if err != nil {
		t.Fatalf("GetPortfolio: %v", err)
	}

	warnCtx, _ := services.NewWarningContext(ctx)

	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, startDate)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}
	if !coverage.AnyGaps {
		t.Fatal("expected AnyGaps=true — TST1 is pre-IPO")
	}

	diffs, err := performanceSvc.SynthesizeCashPrices(warnCtx, coverage, models.MissingDataStrategyCashAppreciating)
	if err != nil {
		t.Fatalf("SynthesizeCashPrices: %v", err)
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, portfolio, startDate, endDate, diffs, nil)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}

	// --- Assertions ---

	// Before IPO: TST2 (split-reversed → 5 shares × $100) + TST3 (5 × $100) = $1,000
	// plus TST1 synthetic ≈ 2 × ~$199 ≈ $399. Total ≈ $1,399.
	// We allow a wide band for the TST1 synthetic component (treasury rate uncertainty).
	preMar2Val, okPreMar2 := dvFor(dailyValues, time.Date(2026, 2, 27, 0, 0, 0, 0, time.UTC))
	if !okPreMar2 {
		t.Fatal("no value for Feb 27 (pre-IPO day)")
	}
	// TST2 (5×$100=500) + TST3 (5×$100=500) = $1,000. TST1 synthetic ≈ $399.
	// If split reversal were broken: TST2 = 10×$100=$1,000; total ≈ $1,900.
	if preMar2Val > 1500 {
		t.Errorf("Feb 27 value=%.2f: appears too high — TST2 may not have had split reversal applied "+
			"(10 shares instead of 5 would give ~$1900 pre-IPO)", preMar2Val)
	}
	if preMar2Val < 1300 {
		t.Errorf("Feb 27 value=%.2f: appears too low — expected ~$1399 (TST1≈$399 + TST2/TST3=$1000)", preMar2Val)
	}

	// At IPO (March 2): TST1 switches to real price $200 — must be value-continuous.
	// cash_appreciating does NOT call recomputeSharesOnTransition, so no dilution.
	// Expected: 2×$200 + 5×$100 + 5×$100 = $1,400 exactly.
	mar2Val, okMar2 := dvFor(dailyValues, ipoDate)
	if !okMar2 {
		t.Fatal("no value for March 2 (IPO date)")
	}
	if math.Abs(mar2Val-1400.0) > 1.0 {
		t.Errorf("March 2 (IPO): value=%.4f, want ≈1400.00 "+
			"(TST1 2×$200 + TST2 5×$100 + TST3 5×$100)", mar2Val)
	}

	// At split (March 16): TST2 5 shares → 10 shares at $50 — value-neutral.
	// Expected: 2×$200 + 10×$50 + 5×$100 = $400+$500+$500 = $1,400.
	mar16Val, okMar16 := dvFor(dailyValues, splitDate)
	if !okMar16 {
		t.Fatal("no value for March 16 (split date)")
	}
	if math.Abs(mar16Val-1400.0) > 0.01 {
		t.Errorf("March 16 (split): value=%.4f, want 1400.00 (value-neutral split)", mar16Val)
	}

	// March 19: TST2 doubles post-split to $100.
	// CORRECT: 2×$200 + 10×$100 + 5×$100 = $400+$1000+$500 = $1,900.
	// WRONG (split reversal broken): 2×$200 + 20×$100 + 5×$100 = $400+$2000+$500 = $2,900.
	const wantMar19 = 1900.0
	const wrongMar19 = 2900.0

	mar19Val, okMar19 := dvFor(dailyValues, priceDouble)
	if !okMar19 {
		t.Fatal("no value for March 19")
	}
	if math.Abs(mar19Val-wantMar19) > 1.0 {
		t.Errorf("March 19 value=%.4f, want ≈%.4f (10 post-split shares × $100); "+
			"pre-reversal-fix value would be ≈%.4f (20 shares from un-reversed init)",
			mar19Val, wantMar19, wrongMar19)
	}
	t.Logf("CashAppreciating_SnapshotSplit: feb27=%.2f, mar2=%.2f, mar16=%.2f, mar19=%.4f (want≈%.4f, wrong≈%.4f)",
		preMar2Val, mar2Val, mar16Val, mar19Val, wantMar19, wrongMar19)
}

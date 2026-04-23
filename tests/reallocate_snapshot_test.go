package tests

// TestReallocate_SnapshotSplit_ExistingMemberInflation is a regression test for the
// bug where recomputeSharesOnTransition used m.PercentageOrShares (post-snapshot DB
// value) instead of sharesMap (correctly split-reversed) for existing members.
//
// Timeline (2026):
//
//	Feb  2  startDate; snapshotted_at = April 1 (post-split)
//	        sharesMap init: TST2 = 10 / 2 = 5 (2:1 split on March 16 reversed)
//	Mar  2  TST1 IPOs — recomputeSharesOnTransition fires
//	Mar 16  TST2 2:1 forward split ($100 → $50); forward loop applies it
//	Mar 19  TST2 price doubles ($50 → $100) — value diverges between fix and no-fix
//	Mar 20  endDate
//	Apr  1  snapshotted_at (DB records post-split shares: TST2 = 10)
//
// Portfolio DB membership (m.PercentageOrShares = post-snapshot/post-split counts):
//
//	TST1: 2 shares (pre-IPO, no splits)
//	TST2: 10 shares (post 2:1 split)
//	TST3: 5 shares (no splits)
//
// At March 2 transition — CORRECT (sharesMap-based):
//
//	totalDollar = 5×$100 + 5×$100 + 2×$200 = $1,400
//	shares[TST2] = 1000 × (500/1400) / 100 = 25/7  ≈ 3.571
//	shares[TST3] = 25/7
//	shares[TST1] = 1000 × (400/1400) / 200 = 10/7  ≈ 1.429
//
// At March 2 transition — WRONG (m.PercentageOrShares, pre-fix):
//
//	totalDollar = 10×$100 + 5×$100 + 2×$200 = $1,900   (TST2 post-split count × pre-split price)
//	shares[TST2] = 1000 × (1000/1900) / 100 = 100/19 ≈ 5.263
//	shares[TST3] = 1000 × (500/1900)  / 100 =  50/19 ≈ 2.632
//	shares[TST1] = 1000 × (400/1900)  / 200 =  20/19 ≈ 1.053
//
// After March 16 split (forward loop doubles TST2 shares):
//
//	CORRECT: 7.143 × $50 + 3.571 × $100 + 1.429 × $200 = $1,000 (value-neutral)
//	WRONG:   10.526 × $50 + 2.632 × $100 + 1.053 × $200 = $1,000 (also value-neutral)
//
// On March 19 (TST2 doubles post-split to $100):
//
//	CORRECT: (50/7)×$100 + (25/7)×$100 + (10/7)×$200 = 9500/7 ≈ $1,357.14
//	WRONG:   (200/19)×$100 + (50/19)×$100 + (20/19)×$200 = 29000/19 ≈ $1,526.32

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
)

func TestReallocate_SnapshotSplit_ExistingMemberInflation(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	// --- Date constants ---
	startDate := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)  // Monday
	ipoDate := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)    // Monday — TST1 IPO
	splitDate := time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC) // Monday — TST2 2:1 split
	priceDouble := time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC) // Thursday — TST2 doubles post-split
	endDate := time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC)   // Friday
	snapDate := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)   // Wednesday — snapshotted_at (post-split)

	// --- Securities ---
	tst1 := nextTicker()
	tst2 := nextTicker()
	tst3 := nextTicker()
	portName := nextPortfolioName()

	// TST1: pre-IPO (no price data before ipoDate)
	tst1ID, err := createTestSecurity(pool, tst1, "SnapSplit Pre-IPO 1", models.SecurityTypeStock, &ipoDate)
	if err != nil {
		t.Fatalf("createTestSecurity(tst1): %v", err)
	}
	defer cleanupTestSecurity(pool, tst1)

	// TST2: 2:1 forward split on March 16; m.PercentageOrShares = 10 (post-split count)
	inception2 := startDate
	tst2ID, err := createTestSecurity(pool, tst2, "SnapSplit Stable 2", models.SecurityTypeStock, &inception2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst2): %v", err)
	}
	defer cleanupTestSecurity(pool, tst2)

	// TST3: no splits; m.PercentageOrShares = 5
	inception3 := startDate
	tst3ID, err := createTestSecurity(pool, tst3, "SnapSplit Stable 3", models.SecurityTypeStock, &inception3)
	if err != nil {
		t.Fatalf("createTestSecurity(tst3): %v", err)
	}
	defer cleanupTestSecurity(pool, tst3)

	// --- Prices ---
	// TST1: $200 from IPO date
	insertPriceRows(t, pool, tst1ID, weekdayPrices(ipoDate, endDate, 200.0))

	// TST2: $100 pre-split, $50 on split day, $50 after split until March 18, $100 from March 19
	tst2Prices := mergePrices(
		weekdayPrices(startDate, time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC), 100.0),
		weekdayPrices(splitDate, time.Date(2026, 3, 18, 0, 0, 0, 0, time.UTC), 50.0),
		weekdayPrices(priceDouble, endDate, 100.0),
	)
	insertPriceRows(t, pool, tst2ID, tst2Prices)

	// TST3: $100 throughout
	insertPriceRows(t, pool, tst3ID, weekdayPrices(startDate, endDate, 100.0))

	// --- Split event ---
	if err := insertSplitEvent(pool, tst2ID, splitDate, 2.0); err != nil {
		t.Fatalf("insertSplitEvent(tst2): %v", err)
	}

	// --- Portfolio ---
	// m.PercentageOrShares = post-snapshot/post-split DB values:
	//   TST2 = 10 (post 2:1 split), TST3 = 5, TST1 = 2 (no splits)
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

	// Set snapshotted_at = April 1 (after the March 16 split)
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

	// --- Data coverage → reallocate diffs ---
	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, startDate)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}
	if !coverage.AnyGaps {
		t.Fatal("expected AnyGaps=true — TST1 is pre-IPO")
	}

	diffs := services.GenerateReallocateDiffs(coverage)

	// --- Compute daily values ---
	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, portfolio, startDate, endDate, diffs, nil)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}

	// --- Assertions ---

	// Before IPO: only TST2 and TST3. Split reversal gives sharesMap[TST2]=5.
	// Value = 5×$100 + 5×$100 = $1,000.
	preMar2Val, okPreMar2 := dvFor(dailyValues, time.Date(2026, 2, 27, 0, 0, 0, 0, time.UTC))
	if !okPreMar2 {
		t.Fatal("no value for Feb 27 (pre-IPO day)")
	}
	if math.Abs(preMar2Val-1000.0) > 0.01 {
		t.Errorf("Feb 27 value=%.4f, want 1000.00 (5 shares TST2 × $100 + 5 shares TST3 × $100)", preMar2Val)
	}

	// At IPO transition (March 2): value must be continuous ($1,000).
	mar2Val, okMar2 := dvFor(dailyValues, ipoDate)
	if !okMar2 {
		t.Fatal("no value for March 2 (IPO date)")
	}
	if math.Abs(mar2Val-1000.0) > 0.01 {
		t.Errorf("March 2 (IPO): value=%.4f, want 1000.00 (value-continuous)", mar2Val)
	}

	// At split (March 16): value must remain $1,000 (split is price-neutral).
	mar16Val, okMar16 := dvFor(dailyValues, splitDate)
	if !okMar16 {
		t.Fatal("no value for March 16 (split date)")
	}
	if math.Abs(mar16Val-1000.0) > 0.01 {
		t.Errorf("March 16 (split): value=%.4f, want 1000.00 (value-neutral split)", mar16Val)
	}

	// March 19: TST2 doubles from $50 to $100 post-split.
	// Correct (fix): (50/7)×$100 + (25/7)×$100 + (10/7)×$200 = 9500/7 ≈ $1,357.14
	// Wrong (pre-fix): (200/19)×$100 + (50/19)×$100 + (20/19)×$200 = 29000/19 ≈ $1,526.32
	const wantMar19 = 9500.0 / 7.0   // ≈ 1357.14
	const wrongMar19 = 29000.0 / 19.0 // ≈ 1526.32

	mar19Val, okMar19 := dvFor(dailyValues, priceDouble)
	if !okMar19 {
		t.Fatal("no value for March 19")
	}
	if math.Abs(mar19Val-wantMar19) > 1.0 {
		t.Errorf("March 19 value=%.4f, want ≈%.4f (sharesMap-based fractions at IPO transition); "+
			"pre-fix value would be ≈%.4f (inflated TST2 fraction from m.PercentageOrShares)",
			mar19Val, wantMar19, wrongMar19)
	}
	t.Logf("SnapshotSplitRegression: feb27=%.2f, mar2=%.2f, mar16=%.2f, mar19=%.4f (want≈%.4f, wrong≈%.4f)",
		preMar2Val, mar2Val, mar16Val, mar19Val, wantMar19, wrongMar19)
}

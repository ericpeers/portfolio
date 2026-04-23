package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
)

// weekdayPrices returns a map of date→price for every weekday in [start, end].
func weekdayPrices(start, end time.Time, price float64) map[time.Time]float64 {
	m := make(map[time.Time]float64)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		if d.Weekday() != time.Saturday && d.Weekday() != time.Sunday {
			m[d] = price
		}
	}
	return m
}

// mergePrices merges price maps, with later maps overwriting earlier ones.
func mergePrices(maps ...map[time.Time]float64) map[time.Time]float64 {
	out := make(map[time.Time]float64)
	for _, m := range maps {
		for d, p := range m {
			out[d] = p
		}
	}
	return out
}

// dvFor returns the DailyValue for the given date, or (0, false) if not found.
func dvFor(dailyValues []services.DailyValue, date time.Time) (float64, bool) {
	for _, dv := range dailyValues {
		if dv.Date.Equal(date) {
			return dv.Value, true
		}
	}
	return 0, false
}

// Scenario constants shared across all tests in this file.
var (
	mathFeb2  = time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)  // Monday — requestedStart
	mathMar2  = time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)  // Monday — IPO date
	mathMar3  = time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)  // Tuesday
	mathMar4  = time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC)  // Wednesday
	mathMar5  = time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)  // Thursday
	mathMar6  = time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)  // Friday
	mathMar20 = time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC) // Friday — endDate
	mathFeb27 = time.Date(2026, 2, 27, 0, 0, 0, 0, time.UTC) // Friday — last trading day before IPO
)

// TestReallocate_PreIPO_ValueOnlyManagedLegs asserts that on every trading day before
// the IPO date, the portfolio value equals exactly the $10,000 normalized start value.
// This proves TST1 contributes $0 pre-IPO and the available weights normalise to 50/50.
func TestReallocate_PreIPO_ValueOnlyManagedLegs(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	tst1 := nextTicker()
	tst2 := nextTicker()
	tst3 := nextTicker()
	portName := nextPortfolioName()

	tst1ID, err := createTestSecurity(pool, tst1, "Math IPO 1", models.SecurityTypeStock, &mathMar2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst1): %v", err)
	}
	defer cleanupTestSecurity(pool, tst1)

	tst2ID, err := createTestSecurity(pool, tst2, "Math Stable 2", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst2): %v", err)
	}
	defer cleanupTestSecurity(pool, tst2)

	tst3ID, err := createTestSecurity(pool, tst3, "Math Stable 3", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst3): %v", err)
	}
	defer cleanupTestSecurity(pool, tst3)

	insertPriceRows(t, pool, tst1ID, weekdayPrices(mathMar2, mathMar20, 200.0))
	insertPriceRows(t, pool, tst2ID, weekdayPrices(mathFeb2, mathMar20, 100.0))
	insertPriceRows(t, pool, tst3ID, weekdayPrices(mathFeb2, mathMar20, 100.0))

	portfolioID, err := createTestPortfolio(pool, portName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: tst1ID, PercentageOrShares: 1.0 / 3.0},
		{SecurityID: tst2ID, PercentageOrShares: 1.0 / 3.0},
		{SecurityID: tst3ID, PercentageOrShares: 1.0 / 3.0},
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

	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, mathFeb2)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}
	if !coverage.AnyGaps {
		t.Fatal("expected AnyGaps=true for pre-IPO member")
	}

	diffs := services.GenerateReallocateDiffs(coverage)
	normalizedPortfolio, origMemberships, err := performanceSvc.NormalizeIdealPortfolio(warnCtx, portfolio, mathFeb2, 10_000.0, diffs)
	if err != nil {
		t.Fatalf("NormalizeIdealPortfolio: %v", err)
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, normalizedPortfolio, mathFeb2, mathMar20, diffs, origMemberships)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}
	if len(dailyValues) == 0 {
		t.Fatal("expected daily values, got none")
	}

	preIPOCount := 0
	for _, dv := range dailyValues {
		if dv.Date.Before(mathMar2) {
			preIPOCount++
			if math.Abs(dv.Value-10_000.0) > 0.01 {
				t.Errorf("pre-IPO %s: value=%.4f, want 10000.00 (TST1 should contribute $0)",
					dv.Date.Format("2006-01-02"), dv.Value)
			}
		}
	}
	if preIPOCount == 0 {
		t.Error("no pre-IPO daily values found")
	}
	t.Logf("pre-IPO days checked: %d; all ≈ $10,000", preIPOCount)
}

// TestReallocate_Transition_IdenticalPrices_NoValueJump asserts that when TST2 and TST3
// have the same price on both sides of the IPO date, portfolio value is identical
// (within $0.01) on the last pre-IPO trading day and on the IPO date itself.
// This proves the reallocation does not introduce a spurious step-change.
func TestReallocate_Transition_IdenticalPrices_NoValueJump(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	tst1 := nextTicker()
	tst2 := nextTicker()
	tst3 := nextTicker()
	portName := nextPortfolioName()

	tst1ID, err := createTestSecurity(pool, tst1, "NoJump IPO 1", models.SecurityTypeStock, &mathMar2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst1): %v", err)
	}
	defer cleanupTestSecurity(pool, tst1)

	tst2ID, err := createTestSecurity(pool, tst2, "NoJump Stable 2", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst2): %v", err)
	}
	defer cleanupTestSecurity(pool, tst2)

	tst3ID, err := createTestSecurity(pool, tst3, "NoJump Stable 3", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst3): %v", err)
	}
	defer cleanupTestSecurity(pool, tst3)

	// All flat prices — TST2 and TST3 are $100 on both sides of the transition.
	insertPriceRows(t, pool, tst1ID, weekdayPrices(mathMar2, mathMar20, 200.0))
	insertPriceRows(t, pool, tst2ID, weekdayPrices(mathFeb2, mathMar20, 100.0))
	insertPriceRows(t, pool, tst3ID, weekdayPrices(mathFeb2, mathMar20, 100.0))

	portfolioID, err := createTestPortfolio(pool, portName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: tst1ID, PercentageOrShares: 1.0 / 3.0},
		{SecurityID: tst2ID, PercentageOrShares: 1.0 / 3.0},
		{SecurityID: tst3ID, PercentageOrShares: 1.0 / 3.0},
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

	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, mathFeb2)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}

	diffs := services.GenerateReallocateDiffs(coverage)
	normalizedPortfolio, origMemberships, err := performanceSvc.NormalizeIdealPortfolio(warnCtx, portfolio, mathFeb2, 10_000.0, diffs)
	if err != nil {
		t.Fatalf("NormalizeIdealPortfolio: %v", err)
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, normalizedPortfolio, mathFeb2, mathMar20, diffs, origMemberships)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}

	beforeIPO, okBefore := dvFor(dailyValues, mathFeb27)
	onIPO, okOn := dvFor(dailyValues, mathMar2)
	if !okBefore || !okOn {
		t.Fatalf("could not find values: feb27=%v (%v), mar2=%v (%v)", beforeIPO, okBefore, onIPO, okOn)
	}

	if math.Abs(onIPO-beforeIPO) > 0.01 {
		t.Errorf("value jump at IPO: feb27=%.4f, mar2=%.4f (diff=%.4f, want < $0.01)",
			beforeIPO, onIPO, onIPO-beforeIPO)
	}
	t.Logf("NoValueJump: feb27=%.4f, mar2=%.4f, diff=%.6f", beforeIPO, onIPO, onIPO-beforeIPO)
}

// TestReallocate_Transition_PriceChange_UsesCurrentPrices verifies that
// recomputeSharesOnTransition uses the IPO date's prices (not the prior day's prices)
// when computing new share counts. We detect this by changing TST2's price between
// Feb 27 and March 2, then doubling it on March 3 — the resulting value differs by
// roughly $3,333 depending on which price was used for the shares calculation.
func TestReallocate_Transition_PriceChange_UsesCurrentPrices(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	tst1 := nextTicker()
	tst2 := nextTicker()
	tst3 := nextTicker()
	portName := nextPortfolioName()

	tst1ID, err := createTestSecurity(pool, tst1, "PriceChg IPO 1", models.SecurityTypeStock, &mathMar2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst1): %v", err)
	}
	defer cleanupTestSecurity(pool, tst1)

	tst2ID, err := createTestSecurity(pool, tst2, "PriceChg Stable 2", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst2): %v", err)
	}
	defer cleanupTestSecurity(pool, tst2)

	tst3ID, err := createTestSecurity(pool, tst3, "PriceChg Stable 3", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst3): %v", err)
	}
	defer cleanupTestSecurity(pool, tst3)

	// TST2: $100 pre-IPO, jumps to $150 on March 2, doubles to $300 on March 3.
	// TST3: $100 throughout. TST1: $200 throughout.
	//
	// If shares are computed with March 2 prices (correct):
	//   shares[TST2] = 10000/3 / 150 = 22.222
	//   March 3 value = 10000/3/200*200 + 22.222*300 + 10000/3/100*100 = 3333 + 6667 + 3333 = $13,333
	//
	// If shares were computed with Feb 27 prices (WRONG):
	//   shares[TST2] = 10000/3 / 100 = 33.333
	//   March 3 value = 3333 + 33.333*300 + 3333 = 3333 + 10000 + 3333 = $16,666
	tst2Prices := mergePrices(
		weekdayPrices(mathFeb2, mathFeb27, 100.0),
		map[time.Time]float64{
			mathMar2: 150.0,
			mathMar3: 300.0,
			mathMar4: 300.0,
			mathMar5: 300.0,
			mathMar6: 300.0,
		},
	)
	insertPriceRows(t, pool, tst1ID, weekdayPrices(mathMar2, mathMar20, 200.0))
	insertPriceRows(t, pool, tst2ID, tst2Prices)
	insertPriceRows(t, pool, tst3ID, weekdayPrices(mathFeb2, mathMar20, 100.0))

	portfolioID, err := createTestPortfolio(pool, portName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: tst1ID, PercentageOrShares: 1.0 / 3.0},
		{SecurityID: tst2ID, PercentageOrShares: 1.0 / 3.0},
		{SecurityID: tst3ID, PercentageOrShares: 1.0 / 3.0},
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
	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, mathFeb2)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}

	diffs := services.GenerateReallocateDiffs(coverage)
	normalizedPortfolio, origMemberships, err := performanceSvc.NormalizeIdealPortfolio(warnCtx, portfolio, mathFeb2, 10_000.0, diffs)
	if err != nil {
		t.Fatalf("NormalizeIdealPortfolio: %v", err)
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, normalizedPortfolio, mathFeb2, mathMar20, diffs, origMemberships)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}

	// Compute expected: shares[TST2] = 10000/3/150, March 3 = that*300 + 10000/3/200*200 + 10000/3/100*100
	const target = 10_000.0
	sharesTST1 := target / 3.0 / 200.0
	sharesTST2correct := target / 3.0 / 150.0 // uses March 2 price
	sharesTST2wrong := target / 3.0 / 100.0   // uses Feb 27 price
	sharesTST3 := target / 3.0 / 100.0
	wantMar3 := sharesTST1*200.0 + sharesTST2correct*300.0 + sharesTST3*100.0
	wrongMar3 := sharesTST1*200.0 + sharesTST2wrong*300.0 + sharesTST3*100.0

	mar3Val, ok := dvFor(dailyValues, mathMar3)
	if !ok {
		t.Fatal("no daily value for March 3")
	}
	if math.Abs(mar3Val-wantMar3) > 1.0 {
		t.Errorf("March 3 value=%.2f; want ≈%.2f (correct, using Mar 2 prices); wrong answer would be ≈%.2f",
			mar3Val, wantMar3, wrongMar3)
	}
	t.Logf("PriceChange: mar3=%.2f, want≈%.2f (correct), wrong≈%.2f", mar3Val, wantMar3, wrongMar3)
}

// TestReallocate_PostIPO_FlatPortfolio_CountervailingReturns verifies that when
// TST1 doubles (+100%) and TST2/TST3 halve (-50%) on the same day, the equal-thirds
// portfolio remains flat. This would fail if recomputeSharesOnTransition set incorrect
// share counts on the transition day.
func TestReallocate_PostIPO_FlatPortfolio_CountervailingReturns(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	tst1 := nextTicker()
	tst2 := nextTicker()
	tst3 := nextTicker()
	portName := nextPortfolioName()

	tst1ID, err := createTestSecurity(pool, tst1, "Flat IPO 1", models.SecurityTypeStock, &mathMar2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst1): %v", err)
	}
	defer cleanupTestSecurity(pool, tst1)

	tst2ID, err := createTestSecurity(pool, tst2, "Flat Stable 2", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst2): %v", err)
	}
	defer cleanupTestSecurity(pool, tst2)

	tst3ID, err := createTestSecurity(pool, tst3, "Flat Stable 3", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst3): %v", err)
	}
	defer cleanupTestSecurity(pool, tst3)

	// Transition at March 2: TST1=$200, TST2=$100, TST3=$100.
	// March 3: TST1=$400 (+100%), TST2=$50 (-50%), TST3=$50 (-50%).
	// Expected: 1/3*2 + 1/3*0.5 + 1/3*0.5 = 1.0 → flat at $10,000.
	tst1Prices := mergePrices(
		map[time.Time]float64{mathMar2: 200.0},
		weekdayPrices(mathMar3, mathMar20, 400.0),
	)
	tst2Prices := mergePrices(
		weekdayPrices(mathFeb2, mathMar2, 100.0),
		weekdayPrices(mathMar3, mathMar20, 50.0),
	)
	tst3Prices := mergePrices(
		weekdayPrices(mathFeb2, mathMar2, 100.0),
		weekdayPrices(mathMar3, mathMar20, 50.0),
	)
	insertPriceRows(t, pool, tst1ID, tst1Prices)
	insertPriceRows(t, pool, tst2ID, tst2Prices)
	insertPriceRows(t, pool, tst3ID, tst3Prices)

	portfolioID, err := createTestPortfolio(pool, portName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: tst1ID, PercentageOrShares: 1.0 / 3.0},
		{SecurityID: tst2ID, PercentageOrShares: 1.0 / 3.0},
		{SecurityID: tst3ID, PercentageOrShares: 1.0 / 3.0},
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
	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, mathFeb2)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}

	diffs := services.GenerateReallocateDiffs(coverage)
	normalizedPortfolio, origMemberships, err := performanceSvc.NormalizeIdealPortfolio(warnCtx, portfolio, mathFeb2, 10_000.0, diffs)
	if err != nil {
		t.Fatalf("NormalizeIdealPortfolio: %v", err)
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, normalizedPortfolio, mathFeb2, mathMar20, diffs, origMemberships)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}

	// On March 2 (transition): shares computed at $200/$100/$100 from budget of $10,000
	//   TST1=10000/3/200=16.667, TST2=10000/3/100=33.333, TST3=33.333 → value=$10,000
	// On March 3: 16.667×400 + 33.333×50 + 33.333×50 = 6666.8 + 1666.65 + 1666.65 = $10,000
	mar2Val, okMar2 := dvFor(dailyValues, mathMar2)
	mar3Val, okMar3 := dvFor(dailyValues, mathMar3)
	if !okMar2 || !okMar3 {
		t.Fatalf("missing values: mar2=%v(%v), mar3=%v(%v)", mar2Val, okMar2, mar3Val, okMar3)
	}

	if math.Abs(mar2Val-10_000.0) > 0.10 {
		t.Errorf("March 2 value=%.4f, want 10000.00 (±0.10)", mar2Val)
	}
	if math.Abs(mar3Val-10_000.0) > 0.10 {
		t.Errorf("March 3 value=%.4f, want ≈10000.00 (±0.10) — TST1 2× and TST2/TST3 0.5× should cancel",
			mar3Val)
	}
	t.Logf("CountervailingReturns: mar2=%.4f, mar3=%.4f", mar2Val, mar3Val)
}

// TestReallocate_Active_DollarFractionsNotEqualWeights verifies that for an active
// portfolio, recomputeSharesOnTransition uses dollar-value fractions derived from
// original shares × current price — NOT equal weights. We prove this by choosing
// an asymmetric share count (1:2:8) and then doubling TST1 on March 4; the resulting
// portfolio value clearly differs between the dollar-fraction prediction ($1,090.91)
// and the equal-weight prediction ($1,333.33).
func TestReallocate_Active_DollarFractionsNotEqualWeights(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	tst1 := nextTicker()
	tst2 := nextTicker()
	tst3 := nextTicker()
	portName := nextPortfolioName()

	tst1ID, err := createTestSecurity(pool, tst1, "DolFrac IPO 1", models.SecurityTypeStock, &mathMar2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst1): %v", err)
	}
	defer cleanupTestSecurity(pool, tst1)

	tst2ID, err := createTestSecurity(pool, tst2, "DolFrac Stable 2", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst2): %v", err)
	}
	defer cleanupTestSecurity(pool, tst2)

	tst3ID, err := createTestSecurity(pool, tst3, "DolFrac Stable 3", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst3): %v", err)
	}
	defer cleanupTestSecurity(pool, tst3)

	// TST1: $100 on March 2-3; $200 from March 4.
	// TST2, TST3: $100 throughout.
	tst1Prices := mergePrices(
		map[time.Time]float64{mathMar2: 100.0, mathMar3: 100.0},
		weekdayPrices(mathMar4, mathMar20, 200.0),
	)
	insertPriceRows(t, pool, tst1ID, tst1Prices)
	insertPriceRows(t, pool, tst2ID, weekdayPrices(mathFeb2, mathMar20, 100.0))
	insertPriceRows(t, pool, tst3ID, weekdayPrices(mathFeb2, mathMar20, 100.0))

	// Active: 1 share TST1, 2 shares TST2, 8 shares TST3.
	portfolioID, err := createTestPortfolio(pool, portName, 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: tst1ID, PercentageOrShares: 1.0},
		{SecurityID: tst2ID, PercentageOrShares: 2.0},
		{SecurityID: tst3ID, PercentageOrShares: 8.0},
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
	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, mathFeb2)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}

	diffs := services.GenerateReallocateDiffs(coverage)
	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, portfolio, mathFeb2, mathMar20, diffs, nil)
	if err != nil {
		t.Fatalf("ComputeDailyValues: %v", err)
	}

	// Pre-transition (Feb 27): 0 TST1 + 2×$100 + 8×$100 = $1,000.
	feb27Val, okFeb27 := dvFor(dailyValues, mathFeb27)
	if !okFeb27 {
		t.Fatal("no value for Feb 27")
	}
	if math.Abs(feb27Val-1_000.0) > 0.01 {
		t.Errorf("Feb 27 (pre-transition): value=%.4f, want 1000.00", feb27Val)
	}

	// March 4: TST1 doubles to $200.
	// Dollar-fraction prediction:
	//   original dollar values: 1×$100=100, 2×$100=200, 8×$100=800 → total=$1,100
	//   budget = $1,000 (Mar 2 lastPortfolioValue)
	//   shares[TST1] = 1000 × (100/1100) / 100 = 1000/1100 = 0.9091
	//   shares[TST2] = 1000 × (200/1100) / 100 = 2000/1100 = 1.8182
	//   shares[TST3] = 1000 × (800/1100) / 100 = 8000/1100 = 7.2727
	//   Mar 4 value = 0.9091×200 + 1.8182×100 + 7.2727×100 = 181.82 + 181.82 + 727.27 = $1,090.91
	const budget = 1_000.0
	const totalOriginal = 1*100.0 + 2*100.0 + 8*100.0 // = 1100
	sharesTST1dollar := budget * (1.0 * 100.0 / totalOriginal) / 100.0
	sharesTST2dollar := budget * (2.0 * 100.0 / totalOriginal) / 100.0
	sharesTST3dollar := budget * (8.0 * 100.0 / totalOriginal) / 100.0
	wantMar4dollar := sharesTST1dollar*200.0 + sharesTST2dollar*100.0 + sharesTST3dollar*100.0

	// Equal-weight prediction (wrong):
	sharesTST1equal := budget / 3.0 / 100.0
	sharesTST2equal := budget / 3.0 / 100.0
	sharesTST3equal := budget / 3.0 / 100.0
	wrongMar4equal := sharesTST1equal*200.0 + sharesTST2equal*100.0 + sharesTST3equal*100.0

	mar4Val, okMar4 := dvFor(dailyValues, mathMar4)
	if !okMar4 {
		t.Fatal("no value for March 4")
	}
	if math.Abs(mar4Val-wantMar4dollar) > 1.0 {
		t.Errorf("March 4 value=%.2f; want ≈%.2f (dollar fractions); wrong equal-weight answer=%.2f",
			mar4Val, wantMar4dollar, wrongMar4equal)
	}
	t.Logf("DollarFractions: feb27=%.2f, mar4=%.2f, want≈%.2f, wrongEqual≈%.2f",
		feb27Val, mar4Val, wantMar4dollar, wrongMar4equal)
}

// TestReallocate_Glance_LifeGain_WithPreIPOMember tests the full /glance HTTP path
// with MissingDataStrategyReallocate. Portfolio created Feb 2, TST1 IPOs March 2.
// TST1 doubles on March 3, driving a ~33% life-of-portfolio gain.
// Asserts: W4004 warning present, LifeOfPortfolioReturn.Percentage ≈ 0.333.
func TestReallocate_Glance_LifeGain_WithPreIPOMember(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)

	tst1 := nextTicker()
	tst2 := nextTicker()
	tst3 := nextTicker()
	portName := nextPortfolioName()

	tst1ID, err := createTestSecurity(pool, tst1, "Glance IPO 1", models.SecurityTypeStock, &mathMar2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst1): %v", err)
	}
	defer cleanupTestSecurity(pool, tst1)

	tst2ID, err := createTestSecurity(pool, tst2, "Glance Stable 2", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst2): %v", err)
	}
	defer cleanupTestSecurity(pool, tst2)

	tst3ID, err := createTestSecurity(pool, tst3, "Glance Stable 3", models.SecurityTypeStock, &mathFeb2)
	if err != nil {
		t.Fatalf("createTestSecurity(tst3): %v", err)
	}
	defer cleanupTestSecurity(pool, tst3)

	// Pre-IPO (Feb 2 - Feb 27): TST2=$100, TST3=$100 — portfolio flat at $10,000.
	// March 2 (transition): TST1=$200; portfolio rebalanced to 1/3 each at $10,000.
	//   shares[TST1]=10000/3/200=16.667, shares[TST2]=10000/3/100=33.333, shares[TST3]=33.333
	// March 3+ (TST1 doubles to $400): value = 16.667×400 + 33.333×100 + 33.333×100 ≈ $13,333
	// Forward-fill from March 3 → life gain ≈ $3,333 / $10,000 = 33.3%.
	tst1Prices := mergePrices(
		map[time.Time]float64{mathMar2: 200.0},
		weekdayPrices(mathMar3, mathMar20, 400.0),
	)
	insertPriceRows(t, pool, tst1ID, tst1Prices)
	insertPriceRows(t, pool, tst2ID, weekdayPrices(mathFeb2, mathMar20, 100.0))
	insertPriceRows(t, pool, tst3ID, weekdayPrices(mathFeb2, mathMar20, 100.0))

	portfolioID, err := createTestPortfolioWithDate(pool, portName, 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{
			{SecurityID: tst1ID, PercentageOrShares: 1.0 / 3.0},
			{SecurityID: tst2ID, PercentageOrShares: 1.0 / 3.0},
			{SecurityID: tst3ID, PercentageOrShares: 1.0 / 3.0},
		},
		mathFeb2,
	)
	if err != nil {
		t.Fatalf("createTestPortfolioWithDate: %v", err)
	}
	defer func() {
		cleanupGlanceEntries(pool, portfolioID)
		cleanupTestPortfolio(pool, portName, 1)
	}()

	router := setupGlanceTestRouter(pool)

	// Pin the portfolio.
	body, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: portfolioID})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("pin: expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// GET glance with reallocate strategy.
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet,
		fmt.Sprintf("/users/1/glance?missing_data_strategy=%s", models.MissingDataStrategyReallocate),
		nil)
	req.Header.Set("X-User-ID", "1")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("glance list: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp models.GlanceListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal GlanceListResponse: %v", err)
	}

	var found *models.GlancePortfolio
	for i := range resp.Portfolios {
		if resp.Portfolios[i].PortfolioID == portfolioID {
			found = &resp.Portfolios[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("portfolio %d not found in glance list (%d portfolios)", portfolioID, len(resp.Portfolios))
	}

	// W4004 must be present.
	foundWarn := false
	for _, w := range found.Warnings {
		if w.Code == models.WarnProportionalReallocation {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Errorf("W4004 not found in glance warnings: %v", found.Warnings)
	}

	// LifeOfPortfolioReturn.Percentage should be ≈ 0.333 (±0.05 tolerance for
	// forward-fill days between March 20 and today extending a flat tail).
	pct := found.LifeOfPortfolioReturn.Percentage
	if pct < 0.28 || pct > 0.38 {
		t.Errorf("LifeOfPortfolioReturn.Percentage=%.4f, want ≈0.333 (±0.05)", pct)
	}

	// Dollar gain must be positive and consistent with percentage.
	dollar := found.LifeOfPortfolioReturn.Dollar
	if dollar <= 0 {
		t.Errorf("LifeOfPortfolioReturn.Dollar=%.2f, want > 0", dollar)
	}
	t.Logf("Glance life gain: dollar=%.2f, percentage=%.4f, W4004=%v", dollar, pct, foundWarn)
}

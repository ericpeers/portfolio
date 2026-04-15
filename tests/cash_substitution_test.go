package tests

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

// newCashSubServices creates a minimal PerformanceService + PortfolioService backed by the
// test DB. The mock AV client (at a non-existent address) satisfies both Price and Treasury
// interfaces; since all test prices are pre-inserted and date ranges are well within the
// cache, neither client will be contacted during these tests.
func newCashSubServices(t *testing.T) (*services.PerformanceService, *services.PortfolioService) {
	t.Helper()
	pool := getTestPool(t)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	priceRepo := repository.NewPriceRepository(pool)
	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{Price: avClient, Treasury: avClient})
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo, 1)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	return performanceSvc, portfolioSvc
}

// TestSynthesizeCashPrices_Flat verifies that CashFlat fills every calendar day in the
// pre-IPO window with the anchor close price at EffectiveStart.
func TestSynthesizeCashPrices_Flat(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ticker := nextTicker()
	effectiveStart := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC) // Monday
	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)  // Monday, one week before
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)

	secID, err := createTestSecurity(pool, ticker, "CashFlat Test", models.SecurityTypeStock, &effectiveStart)
	if err != nil {
		t.Fatalf("failed to create security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	// insertPriceData sets close = basePrice + 2
	const basePrice = 100.0
	const anchorClose = basePrice + 2

	if err := insertPriceData(pool, secID, effectiveStart, endDate, basePrice); err != nil {
		t.Fatalf("failed to insert price data: %v", err)
	}

	performanceSvc, _ := newCashSubServices(t)

	// Build coverage report manually: one gapped member.
	coverage := &services.DataCoverageReport{
		RequestedStart:   requestedStart,
		ConstrainedStart: effectiveStart,
		AnyGaps:          true,
		Members: []services.MemberCoverage{
			{
				SecurityID:      secID,
				Ticker:          ticker,
				EffectiveStart:  effectiveStart,
				HasFullCoverage: false,
			},
		},
	}

	overlay, err := performanceSvc.SynthesizeCashPrices(ctx, coverage, models.MissingDataStrategyCashFlat)
	if err != nil {
		t.Fatalf("SynthesizeCashPrices failed: %v", err)
	}

	secOverlay, ok := overlay[secID]
	if !ok {
		t.Fatal("expected overlay entry for security, got none")
	}

	// Calendar days from Jan 6 to Jan 12 inclusive = 7 days.
	expectedDays := 7
	if len(secOverlay) != expectedDays {
		t.Errorf("overlay has %d entries, want %d", len(secOverlay), expectedDays)
	}

	// Every pre-IPO day must equal the anchor close.
	for d := requestedStart; d.Before(effectiveStart); d = d.AddDate(0, 0, 1) {
		price, exists := secOverlay[d]
		if !exists {
			t.Errorf("missing overlay entry for %s", d.Format("2006-01-02"))
			continue
		}
		if math.Abs(price-anchorClose) > 0.001 {
			t.Errorf("date %s: overlay price = %.4f, want %.4f", d.Format("2006-01-02"), price, anchorClose)
		}
	}

	// effectiveStart itself must NOT be in the overlay (real price data exists from that day).
	if _, exists := secOverlay[effectiveStart]; exists {
		t.Errorf("effectiveStart %s unexpectedly present in overlay", effectiveStart.Format("2006-01-02"))
	}
}

// TestSynthesizeCashPrices_Appreciating verifies that CashAppreciating fills pre-IPO days
// with monotonically non-decreasing prices (toward the anchor at EffectiveStart).
// Uses 2024 dates so DGS10 data is guaranteed cached in the test DB.
func TestSynthesizeCashPrices_Appreciating(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ticker := nextTicker()
	effectiveStart := time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC) // Tuesday after MLK Day
	requestedStart := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)  // Monday, 8 calendar days before
	endDate := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	secID, err := createTestSecurity(pool, ticker, "CashAppreciating Test", models.SecurityTypeStock, &effectiveStart)
	if err != nil {
		t.Fatalf("failed to create security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	const basePrice = 100.0
	const anchorClose = basePrice + 2

	if err := insertPriceData(pool, secID, effectiveStart, endDate, basePrice); err != nil {
		t.Fatalf("failed to insert price data: %v", err)
	}

	performanceSvc, _ := newCashSubServices(t)

	coverage := &services.DataCoverageReport{
		RequestedStart:   requestedStart,
		ConstrainedStart: effectiveStart,
		AnyGaps:          true,
		Members: []services.MemberCoverage{
			{
				SecurityID:      secID,
				Ticker:          ticker,
				EffectiveStart:  effectiveStart,
				HasFullCoverage: false,
			},
		},
	}

	overlay, err := performanceSvc.SynthesizeCashPrices(ctx, coverage, models.MissingDataStrategyCashAppreciating)
	if err != nil {
		t.Fatalf("SynthesizeCashPrices failed: %v", err)
	}

	secOverlay, ok := overlay[secID]
	if !ok {
		t.Fatal("expected overlay entry for security, got none")
	}

	// Calendar days from Jan 8 to Jan 15 inclusive = 8 days.
	expectedDays := 8
	if len(secOverlay) != expectedDays {
		t.Errorf("overlay has %d entries, want %d", len(secOverlay), expectedDays)
	}

	// Collect pre-IPO days in forward order.
	var days []time.Time
	for d := requestedStart; d.Before(effectiveStart); d = d.AddDate(0, 0, 1) {
		days = append(days, d)
	}

	// All pre-IPO prices must be positive and <= anchor close.
	for _, d := range days {
		price, exists := secOverlay[d]
		if !exists {
			t.Errorf("missing overlay entry for %s", d.Format("2006-01-02"))
			continue
		}
		if price <= 0 {
			t.Errorf("date %s: overlay price = %.6f, want > 0", d.Format("2006-01-02"), price)
		}
		if price > anchorClose+0.001 {
			t.Errorf("date %s: overlay price = %.6f exceeds anchorClose = %.6f", d.Format("2006-01-02"), price, anchorClose)
		}
	}

	// Prices must be monotonically non-decreasing going forward in time toward IPO.
	for i := 1; i < len(days); i++ {
		p0 := secOverlay[days[i-1]]
		p1 := secOverlay[days[i]]
		if p1 < p0-0.0001 {
			t.Errorf("price decreased forward: %s (%.6f) > %s (%.6f)",
				days[i-1].Format("2006-01-02"), p0,
				days[i].Format("2006-01-02"), p1)
		}
	}

	t.Logf("Appreciating: price at requestedStart=%.6f, anchorClose=%.6f (discount=%.6f)",
		secOverlay[requestedStart], anchorClose, anchorClose-secOverlay[requestedStart])
}

// TestCashSubstitution_IntegrationFlat verifies the full pipeline for CashFlat strategy:
// ComputeDataCoverage → SynthesizeCashPrices → ComputeDailyValues produces daily values
// covering the full date range including pre-IPO trading days.
func TestCashSubstitution_IntegrationFlat(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ticker := nextTicker()
	portName := nextPortfolioName()

	// requestedStart one week before IPO so there are 5 clear pre-IPO trading days.
	effectiveStart := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)
	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)

	secID, err := createTestSecurity(pool, ticker, "CashFlat Integration Test", models.SecurityTypeStock, &effectiveStart)
	if err != nil {
		t.Fatalf("failed to create security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	// insertPriceData sets close = basePrice + 2
	const basePrice = 100.0
	const anchorClose = basePrice + 2

	if err := insertPriceData(pool, secID, effectiveStart, endDate, basePrice); err != nil {
		t.Fatalf("failed to insert price data: %v", err)
	}

	portfolioID, err := createTestPortfolio(pool, portName, 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("failed to create portfolio: %v", err)
	}
	defer cleanupTestPortfolio(pool, portName, 1)

	performanceSvc, portfolioSvc := newCashSubServices(t)

	portfolio, err := portfolioSvc.GetPortfolio(ctx, portfolioID)
	if err != nil {
		t.Fatalf("failed to get portfolio: %v", err)
	}

	warnCtx, wc := services.NewWarningContext(ctx)

	coverage, err := performanceSvc.ComputeDataCoverage(warnCtx, portfolio, requestedStart)
	if err != nil {
		t.Fatalf("ComputeDataCoverage failed: %v", err)
	}
	if !coverage.AnyGaps {
		t.Fatal("expected AnyGaps = true for pre-IPO security, got false")
	}

	overlay, err := performanceSvc.SynthesizeCashPrices(warnCtx, coverage, models.MissingDataStrategyCashFlat)
	if err != nil {
		t.Fatalf("SynthesizeCashPrices failed: %v", err)
	}
	if len(overlay) == 0 {
		t.Fatal("expected non-empty overlay for gapped security, got empty")
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(warnCtx, portfolio, requestedStart, endDate, overlay)
	if err != nil {
		t.Fatalf("ComputeDailyValues failed: %v", err)
	}
	if len(dailyValues) == 0 {
		t.Fatal("expected daily values, got none")
	}

	// Pre-IPO: Jan 6-10 — 4 trading days (Jan 9 closed for Carter state funeral)
	// Post-IPO: Jan 13-17 (Mon-Fri) = 5 trading days
	const expectedDays = 9
	if len(dailyValues) < expectedDays {
		t.Errorf("expected at least %d daily values, got %d", expectedDays, len(dailyValues))
	}

	// First daily value must start at requestedStart (Monday, no holiday).
	if !dailyValues[0].Date.Equal(requestedStart) {
		t.Errorf("first daily value date = %s, want %s",
			dailyValues[0].Date.Format("2006-01-02"),
			requestedStart.Format("2006-01-02"))
	}

	// Pre-IPO dates must have value = anchorClose × 1 share = anchorClose.
	const expectedPreIPOValue = anchorClose * 1.0
	for _, dv := range dailyValues {
		if dv.Value == 0 {
			t.Errorf("date %s has zero value", dv.Date.Format("2006-01-02"))
		}
		if dv.Date.Before(effectiveStart) {
			if math.Abs(dv.Value-expectedPreIPOValue) > 0.01 {
				t.Errorf("pre-IPO date %s: value = %.4f, want %.4f (flat cash = anchor × shares)",
					dv.Date.Format("2006-01-02"), dv.Value, expectedPreIPOValue)
			}
		}
	}

	// No W3001 (missing price history) warnings should be emitted.
	for _, w := range wc.GetWarnings() {
		if w.Code == models.WarnMissingPriceHistory {
			t.Errorf("unexpected W3001 warning: %s", w.Message)
		}
	}

	t.Logf("CashFlat integration: %d daily values from %s to %s (first value = %.2f)",
		len(dailyValues),
		dailyValues[0].Date.Format("2006-01-02"),
		dailyValues[len(dailyValues)-1].Date.Format("2006-01-02"),
		dailyValues[0].Value)
}

// TestSynthesizeCashPrices_NoGap_EmptyOverlay verifies that when all portfolio members have
// full price coverage, SynthesizeCashPrices returns an empty overlay for both strategies.
func TestSynthesizeCashPrices_NoGap_EmptyOverlay(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ticker := nextTicker()
	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)

	secID, err := createTestSecurity(pool, ticker, "NoGap Test", models.SecurityTypeStock, &requestedStart)
	if err != nil {
		t.Fatalf("failed to create security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	performanceSvc, _ := newCashSubServices(t)

	// Coverage report with full coverage — no members have gaps.
	coverage := &services.DataCoverageReport{
		RequestedStart:   requestedStart,
		ConstrainedStart: requestedStart,
		AnyGaps:          false,
		Members: []services.MemberCoverage{
			{
				SecurityID:      secID,
				Ticker:          ticker,
				EffectiveStart:  requestedStart,
				HasFullCoverage: true,
			},
		},
	}

	for _, strategy := range []models.MissingDataStrategy{
		models.MissingDataStrategyCashFlat,
		models.MissingDataStrategyCashAppreciating,
	} {
		overlay, err := performanceSvc.SynthesizeCashPrices(ctx, coverage, strategy)
		if err != nil {
			t.Fatalf("SynthesizeCashPrices(%q) failed: %v", strategy, err)
		}
		if len(overlay) != 0 {
			t.Errorf("strategy %q: expected empty overlay for fully-covered portfolio, got %d entries",
				strategy, len(overlay))
		}
	}
}

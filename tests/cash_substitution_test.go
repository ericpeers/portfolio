package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
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

// newComparisonService creates a full ComparisonService for integration testing.
func newComparisonService(t *testing.T) *services.ComparisonService {
	t.Helper()
	pool := getTestPool(t)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	priceRepo := repository.NewPriceRepository(pool)
	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{Price: avClient, Treasury: avClient})
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo, 1)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	return services.NewComparisonService(portfolioSvc, membershipSvc, performanceSvc, securityRepo)
}

// TestComparePortfolios_IdealWithPreIPOGap reproduces the 500 error when an ideal
// portfolio contains a security that hasn't IPO'd at the requested start date.
func TestComparePortfolios_IdealWithPreIPOGap(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	ctx := context.Background()

	ticker := nextTicker()
	portNameActual := nextPortfolioName()
	portNameIdeal := nextPortfolioName()

	// IPO is in the middle of requested range
	requestedStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) // Monday
	effectiveStart := time.Date(2024, 6, 3, 0, 0, 0, 0, time.UTC) // Monday
	endDate := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	secID, err := createTestSecurity(pool, ticker, "IPO Gap Test", models.SecurityTypeStock, &effectiveStart)
	if err != nil {
		t.Fatalf("failed to create security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	// Insert some price data starting from IPO
	if err := insertPriceData(pool, secID, effectiveStart, endDate, 100.0); err != nil {
		t.Fatalf("failed to insert price data: %v", err)
	}

	// Create an actual portfolio with some other security that has full coverage
	otherTicker := nextTicker()
	otherSecID, err := createTestStock(pool, otherTicker, "Full Coverage Stock")
	if err != nil {
		t.Fatalf("failed to create other security: %v", err)
	}
	defer cleanupTestSecurity(pool, otherTicker)
	if err := insertPriceData(pool, otherSecID, requestedStart, endDate, 50.0); err != nil {
		t.Fatalf("failed to insert price data for other security: %v", err)
	}

	actualPortfolioID, err := createTestPortfolio(pool, portNameActual, 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: otherSecID, PercentageOrShares: 100.0},
	})
	if err != nil {
		t.Fatalf("failed to create actual portfolio: %v", err)
	}
	defer cleanupTestPortfolio(pool, portNameActual, 1)

	// Create an ideal portfolio with the IPO-gap security
	idealPortfolioID, err := createTestPortfolio(pool, portNameIdeal, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secID, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("failed to create ideal portfolio: %v", err)
	}
	defer cleanupTestPortfolio(pool, portNameIdeal, 1)

	// ComparisonService
	comparisonSvc := newComparisonService(t)

	req := &models.CompareRequest{
		PortfolioA:          actualPortfolioID,
		PortfolioB:          idealPortfolioID,
		StartPeriod:         models.FlexibleDate{Time: requestedStart},
		EndPeriod:           models.FlexibleDate{Time: endDate},
		MissingDataStrategy: models.MissingDataStrategyCashFlat,
	}

	// This used to return a 500 error because NormalizeIdealPortfolio called
	// GetPriceAtDate on a pre-IPO date and got an error. Now it uses overlayB
	// (synthesized cash prices) so the pre-IPO period is covered.
	resp, err := comparisonSvc.ComparePortfolios(ctx, req)
	if err != nil {
		t.Fatalf("ComparePortfolios failed: %v", err)
	}

	dvA := resp.PerformanceMetrics.PortfolioAMetrics.DailyValues
	dvB := resp.PerformanceMetrics.PortfolioBMetrics.DailyValues

	// Portfolio A has full price coverage for the entire range, so its daily-value
	// count is the authoritative trading-day count (weekdays minus US market holidays).
	// Use it as the expected count rather than computing weekdays ourselves, since
	// ComputeDailyValues excludes holidays but insertPriceData doesn't know about them.
	if len(dvA) < 200 {
		t.Fatalf("portfolio A: only %d daily values for a full-year range — test data may not have loaded correctly", len(dvA))
	}
	// This is the key regression check: the ideal portfolio must cover the full
	// range including pre-IPO dates, not just the post-IPO window (effectiveStart–endDate).
	if len(dvB) != len(dvA) {
		t.Errorf("portfolio B (ideal with pre-IPO gap): got %d daily values, want %d (same as portfolio A) — pre-IPO dates likely truncated", len(dvB), len(dvA))
	}

	// Detect normalization explosion: the ideal portfolio's values should stay
	// near the actual portfolio's start value. Prices in this test are constant
	// ($100 for the ideal security, anchor-priced at $100 for cash substitution),
	// so no daily value should be more than 2× the start value.
	startVal := resp.PerformanceMetrics.PortfolioAMetrics.StartValue
	if startVal == 0 {
		t.Fatal("portfolio A start value is zero — test data may not have loaded correctly")
	}
	for i, dv := range dvB {
		if dv.Value > startVal*2 {
			t.Errorf("portfolio B daily value[%d] (%s) = %.2f is more than 2× start value %.2f — possible normalization explosion from wrong pre-IPO price", i, dv.Date, dv.Value, startVal)
		}
	}
}

// TestCashSubstitutionWarning_BothPortfoliosPreIPO verifies that W4003 (WarnCashSubstituted)
// appears in the response for both the /portfolios/compare endpoint and the /glance endpoint
// when the ideal portfolio and the actual portfolio each contain a security that hasn't IPO'd
// at the requested start date.
//
// This is a TDD test: it is written before the fix and is expected to fail until the code
// correctly surfaces W4003 for ideal portfolios.
func TestCashSubstitutionWarning_BothPortfoliosPreIPO(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Two securities — one per portfolio — each IPO'd mid-year.
	// Using 2024 so treasury-rate data (required for CashAppreciating) is cached in the test DB.
	requestedStart := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)  // first 2024 market day
	ipoDate := time.Date(2024, 6, 3, 0, 0, 0, 0, time.UTC)         // Monday
	endDate := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	// secIdeal is held 100% in the ideal portfolio.
	tickerIdeal := nextTicker()
	secIdeal, err := createTestSecurity(pool, tickerIdeal, "PreIPO Ideal Security", models.SecurityTypeStock, &ipoDate)
	if err != nil {
		t.Fatalf("create ideal security: %v", err)
	}
	defer cleanupTestSecurity(pool, tickerIdeal)
	if err := insertPriceData(pool, secIdeal, ipoDate, endDate, 100.0); err != nil {
		t.Fatalf("insert ideal security prices: %v", err)
	}

	// secActual is held in the actual portfolio with share count.
	tickerActual := nextTicker()
	secActual, err := createTestSecurity(pool, tickerActual, "PreIPO Actual Security", models.SecurityTypeStock, &ipoDate)
	if err != nil {
		t.Fatalf("create actual security: %v", err)
	}
	defer cleanupTestSecurity(pool, tickerActual)
	if err := insertPriceData(pool, secActual, ipoDate, endDate, 50.0); err != nil {
		t.Fatalf("insert actual security prices: %v", err)
	}

	// Ideal portfolio — created before the IPO so glance uses the full pre-IPO range.
	portNameIdeal := nextPortfolioName()
	idealPortfolioID, err := createTestPortfolioWithDate(pool, portNameIdeal, 1, models.PortfolioTypeIdeal,
		[]models.MembershipRequest{{SecurityID: secIdeal, PercentageOrShares: 1.0}},
		requestedStart,
	)
	if err != nil {
		t.Fatalf("create ideal portfolio: %v", err)
	}
	defer func() {
		cleanupGlanceEntries(pool, idealPortfolioID)
		cleanupTestPortfolio(pool, portNameIdeal, 1)
	}()

	// Actual portfolio — also created before the IPO.
	portNameActual := nextPortfolioName()
	actualPortfolioID, err := createTestPortfolioWithDate(pool, portNameActual, 1, models.PortfolioTypeActive,
		[]models.MembershipRequest{{SecurityID: secActual, PercentageOrShares: 10}},
		requestedStart,
	)
	if err != nil {
		t.Fatalf("create actual portfolio: %v", err)
	}
	defer func() {
		cleanupGlanceEntries(pool, actualPortfolioID)
		cleanupTestPortfolio(pool, portNameActual, 1)
	}()

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	compareRouter := setupDailyValuesTestRouter(pool, avClient)
	glanceRouter := setupGlanceTestRouter(pool, avClient)

	// -----------------------------------------------------------------------
	// Part 1: /portfolios/compare must include W4003 in resp.Warnings
	// when both portfolios have pre-IPO securities and strategy = cash_flat.
	// -----------------------------------------------------------------------

	compareBody, _ := json.Marshal(models.CompareRequest{
		PortfolioA:          actualPortfolioID,
		PortfolioB:          idealPortfolioID,
		StartPeriod:         models.FlexibleDate{Time: requestedStart},
		EndPeriod:           models.FlexibleDate{Time: endDate},
		MissingDataStrategy: models.MissingDataStrategyCashFlat,
	})
	wCmp := httptest.NewRecorder()
	reqCmp, _ := http.NewRequest(http.MethodPost, "/portfolios/compare", bytes.NewReader(compareBody))
	reqCmp.Header.Set("Content-Type", "application/json")
	compareRouter.ServeHTTP(wCmp, reqCmp)
	if wCmp.Code != http.StatusOK {
		t.Fatalf("POST /portfolios/compare: expected 200, got %d: %s", wCmp.Code, wCmp.Body.String())
	}

	var cmpResp models.CompareResponse
	if err := json.Unmarshal(wCmp.Body.Bytes(), &cmpResp); err != nil {
		t.Fatalf("unmarshal compare response: %v", err)
	}

	hasCashSubstWarning := func(warnings []models.Warning) bool {
		for _, w := range warnings {
			if w.Code == models.WarnCashSubstituted {
				return true
			}
		}
		return false
	}

	if !hasCashSubstWarning(cmpResp.Warnings) {
		t.Errorf("compare: resp.Warnings missing W4003 (WarnCashSubstituted); got %d warning(s): %v",
			len(cmpResp.Warnings), cmpResp.Warnings)
	} else {
		t.Logf("compare: W4003 present in resp.Warnings (%d total warnings)", len(cmpResp.Warnings))
	}

	// -----------------------------------------------------------------------
	// Part 2: /glance must include W4003 per-portfolio for both the ideal
	// and actual portfolios when strategy = cash_flat.
	// -----------------------------------------------------------------------

	// Pin both portfolios to glance.
	for _, pid := range []int64{idealPortfolioID, actualPortfolioID} {
		pinBody, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: pid})
		wPin := httptest.NewRecorder()
		reqPin, _ := http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(pinBody))
		reqPin.Header.Set("Content-Type", "application/json")
		glanceRouter.ServeHTTP(wPin, reqPin)
		if wPin.Code != http.StatusCreated && wPin.Code != http.StatusOK {
			t.Fatalf("pin portfolio %d: expected 201/200, got %d: %s", pid, wPin.Code, wPin.Body.String())
		}
	}

	wGlance := httptest.NewRecorder()
	reqGlance, _ := http.NewRequest(http.MethodGet, "/users/1/glance?missing_data_strategy=cash_flat", nil)
	glanceRouter.ServeHTTP(wGlance, reqGlance)
	if wGlance.Code != http.StatusOK {
		t.Fatalf("GET /users/1/glance: expected 200, got %d: %s", wGlance.Code, wGlance.Body.String())
	}

	var glanceResp models.GlanceListResponse
	if err := json.Unmarshal(wGlance.Body.Bytes(), &glanceResp); err != nil {
		t.Fatalf("unmarshal glance response: %v", err)
	}

	findGlancePortfolio := func(id int64) *models.GlancePortfolio {
		for i := range glanceResp.Portfolios {
			if glanceResp.Portfolios[i].PortfolioID == id {
				return &glanceResp.Portfolios[i]
			}
		}
		return nil
	}

	idealGlance := findGlancePortfolio(idealPortfolioID)
	if idealGlance == nil {
		t.Fatalf("ideal portfolio %d not found in glance response", idealPortfolioID)
	}
	if !hasCashSubstWarning(idealGlance.Warnings) {
		t.Errorf("glance ideal portfolio: Warnings missing W4003; got %d warning(s): %v",
			len(idealGlance.Warnings), idealGlance.Warnings)
	} else {
		t.Logf("glance ideal portfolio: W4003 present (%d warnings)", len(idealGlance.Warnings))
	}

	actualGlance := findGlancePortfolio(actualPortfolioID)
	if actualGlance == nil {
		t.Fatalf("actual portfolio %d not found in glance response", actualPortfolioID)
	}
	if !hasCashSubstWarning(actualGlance.Warnings) {
		t.Errorf("glance actual portfolio: Warnings missing W4003; got %d warning(s): %v",
			len(actualGlance.Warnings), actualGlance.Warnings)
	} else {
		t.Logf("glance actual portfolio: W4003 present (%d warnings)", len(actualGlance.Warnings))
	}
}

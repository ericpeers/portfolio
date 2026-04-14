package tests

import (
	"context"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

// buildPerformanceSvc creates a PerformanceService wired to the real DB (no mock AV).
// The service is used solely for ComputeDataCoverage, which only needs secRepo and
// pricingSvc.GetFirstPriceDate — it never calls out to AV.
func buildCoveragePerformanceSvc(t *testing.T) *services.PerformanceService {
	t.Helper()
	pool := getTestPool(t)
	secRepo := repository.NewSecurityRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	// Pass nil clients — coverage resolution does not trigger any AV fetches.
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{})
	return services.NewPerformanceService(pricingSvc, repository.NewPortfolioRepository(pool), secRepo, 1)
}

// TestComputeDataCoverage_FullCoverage verifies that a portfolio whose securities
// all have inception dates before the requested start date reports no gaps.
func TestComputeDataCoverage_FullCoverage(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ticker := nextTicker()
	defer cleanupTestSecurity(pool, ticker)

	// Security with inception well before the requested window.
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := createTestSecurity(pool, ticker, "Coverage Full Test", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("createTestSecurity: %v", err)
	}

	portfolio := &models.PortfolioWithMemberships{
		Portfolio: models.Portfolio{ID: 0, PortfolioType: models.PortfolioTypeIdeal},
		Memberships: []models.PortfolioMembership{
			{SecurityID: secID, Ticker: ticker, PercentageOrShares: 1.0},
		},
	}

	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	svc := buildCoveragePerformanceSvc(t)
	report, err := svc.ComputeDataCoverage(context.Background(), portfolio, requestedStart)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}

	if report.AnyGaps {
		t.Errorf("expected AnyGaps=false, got true")
	}
	if !report.ConstrainedStart.Equal(requestedStart) {
		t.Errorf("expected ConstrainedStart=%s, got %s", requestedStart, report.ConstrainedStart)
	}
	if len(report.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(report.Members))
	}
	if !report.Members[0].HasFullCoverage {
		t.Errorf("expected HasFullCoverage=true for %s", ticker)
	}
}

// TestComputeDataCoverage_GapDetected verifies that a security whose inception date
// is after the requested start is flagged as a gap and ConstrainedStart is set accordingly.
func TestComputeDataCoverage_GapDetected(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ticker := nextTicker()
	defer cleanupTestSecurity(pool, ticker)

	// Security IPO'd after the requested start.
	inception := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	secID, err := createTestSecurity(pool, ticker, "Coverage Gap Test", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("createTestSecurity: %v", err)
	}

	portfolio := &models.PortfolioWithMemberships{
		Portfolio:   models.Portfolio{ID: 0, PortfolioType: models.PortfolioTypeIdeal},
		Memberships: []models.PortfolioMembership{{SecurityID: secID, Ticker: ticker, PercentageOrShares: 1.0}},
	}

	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	svc := buildCoveragePerformanceSvc(t)
	report, err := svc.ComputeDataCoverage(context.Background(), portfolio, requestedStart)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}

	if !report.AnyGaps {
		t.Errorf("expected AnyGaps=true, got false")
	}
	if !report.ConstrainedStart.Equal(inception) {
		t.Errorf("expected ConstrainedStart=%s, got %s", inception, report.ConstrainedStart)
	}
	if len(report.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(report.Members))
	}
	if report.Members[0].HasFullCoverage {
		t.Errorf("expected HasFullCoverage=false for %s", ticker)
	}
}

// TestComputeDataCoverage_NullInceptionFallback verifies that when a security has no
// inception date set, ComputeDataCoverage falls back to the first price row date.
func TestComputeDataCoverage_NullInceptionFallback(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ticker := nextTicker()
	defer cleanupTestSecurity(pool, ticker)

	// No inception date — ComputeDataCoverage must fall back to first price row.
	secID, err := createTestSecurity(pool, ticker, "Coverage Null Inception", models.SecurityTypeStock, nil)
	if err != nil {
		t.Fatalf("createTestSecurity: %v", err)
	}

	// Insert prices starting at a date after the requested start.
	firstPriceDate := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	lastPriceDate := time.Date(2025, 4, 10, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(pool, secID, firstPriceDate, lastPriceDate, 100.0); err != nil {
		t.Fatalf("insertPriceData: %v", err)
	}

	portfolio := &models.PortfolioWithMemberships{
		Portfolio:   models.Portfolio{ID: 0, PortfolioType: models.PortfolioTypeIdeal},
		Memberships: []models.PortfolioMembership{{SecurityID: secID, Ticker: ticker, PercentageOrShares: 1.0}},
	}

	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	svc := buildCoveragePerformanceSvc(t)
	report, err := svc.ComputeDataCoverage(context.Background(), portfolio, requestedStart)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}

	if !report.AnyGaps {
		t.Errorf("expected AnyGaps=true (first price is after requested start), got false")
	}
	if !report.ConstrainedStart.Equal(firstPriceDate) {
		t.Errorf("expected ConstrainedStart=%s (first price date), got %s", firstPriceDate, report.ConstrainedStart)
	}
	if len(report.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(report.Members))
	}
	if report.Members[0].HasFullCoverage {
		t.Errorf("expected HasFullCoverage=false when first price is after requested start")
	}
}

// TestComputeDataCoverage_MultiMember verifies that ConstrainedStart is the maximum
// effective start across all members in the portfolio.
func TestComputeDataCoverage_MultiMember(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	tickerA := nextTicker()
	tickerB := nextTicker()
	defer cleanupTestSecurity(pool, tickerA)
	defer cleanupTestSecurity(pool, tickerB)

	inceptionA := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC) // before requested start
	inceptionB := time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC) // after requested start

	secIDA, err := createTestSecurity(pool, tickerA, "Coverage Multi A", models.SecurityTypeStock, &inceptionA)
	if err != nil {
		t.Fatalf("createTestSecurity A: %v", err)
	}
	secIDB, err := createTestSecurity(pool, tickerB, "Coverage Multi B", models.SecurityTypeStock, &inceptionB)
	if err != nil {
		t.Fatalf("createTestSecurity B: %v", err)
	}

	portfolio := &models.PortfolioWithMemberships{
		Portfolio: models.Portfolio{ID: 0, PortfolioType: models.PortfolioTypeIdeal},
		Memberships: []models.PortfolioMembership{
			{SecurityID: secIDA, Ticker: tickerA, PercentageOrShares: 0.5},
			{SecurityID: secIDB, Ticker: tickerB, PercentageOrShares: 0.5},
		},
	}

	requestedStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	svc := buildCoveragePerformanceSvc(t)
	report, err := svc.ComputeDataCoverage(context.Background(), portfolio, requestedStart)
	if err != nil {
		t.Fatalf("ComputeDataCoverage: %v", err)
	}

	if !report.AnyGaps {
		t.Errorf("expected AnyGaps=true")
	}
	if !report.ConstrainedStart.Equal(inceptionB) {
		t.Errorf("expected ConstrainedStart=%s (later inception), got %s", inceptionB, report.ConstrainedStart)
	}

	// tickerA should have full coverage; tickerB should not.
	var covA, covB *MemberCoverageResult
	for _, mc := range report.Members {
		if mc.SecurityID == secIDA {
			covA = &MemberCoverageResult{HasFullCoverage: mc.HasFullCoverage}
		} else if mc.SecurityID == secIDB {
			covB = &MemberCoverageResult{HasFullCoverage: mc.HasFullCoverage}
		}
	}
	if covA == nil || covB == nil {
		t.Fatal("expected entries for both members in report")
	}
	if !covA.HasFullCoverage {
		t.Errorf("expected %s to have full coverage", tickerA)
	}
	if covB.HasFullCoverage {
		t.Errorf("expected %s to NOT have full coverage", tickerB)
	}
}

// MemberCoverageResult is a small local helper to make the multi-member test readable.
type MemberCoverageResult struct {
	HasFullCoverage bool
}

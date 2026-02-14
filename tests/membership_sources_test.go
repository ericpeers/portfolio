package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupMembershipSourcesService creates a MembershipService for testing
func setupMembershipSourcesService(pool *pgxpool.Pool, avClient *alphavantage.Client) *services.MembershipService {
	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceCacheRepo := repository.NewPriceCacheRepository(pool)
	pricingSvc := services.NewPricingService(priceCacheRepo, securityRepo, avClient)
	return services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
}

// insertETFHoldings directly inserts ETF holdings and pull range into the database
func insertETFHoldings(pool *pgxpool.Pool, etfID int64, holdings map[int64]float64) error {
	ctx := context.Background()

	// Insert ETF holdings
	for secID, percentage := range holdings {
		_, err := pool.Exec(ctx, `
			INSERT INTO dim_etf_membership (dim_security_id, dim_composite_id, percentage)
			VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING
		`, secID, etfID, percentage)
		if err != nil {
			return fmt.Errorf("failed to insert ETF holding: %w", err)
		}
	}

	// Set pull range with far-future next_update so cache is used
	futureUpdate := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := pool.Exec(ctx, `
		INSERT INTO dim_etf_pull_range (composite_id, pull_date, next_update)
		VALUES ($1, $2, $3)
		ON CONFLICT (composite_id) DO UPDATE SET pull_date = $2, next_update = $3
	`, etfID, time.Now(), futureUpdate)
	if err != nil {
		return fmt.Errorf("failed to insert ETF pull range: %w", err)
	}

	return nil
}

// cleanupMembershipSourcesData cleans up test data for membership sources tests
func cleanupMembershipSourcesData(pool *pgxpool.Pool, tickers []string) {
	ctx := context.Background()
	for _, ticker := range tickers {
		var secID int64
		err := pool.QueryRow(ctx, `SELECT id FROM dim_security WHERE ticker = $1`, ticker).Scan(&secID)
		if err != nil {
			continue
		}
		pool.Exec(ctx, `DELETE FROM portfolio_membership WHERE security_id = $1`, secID)
		pool.Exec(ctx, `DELETE FROM dim_etf_membership WHERE dim_composite_id = $1`, secID)
		pool.Exec(ctx, `DELETE FROM dim_etf_membership WHERE dim_security_id = $1`, secID)
		pool.Exec(ctx, `DELETE FROM dim_etf_pull_range WHERE composite_id = $1`, secID)
		pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
	}
}

// findMembership finds an ExpandedMembership by symbol in a slice
func findMembership(memberships []models.ExpandedMembership, symbol string) *models.ExpandedMembership {
	for i := range memberships {
		if memberships[i].Symbol == symbol {
			return &memberships[i]
		}
	}
	return nil
}

// findSource finds a MembershipSource by symbol in a slice
func findSource(sources []models.MembershipSource, symbol string) *models.MembershipSource {
	for i := range sources {
		if sources[i].Symbol == symbol {
			return &sources[i]
		}
	}
	return nil
}

// sourcesSum returns the sum of source allocations
func sourcesSum(sources []models.MembershipSource) float64 {
	sum := 0.0
	for _, s := range sources {
		sum += s.Allocation
	}
	return sum
}

// TestMembershipSourcesDirectOnly tests that direct holdings have themselves as the sole source
func TestMembershipSourcesDirectOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	tickers := []string{"MSTSTA", "MSTSTB"}
	cleanupMembershipSourcesData(pool, tickers)
	defer cleanupMembershipSourcesData(pool, tickers)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secIDA, err := setupDailyValuesTestSecurity(pool, "MSTSTA", "Membership Source Test A", &inception)
	if err != nil {
		t.Fatalf("Failed to setup security A: %v", err)
	}
	secIDB, err := setupDailyValuesTestSecurity(pool, "MSTSTB", "Membership Source Test B", &inception)
	if err != nil {
		t.Fatalf("Failed to setup security B: %v", err)
	}

	// Create ideal portfolio with two direct holdings
	cleanupDailyValuesTestPortfolio(pool, "MS Direct Only", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "MS Direct Only", 1)

	portfolioID, err := createTestPortfolio(pool, "MS Direct Only", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: secIDA, PercentageOrShares: 0.60},
		{SecurityID: secIDB, PercentageOrShares: 0.40},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	svc := setupMembershipSourcesService(pool, avClient)
	ctx := context.Background()
	endDate := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	result, err := svc.ComputeMembership(ctx, portfolioID, models.PortfolioTypeIdeal, endDate, nil)
	if err != nil {
		t.Fatalf("ComputeMembership failed: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("Expected 2 expanded memberships, got %d", len(result))
	}

	// Each direct holding should have exactly 1 source: itself
	for _, em := range result {
		if len(em.Sources) != 1 {
			t.Errorf("Security %s: expected 1 source, got %d", em.Symbol, len(em.Sources))
			continue
		}
		if em.Sources[0].SecurityID != em.SecurityID {
			t.Errorf("Security %s: source security_id %d doesn't match %d", em.Symbol, em.Sources[0].SecurityID, em.SecurityID)
		}
		if em.Sources[0].Symbol != em.Symbol {
			t.Errorf("Security %s: source symbol %s doesn't match", em.Symbol, em.Sources[0].Symbol)
		}
		if em.Sources[0].Allocation != 1.0 {
			t.Errorf("Security %s: source allocation %.4f, expected 1.0", em.Symbol, em.Sources[0].Allocation)
		}
	}

	t.Logf("Direct-only portfolio: %d memberships, all with self as sole source", len(result))
}

// TestMembershipSourcesETFOnly tests that ETF-expanded holdings have the ETF as source
func TestMembershipSourcesETFOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	tickers := []string{"MSETF1", "MSUND1", "MSUND2"}
	cleanupMembershipSourcesData(pool, tickers)
	defer cleanupMembershipSourcesData(pool, tickers)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create ETF and its underlying stocks
	etfID, err := setupTestETF(pool, "MSETF1", "Membership Source ETF 1")
	if err != nil {
		t.Fatalf("Failed to setup ETF: %v", err)
	}
	undID1, err := setupDailyValuesTestSecurity(pool, "MSUND1", "Underlying Stock 1", &inception)
	if err != nil {
		t.Fatalf("Failed to setup underlying 1: %v", err)
	}
	undID2, err := setupDailyValuesTestSecurity(pool, "MSUND2", "Underlying Stock 2", &inception)
	if err != nil {
		t.Fatalf("Failed to setup underlying 2: %v", err)
	}

	// Insert ETF holdings: MSETF1 holds 60% MSUND1 + 40% MSUND2
	err = insertETFHoldings(pool, etfID, map[int64]float64{
		undID1: 0.60,
		undID2: 0.40,
	})
	if err != nil {
		t.Fatalf("Failed to insert ETF holdings: %v", err)
	}

	// Create portfolio holding 100% ETF
	cleanupDailyValuesTestPortfolio(pool, "MS ETF Only", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "MS ETF Only", 1)

	portfolioID, err := createTestPortfolio(pool, "MS ETF Only", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: etfID, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	svc := setupMembershipSourcesService(pool, avClient)
	ctx := context.Background()
	endDate := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	result, err := svc.ComputeMembership(ctx, portfolioID, models.PortfolioTypeIdeal, endDate, nil)
	if err != nil {
		t.Fatalf("ComputeMembership failed: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("Expected 2 expanded memberships (ETF underlyings), got %d", len(result))
	}

	// Each underlying should have exactly 1 source: the ETF
	for _, em := range result {
		if len(em.Sources) != 1 {
			t.Errorf("Security %s: expected 1 source, got %d", em.Symbol, len(em.Sources))
			continue
		}
		if em.Sources[0].SecurityID != etfID {
			t.Errorf("Security %s: source security_id %d, expected ETF id %d", em.Symbol, em.Sources[0].SecurityID, etfID)
		}
		if em.Sources[0].Symbol != "MSETF1" {
			t.Errorf("Security %s: source symbol %s, expected MSETF1", em.Symbol, em.Sources[0].Symbol)
		}
		if em.Sources[0].Allocation != 1.0 {
			t.Errorf("Security %s: source allocation %.4f, expected 1.0", em.Symbol, em.Sources[0].Allocation)
		}
	}

	t.Logf("ETF-only portfolio: %d memberships, all sourced from MSETF1", len(result))
}

// TestMembershipSourcesMixedDirectAndETF tests a portfolio with both direct holdings
// and an ETF that also holds one of the direct stocks
func TestMembershipSourcesMixedDirectAndETF(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	tickers := []string{"MSMIX1", "MSMIX2", "MSMIXETF"}
	cleanupMembershipSourcesData(pool, tickers)
	defer cleanupMembershipSourcesData(pool, tickers)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// MSMIX1 = a stock held directly AND inside the ETF
	// MSMIX2 = a stock only inside the ETF
	// MSMIXETF = an ETF holding MSMIX1 (50%) and MSMIX2 (50%)
	stockID1, err := setupDailyValuesTestSecurity(pool, "MSMIX1", "Mix Stock 1", &inception)
	if err != nil {
		t.Fatalf("Failed to setup stock 1: %v", err)
	}
	stockID2, err := setupDailyValuesTestSecurity(pool, "MSMIX2", "Mix Stock 2", &inception)
	if err != nil {
		t.Fatalf("Failed to setup stock 2: %v", err)
	}
	etfID, err := setupTestETF(pool, "MSMIXETF", "Mix Source ETF")
	if err != nil {
		t.Fatalf("Failed to setup ETF: %v", err)
	}

	// ETF holds 50% MSMIX1 + 50% MSMIX2
	err = insertETFHoldings(pool, etfID, map[int64]float64{
		stockID1: 0.50,
		stockID2: 0.50,
	})
	if err != nil {
		t.Fatalf("Failed to insert ETF holdings: %v", err)
	}

	// Portfolio: 50% direct MSMIX1 + 50% MSMIXETF
	cleanupDailyValuesTestPortfolio(pool, "MS Mixed Portfolio", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "MS Mixed Portfolio", 1)

	portfolioID, err := createTestPortfolio(pool, "MS Mixed Portfolio", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: stockID1, PercentageOrShares: 0.50},
		{SecurityID: etfID, PercentageOrShares: 0.50},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	svc := setupMembershipSourcesService(pool, avClient)
	ctx := context.Background()
	endDate := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	result, err := svc.ComputeMembership(ctx, portfolioID, models.PortfolioTypeIdeal, endDate, nil)
	if err != nil {
		t.Fatalf("ComputeMembership failed: %v", err)
	}

	// Expected: MSMIX1 (75% of portfolio: 50% direct + 25% from ETF) and MSMIX2 (25% from ETF)
	if len(result) != 2 {
		t.Fatalf("Expected 2 expanded memberships, got %d", len(result))
	}

	// Check MSMIX1: should have 2 sources (direct + ETF)
	mix1 := findMembership(result, "MSMIX1")
	if mix1 == nil {
		t.Fatal("MSMIX1 not found in expanded memberships")
	}

	// Total allocation for MSMIX1: 0.50 direct + (0.50 ETF * 0.50 holding) = 0.75
	expectedAllocation := 0.75
	if math.Abs(mix1.Allocation-expectedAllocation) > 0.01 {
		t.Errorf("MSMIX1 allocation: got %.2f, expected %.2f", mix1.Allocation, expectedAllocation)
	}

	if len(mix1.Sources) != 2 {
		t.Fatalf("MSMIX1: expected 2 sources, got %d", len(mix1.Sources))
	}

	// Source from direct: contributed 50 out of 75 = 2/3
	directSrc := findSource(mix1.Sources, "MSMIX1")
	if directSrc == nil {
		t.Fatal("MSMIX1: direct source not found")
	}
	expectedDirectProportion := 50.0 / 75.0 // ~0.6667
	if math.Abs(directSrc.Allocation-expectedDirectProportion) > 0.001 {
		t.Errorf("MSMIX1 direct source allocation: got %.4f, expected %.4f", directSrc.Allocation, expectedDirectProportion)
	}

	// Source from ETF: contributed 25 out of 75 = 1/3
	etfSrc := findSource(mix1.Sources, "MSMIXETF")
	if etfSrc == nil {
		t.Fatal("MSMIX1: ETF source not found")
	}
	expectedETFProportion := 25.0 / 75.0 // ~0.3333
	if math.Abs(etfSrc.Allocation-expectedETFProportion) > 0.001 {
		t.Errorf("MSMIX1 ETF source allocation: got %.4f, expected %.4f", etfSrc.Allocation, expectedETFProportion)
	}

	// Sources should sum to 1.0
	if math.Abs(sourcesSum(mix1.Sources)-1.0) > 0.001 {
		t.Errorf("MSMIX1 sources sum: got %.4f, expected 1.0", sourcesSum(mix1.Sources))
	}

	// Check MSMIX2: should have 1 source (ETF only)
	mix2 := findMembership(result, "MSMIX2")
	if mix2 == nil {
		t.Fatal("MSMIX2 not found in expanded memberships")
	}
	if len(mix2.Sources) != 1 {
		t.Errorf("MSMIX2: expected 1 source, got %d", len(mix2.Sources))
	}
	if mix2.Sources[0].Symbol != "MSMIXETF" {
		t.Errorf("MSMIX2: source symbol %s, expected MSMIXETF", mix2.Sources[0].Symbol)
	}
	if mix2.Sources[0].Allocation != 1.0 {
		t.Errorf("MSMIX2: source allocation %.4f, expected 1.0", mix2.Sources[0].Allocation)
	}

	t.Logf("Mixed portfolio: MSMIX1 has %d sources (allocation %.4f), MSMIX2 has %d sources (allocation %.4f)",
		len(mix1.Sources), mix1.Allocation, len(mix2.Sources), mix2.Allocation)
}

// TestMembershipSourcesMultipleETFs tests a portfolio with two ETFs that both hold
// the same underlying security
func TestMembershipSourcesMultipleETFs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	tickers := []string{"MSMETF1", "MSMETF2", "MSMSTK1", "MSMSTK2", "MSMSTK3"}
	cleanupMembershipSourcesData(pool, tickers)
	defer cleanupMembershipSourcesData(pool, tickers)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create stocks
	stkID1, err := setupDailyValuesTestSecurity(pool, "MSMSTK1", "Multi ETF Stock 1", &inception)
	if err != nil {
		t.Fatalf("Failed to setup stock 1: %v", err)
	}
	stkID2, err := setupDailyValuesTestSecurity(pool, "MSMSTK2", "Multi ETF Stock 2", &inception)
	if err != nil {
		t.Fatalf("Failed to setup stock 2: %v", err)
	}
	stkID3, err := setupDailyValuesTestSecurity(pool, "MSMSTK3", "Multi ETF Stock 3", &inception)
	if err != nil {
		t.Fatalf("Failed to setup stock 3: %v", err)
	}

	// ETF1: holds MSMSTK1 (60%) + MSMSTK2 (40%)
	etfID1, err := setupTestETF(pool, "MSMETF1", "Multi Source ETF 1")
	if err != nil {
		t.Fatalf("Failed to setup ETF 1: %v", err)
	}
	err = insertETFHoldings(pool, etfID1, map[int64]float64{
		stkID1: 0.60,
		stkID2: 0.40,
	})
	if err != nil {
		t.Fatalf("Failed to insert ETF1 holdings: %v", err)
	}

	// ETF2: holds MSMSTK1 (30%) + MSMSTK3 (70%)
	etfID2, err := setupTestETF(pool, "MSMETF2", "Multi Source ETF 2")
	if err != nil {
		t.Fatalf("Failed to setup ETF 2: %v", err)
	}
	err = insertETFHoldings(pool, etfID2, map[int64]float64{
		stkID1: 0.30,
		stkID3: 0.70,
	})
	if err != nil {
		t.Fatalf("Failed to insert ETF2 holdings: %v", err)
	}

	// Portfolio: 50% MSMETF1 + 50% MSMETF2
	cleanupDailyValuesTestPortfolio(pool, "MS Multi ETF Portfolio", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "MS Multi ETF Portfolio", 1)

	portfolioID, err := createTestPortfolio(pool, "MS Multi ETF Portfolio", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: etfID1, PercentageOrShares: 0.50},
		{SecurityID: etfID2, PercentageOrShares: 0.50},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	svc := setupMembershipSourcesService(pool, avClient)
	ctx := context.Background()
	endDate := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	result, err := svc.ComputeMembership(ctx, portfolioID, models.PortfolioTypeIdeal, endDate, nil)
	if err != nil {
		t.Fatalf("ComputeMembership failed: %v", err)
	}

	// Expected expanded memberships:
	// MSMSTK1: 0.50*0.60 + 0.50*0.30 = 0.30 + 0.15 = 0.45
	// MSMSTK2: 0.50*0.40 = 0.20
	// MSMSTK3: 0.50*0.70 = 0.35
	if len(result) != 3 {
		t.Fatalf("Expected 3 expanded memberships, got %d", len(result))
	}

	// MSMSTK1 should have 2 sources (ETF1 and ETF2)
	stk1 := findMembership(result, "MSMSTK1")
	if stk1 == nil {
		t.Fatal("MSMSTK1 not found in expanded memberships")
	}
	if math.Abs(stk1.Allocation-0.45) > 0.01 {
		t.Errorf("MSMSTK1 allocation: got %.4f, expected 0.45", stk1.Allocation)
	}
	if len(stk1.Sources) != 2 {
		t.Fatalf("MSMSTK1: expected 2 sources, got %d", len(stk1.Sources))
	}

	// ETF1 contributed 30 out of 45 = 2/3
	etf1Src := findSource(stk1.Sources, "MSMETF1")
	if etf1Src == nil {
		t.Fatal("MSMSTK1: ETF1 source not found")
	}
	expectedETF1 := 30.0 / 45.0
	if math.Abs(etf1Src.Allocation-expectedETF1) > 0.001 {
		t.Errorf("MSMSTK1 ETF1 source: got %.4f, expected %.4f", etf1Src.Allocation, expectedETF1)
	}

	// ETF2 contributed 15 out of 45 = 1/3
	etf2Src := findSource(stk1.Sources, "MSMETF2")
	if etf2Src == nil {
		t.Fatal("MSMSTK1: ETF2 source not found")
	}
	expectedETF2 := 15.0 / 45.0
	if math.Abs(etf2Src.Allocation-expectedETF2) > 0.001 {
		t.Errorf("MSMSTK1 ETF2 source: got %.4f, expected %.4f", etf2Src.Allocation, expectedETF2)
	}

	// Sources should sum to 1.0
	if math.Abs(sourcesSum(stk1.Sources)-1.0) > 0.001 {
		t.Errorf("MSMSTK1 sources sum: got %.4f, expected 1.0", sourcesSum(stk1.Sources))
	}

	// MSMSTK2 should have 1 source (ETF1)
	stk2 := findMembership(result, "MSMSTK2")
	if stk2 == nil {
		t.Fatal("MSMSTK2 not found")
	}
	if len(stk2.Sources) != 1 {
		t.Errorf("MSMSTK2: expected 1 source, got %d", len(stk2.Sources))
	}
	if stk2.Sources[0].Symbol != "MSMETF1" {
		t.Errorf("MSMSTK2: source symbol %s, expected MSMETF1", stk2.Sources[0].Symbol)
	}
	if stk2.Sources[0].Allocation != 1.0 {
		t.Errorf("MSMSTK2: source allocation %.4f, expected 1.0", stk2.Sources[0].Allocation)
	}

	// MSMSTK3 should have 1 source (ETF2)
	stk3 := findMembership(result, "MSMSTK3")
	if stk3 == nil {
		t.Fatal("MSMSTK3 not found")
	}
	if len(stk3.Sources) != 1 {
		t.Errorf("MSMSTK3: expected 1 source, got %d", len(stk3.Sources))
	}
	if stk3.Sources[0].Symbol != "MSMETF2" {
		t.Errorf("MSMSTK3: source symbol %s, expected MSMETF2", stk3.Sources[0].Symbol)
	}
	if stk3.Sources[0].Allocation != 1.0 {
		t.Errorf("MSMSTK3: source allocation %.4f, expected 1.0", stk3.Sources[0].Allocation)
	}

	// All memberships should have sources summing to 1.0
	for _, em := range result {
		sum := sourcesSum(em.Sources)
		if math.Abs(sum-1.0) > 0.001 {
			t.Errorf("Security %s: sources sum %.4f, expected 1.0", em.Symbol, sum)
		}
	}

	t.Logf("Multi-ETF portfolio: MSMSTK1(%.4f) from 2 ETFs, MSMSTK2(%.4f) from ETF1, MSMSTK3(%.4f) from ETF2",
		stk1.Allocation, stk2.Allocation, stk3.Allocation)
}

// TestMembershipSourcesZeroWeightHolding tests that an ETF holding with 0% weight
// does not produce NaN in source allocations (the bug that caused "json: unsupported value: NaN")
func TestMembershipSourcesZeroWeightHolding(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	tickers := []string{"MSZETF", "MSZSTK1", "MSZSTK2"}
	cleanupMembershipSourcesData(pool, tickers)
	defer cleanupMembershipSourcesData(pool, tickers)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create ETF and underlying stocks
	etfID, err := setupTestETF(pool, "MSZETF", "Zero Weight ETF")
	if err != nil {
		t.Fatalf("Failed to setup ETF: %v", err)
	}
	stkID1, err := setupDailyValuesTestSecurity(pool, "MSZSTK1", "Zero Weight Stock 1", &inception)
	if err != nil {
		t.Fatalf("Failed to setup stock 1: %v", err)
	}
	stkID2, err := setupDailyValuesTestSecurity(pool, "MSZSTK2", "Zero Weight Stock 2", &inception)
	if err != nil {
		t.Fatalf("Failed to setup stock 2: %v", err)
	}

	// ETF holds MSZSTK1 at 100% and MSZSTK2 at 0%
	err = insertETFHoldings(pool, etfID, map[int64]float64{
		stkID1: 1.0,
		stkID2: 0.0,
	})
	if err != nil {
		t.Fatalf("Failed to insert ETF holdings: %v", err)
	}

	// Portfolio: 100% MSZETF
	cleanupDailyValuesTestPortfolio(pool, "MS Zero Weight Portfolio", 1)
	defer cleanupDailyValuesTestPortfolio(pool, "MS Zero Weight Portfolio", 1)

	portfolioID, err := createTestPortfolio(pool, "MS Zero Weight Portfolio", 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: etfID, PercentageOrShares: 1.0},
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	svc := setupMembershipSourcesService(pool, avClient)
	ctx := context.Background()
	endDate := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	result, err := svc.ComputeMembership(ctx, portfolioID, models.PortfolioTypeIdeal, endDate, nil)
	if err != nil {
		t.Fatalf("ComputeMembership failed: %v", err)
	}

	// The zero-weight holding should be excluded from the result
	zeroStk := findMembership(result, "MSZSTK2")
	if zeroStk != nil {
		t.Errorf("MSZSTK2 (0%% weight) should not appear in expanded memberships, but found with allocation %.4f", zeroStk.Allocation)
	}

	// The non-zero holding should be present and have no NaN values
	stk1 := findMembership(result, "MSZSTK1")
	if stk1 == nil {
		t.Fatal("MSZSTK1 not found in expanded memberships")
	}

	// Verify no NaN in allocation or sources
	if math.IsNaN(stk1.Allocation) {
		t.Error("MSZSTK1 allocation is NaN")
	}
	for _, src := range stk1.Sources {
		if math.IsNaN(src.Allocation) {
			t.Errorf("MSZSTK1 source %s allocation is NaN", src.Symbol)
		}
	}

	// Verify the result is JSON-serializable (the original bug caused json.Marshal to fail)
	_, jsonErr := json.Marshal(result)
	if jsonErr != nil {
		t.Errorf("Failed to marshal result to JSON: %v", jsonErr)
	}

	t.Logf("Zero-weight test: %d memberships returned, zero-weight holding correctly excluded", len(result))
}

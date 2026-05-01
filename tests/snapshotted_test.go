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
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

// TestSnapshottedAtStoredAndReturned verifies that the `snapshotted` date is
// persisted on create, survives a GET round-trip, and can be updated via PUT.
func TestSnapshottedAtStoredAndReturned(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	secID, err := createTestStock(pool, "TSNAP1", "Test Snapshotted 1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSNAP1")

	cleanupTestPortfolio(pool, "Snapshotted Explicit", 1)
	defer cleanupTestPortfolio(pool, "Snapshotted Explicit", 1)

	snapDate := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)

	// --- Create with explicit snapshotted date ---
	createReq := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveGrowth,
		Name:          "Snapshotted Explicit",
		OwnerID:       1,
		Memberships:   []models.MembershipRequest{{SecurityID: secID, PercentageOrShares: 10}},
		CreatedAt:     &models.FlexibleDate{Time: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)},
		SnapshottedAt: &models.FlexibleDate{Time: snapDate},
	}
	body, _ := json.Marshal(createReq)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var created models.PortfolioWithMemberships
	json.Unmarshal(w.Body.Bytes(), &created)

	if created.Portfolio.SnapshottedAt == nil {
		t.Fatal("create response: snapshotted is nil, expected a date")
	}
	gotSnap := created.Portfolio.SnapshottedAt.UTC().Truncate(24 * time.Hour)
	if !gotSnap.Equal(snapDate) {
		t.Errorf("create: snapshotted got %v, want %v", gotSnap, snapDate)
	}

	// --- GET should return the same snapshotted date ---
	reqG, _ := http.NewRequest("GET", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), nil)
	reqG.Header.Set("Authorization", authHeader(1, "USER"))
	wG := httptest.NewRecorder()
	router.ServeHTTP(wG, reqG)
	if wG.Code != http.StatusOK {
		t.Fatalf("get: expected 200, got %d: %s", wG.Code, wG.Body.String())
	}
	var fetched models.PortfolioWithMemberships
	json.Unmarshal(wG.Body.Bytes(), &fetched)
	if fetched.Portfolio.SnapshottedAt == nil {
		t.Fatal("get: snapshotted is nil after round-trip")
	}
	gotSnap2 := fetched.Portfolio.SnapshottedAt.UTC().Truncate(24 * time.Hour)
	if !gotSnap2.Equal(snapDate) {
		t.Errorf("get: snapshotted got %v, want %v", gotSnap2, snapDate)
	}

	// --- PUT with a new snapshotted date ---
	newSnapDate := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	updateReq := models.UpdatePortfolioRequest{
		SnapshottedAt: &models.FlexibleDate{Time: newSnapDate},
	}
	ubody, _ := json.Marshal(updateReq)
	reqU, _ := http.NewRequest("PUT", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), bytes.NewBuffer(ubody))
	reqU.Header.Set("Content-Type", "application/json")
	reqU.Header.Set("Authorization", authHeader(1, "USER"))
	wU := httptest.NewRecorder()
	router.ServeHTTP(wU, reqU)
	if wU.Code != http.StatusOK {
		t.Fatalf("update: expected 200, got %d: %s", wU.Code, wU.Body.String())
	}

	// --- GET again to confirm updated snapshotted date ---
	reqG2, _ := http.NewRequest("GET", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), nil)
	reqG2.Header.Set("Authorization", authHeader(1, "USER"))
	wG2 := httptest.NewRecorder()
	router.ServeHTTP(wG2, reqG2)
	var updated models.PortfolioWithMemberships
	json.Unmarshal(wG2.Body.Bytes(), &updated)
	if updated.Portfolio.SnapshottedAt == nil {
		t.Fatal("get after update: snapshotted is nil")
	}
	gotSnap3 := updated.Portfolio.SnapshottedAt.UTC().Truncate(24 * time.Hour)
	if !gotSnap3.Equal(newSnapDate) {
		t.Errorf("get after update: snapshotted got %v, want %v", gotSnap3, newSnapDate)
	}
}

// TestSnapshottedAtSplitReversal verifies that when snapshotted is set on an Active
// portfolio, ComputeDailyValues reverse-applies splits that occurred between created_at
// and the snapshot date. This prevents double-counting splits when share counts were
// entered from a current (post-split) snapshot rather than from inception.
//
// Scenario:
//   - 2-for-1 split on Jan 13 2025
//   - Portfolio holds 20 shares (post-split count, recorded on Jan 17)
//   - created_at = Jan 6; snapshotted = Jan 17
//   - Without fix: Jan 10 value = 20 shares × $200 = $4000 (wrong — split double-counted)
//   - With fix:    Jan 10 value = 10 shares × $200 = $2000 (correct pre-split baseline)
//   - Post-split:  Jan 13 value = 20 shares × $100 = $2000 (continuous across split)
func TestSnapshottedAtSplitReversal(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := createTestSecurity(pool, "TSNAPRVL", "Snapshotted Split Reversal", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSNAPRVL")

	startDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 24, 0, 0, 0, 0, time.UTC)
	splitDate := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)
	splitCoefficient := 2.0
	basePrice := 200.0 // $200 pre-split, $100 post-split

	if err := insertPriceDataWithSplit(pool, secID, startDate, endDate, basePrice, splitDate, splitCoefficient); err != nil {
		t.Fatalf("Failed to insert price data: %v", err)
	}
	if err := insertSplitEvent(pool, secID, splitDate, splitCoefficient); err != nil {
		t.Fatalf("Failed to insert split event: %v", err)
	}

	// Portfolio holds 20 shares — the post-split share count as of the snapshot date.
	cleanupTestPortfolio(pool, "Snapshotted Split Reversal Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Snapshotted Split Reversal Portfolio", 1)

	portfolioID, err := createTestPortfolio(pool, "Snapshotted Split Reversal Portfolio", 1, models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: secID, PercentageOrShares: 20}, // 20 shares (post-split count)
	})
	if err != nil {
		t.Fatalf("Failed to create portfolio: %v", err)
	}

	// Set snapshotted = Jan 17 (after the split on Jan 13, before endDate).
	// This tells ComputeDailyValues that the 20 shares are as-of Jan 17.
	snapDate := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)
	_, err = pool.Exec(ctx, `UPDATE portfolio SET snapshotted_at = $1 WHERE id = $2`, snapDate, portfolioID)
	if err != nil {
		t.Fatalf("Failed to set snapshotted date: %v", err)
	}

	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
	})
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo, 20)

	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	portfolio, err := portfolioSvc.GetPortfolio(ctx, portfolioID)
	if err != nil {
		t.Fatalf("Failed to get portfolio: %v", err)
	}

	if portfolio.Portfolio.SnapshottedAt == nil {
		t.Fatal("Portfolio snapshotted_at is nil — GetByID did not return it")
	}

	dailyValues, err := performanceSvc.ComputeDailyValues(ctx, portfolio, startDate, endDate, nil, nil)
	if err != nil {
		t.Fatalf("Failed to compute daily values: %v", err)
	}
	if len(dailyValues) == 0 {
		t.Fatal("Expected daily values, got none")
	}

	var preSplitValue, splitDayValue float64
	for _, dv := range dailyValues {
		dateStr := dv.Date.Format("2006-01-02")
		if dateStr == "2025-01-10" { // Friday before split
			preSplitValue = dv.Value
		}
		if dateStr == "2025-01-13" { // Split day
			splitDayValue = dv.Value
		}
	}

	if preSplitValue == 0 {
		t.Fatal("Did not find pre-split value for 2025-01-10")
	}
	if splitDayValue == 0 {
		t.Fatal("Did not find split day value for 2025-01-13")
	}

	// Pre-split: reversal gives 20/2 = 10 shares; 10 × $200 = $2000
	// Split day:  forward applies 2×; 20 shares × $100 = $2000 (continuous)
	expectedValue := 2000.0
	epsilon := 0.01

	if math.Abs(preSplitValue-expectedValue) > epsilon {
		t.Errorf("pre-split (Jan 10) value = %.2f, want %.2f (snapshotted reversal not applied)", preSplitValue, expectedValue)
	}
	if math.Abs(splitDayValue-expectedValue) > epsilon {
		t.Errorf("split day (Jan 13) value = %.2f, want %.2f (should be continuous across split)", splitDayValue, expectedValue)
	}

	t.Logf("Snapshotted split reversal: pre-split=%.2f, split-day=%.2f (expected both %.2f)",
		preSplitValue, splitDayValue, expectedValue)
}

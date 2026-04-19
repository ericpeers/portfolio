package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
)

func TestPreIPOCompare_FullyPreIPOMember(t *testing.T) {
	// Exercises the bug where /portfolios/compare with a date window entirely before
	// one member security's IPO date returns 500.
	//
	// Portfolio A holds five test securities (all TST-prefixed). Four have price data
	// covering the requested window (2023-05-30 to 2023-06-02). The fifth has an IPO
	// on 2024-01-02 — entirely after the window.
	//
	// With the default ConstrainDateRange strategy:
	//
	//  1. ComputeDataCoverage sets the late security's effectiveStart = 2024-01-02.
	//  2. ConstrainDateRange shifts startDate forward to 2024-01-02 (the latest effective start).
	//  3. 2024-01-02 is after windowEnd (2023-06-02) — the range is inverted.
	//  4. GetDailyPricesMulti returns 0 rows. ComputeDailyValues fails.
	//  5. The handler returns 500.
	//
	// The correct behaviour is to synthesise cash prices for the late security over the
	// pre-IPO gap so that the endpoint returns 200. This test will FAIL until that fix
	// is implemented.
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	windowStart := time.Date(2023, 5, 30, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2023, 6, 2, 0, 0, 0, 0, time.UTC)

	earlyInception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	lateInception := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC) // entirely after window

	// Four securities with full price history covering the window.
	type secEntry struct {
		id     int64
		ticker string
	}
	var earlySecIDs []secEntry
	for range 4 {
		ticker := nextTicker()
		id, err := createTestSecurity(pool, ticker, "Pre-IPO Compare Early "+ticker, models.SecurityTypeStock, &earlyInception)
		if err != nil {
			t.Fatalf("create early security %s: %v", ticker, err)
		}
		defer cleanupTestSecurity(pool, ticker)
		if err := insertPriceData(pool, id, windowStart, windowEnd, 100.0); err != nil {
			t.Fatalf("insert prices for %s: %v", ticker, err)
		}
		defer func(securityID int64) {
			pool.Exec(context.Background(), `DELETE FROM fact_price WHERE security_id = $1`, securityID)
			pool.Exec(context.Background(), `DELETE FROM fact_price_range WHERE security_id = $1`, securityID)
		}(id)
		earlySecIDs = append(earlySecIDs, secEntry{id: id, ticker: ticker})
	}

	// One security that IPO'd after the window — the one that triggers the bug.
	lateTicker := nextTicker()
	lateSecID, err := createTestSecurity(pool, lateTicker, "Pre-IPO Compare Late "+lateTicker, models.SecurityTypeStock, &lateInception)
	if err != nil {
		t.Fatalf("create late security: %v", err)
	}
	defer cleanupTestSecurity(pool, lateTicker)
	// Insert prices starting from its IPO date so the service can find an anchor price.
	lateDataEnd := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(pool, lateSecID, lateInception, lateDataEnd, 50.0); err != nil {
		t.Fatalf("insert prices for late security: %v", err)
	}
	defer func() {
		pool.Exec(context.Background(), `DELETE FROM fact_price WHERE security_id = $1`, lateSecID)
		pool.Exec(context.Background(), `DELETE FROM fact_price_range WHERE security_id = $1`, lateSecID)
	}()

	// Portfolio A: all five securities at equal ideal allocations (5 × 0.20 = 1.0).
	nameA := nextPortfolioName()
	cleanupTestPortfolio(pool, nameA, 1)
	defer cleanupTestPortfolio(pool, nameA, 1)

	membershipsA := []models.MembershipRequest{
		{SecurityID: earlySecIDs[0].id, PercentageOrShares: 0.20},
		{SecurityID: earlySecIDs[1].id, PercentageOrShares: 0.20},
		{SecurityID: earlySecIDs[2].id, PercentageOrShares: 0.20},
		{SecurityID: earlySecIDs[3].id, PercentageOrShares: 0.20},
		{SecurityID: lateSecID, PercentageOrShares: 0.20},
	}
	portfolioAID, err := createTestPortfolio(pool, nameA, 1, models.PortfolioTypeActive, membershipsA)
	if err != nil {
		t.Fatalf("create portfolio A: %v", err)
	}

	// Portfolio B: the four early securities at equal ideal allocations (4 × 0.25 = 1.0).
	nameB := nextPortfolioName()
	cleanupTestPortfolio(pool, nameB, 1)
	defer cleanupTestPortfolio(pool, nameB, 1)

	membershipsB := []models.MembershipRequest{
		{SecurityID: earlySecIDs[0].id, PercentageOrShares: 0.25},
		{SecurityID: earlySecIDs[1].id, PercentageOrShares: 0.25},
		{SecurityID: earlySecIDs[2].id, PercentageOrShares: 0.25},
		{SecurityID: earlySecIDs[3].id, PercentageOrShares: 0.25},
	}
	portfolioBID, err := createTestPortfolio(pool, nameB, 1, models.PortfolioTypeIdeal, membershipsB)
	if err != nil {
		t.Fatalf("create portfolio B: %v", err)
	}

	router := setupDailyValuesTestRouter(pool)

	body, _ := json.Marshal(models.CompareRequest{
		PortfolioA:          portfolioAID,
		PortfolioB:          portfolioBID,
		StartPeriod:         models.FlexibleDate{Time: windowStart},
		EndPeriod:           models.FlexibleDate{Time: windowEnd},
		MissingDataStrategy: models.MissingDataStrategyCashAppreciating,
		// Default strategy (ConstrainDateRange). The late security's inception (2024-01-02)
		// is after windowEnd (2023-06-02), so ConstrainedStart > endDate → inverted range → 500.
	})
	req, _ := http.NewRequest(http.MethodPost, "/portfolios/compare", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// BUG: currently returns 500. The fix should substitute cash prices for the late security
	// over the pre-IPO gap and return 200 with a W4003 warning.
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

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

// TestPreIPOCompare_BothPortfoliosPreIPO mirrors TestPreIPOCompare_FullyPreIPOMember but
// gives BOTH Portfolio A and Portfolio B their own pre-IPO security. This exercises the
// code paths that the first test cannot reach: SynthesizeCashPrices called for B
// (comparison_service.go line 93), overlayB non-nil, and overlayAwareBatchPrices merging
// synthetic prices for B's late security in ComputeMembership/ComputeDirectMembership.
func TestPreIPOCompare_BothPortfoliosPreIPO(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	windowStart := time.Date(2023, 5, 30, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2023, 6, 2, 0, 0, 0, 0, time.UTC)
	earlyInception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	lateInception := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	lateDataEnd := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	type secEntry struct {
		id     int64
		ticker string
	}

	// 4 early securities shared by both portfolios.
	var earlySecIDs []secEntry
	for range 4 {
		ticker := nextTicker()
		id, err := createTestSecurity(pool, ticker, "Both Pre-IPO Early "+ticker, models.SecurityTypeStock, &earlyInception)
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

	// Separate late security for Portfolio A.
	lateTickerA := nextTicker()
	lateSecIDA, err := createTestSecurity(pool, lateTickerA, "Both Pre-IPO Late A "+lateTickerA, models.SecurityTypeStock, &lateInception)
	if err != nil {
		t.Fatalf("create late security A: %v", err)
	}
	defer cleanupTestSecurity(pool, lateTickerA)
	if err := insertPriceData(pool, lateSecIDA, lateInception, lateDataEnd, 50.0); err != nil {
		t.Fatalf("insert prices for late security A: %v", err)
	}
	defer func() {
		pool.Exec(context.Background(), `DELETE FROM fact_price WHERE security_id = $1`, lateSecIDA)
		pool.Exec(context.Background(), `DELETE FROM fact_price_range WHERE security_id = $1`, lateSecIDA)
	}()

	// Separate late security for Portfolio B.
	lateTickerB := nextTicker()
	lateSecIDB, err := createTestSecurity(pool, lateTickerB, "Both Pre-IPO Late B "+lateTickerB, models.SecurityTypeStock, &lateInception)
	if err != nil {
		t.Fatalf("create late security B: %v", err)
	}
	defer cleanupTestSecurity(pool, lateTickerB)
	if err := insertPriceData(pool, lateSecIDB, lateInception, lateDataEnd, 75.0); err != nil {
		t.Fatalf("insert prices for late security B: %v", err)
	}
	defer func() {
		pool.Exec(context.Background(), `DELETE FROM fact_price WHERE security_id = $1`, lateSecIDB)
		pool.Exec(context.Background(), `DELETE FROM fact_price_range WHERE security_id = $1`, lateSecIDB)
	}()

	type comboCase struct {
		name  string
		typeA models.PortfolioType
		typeB models.PortfolioType
	}
	combos := []comboCase{
		{"Ideal_Ideal", models.PortfolioTypeIdeal, models.PortfolioTypeIdeal},
		{"Active_Active", models.PortfolioTypeActive, models.PortfolioTypeActive},
		{"Ideal_Active", models.PortfolioTypeIdeal, models.PortfolioTypeActive},
	}

	type strategyCase struct {
		name     string
		strategy models.MissingDataStrategy
		wantCode int
	}
	strategies := []strategyCase{
		{"constrain_date_range", models.MissingDataStrategyConstrainDateRange, http.StatusBadRequest},
		{"cash_flat", models.MissingDataStrategyCashFlat, http.StatusOK},
		{"cash_appreciating", models.MissingDataStrategyCashAppreciating, http.StatusOK},
	}

	membershipValue := func(ptype models.PortfolioType, n int) float64 {
		if ptype == models.PortfolioTypeIdeal {
			return 1.0 / float64(n)
		}
		return 10.0
	}

	buildMemberships := func(ptype models.PortfolioType, lateSecID int64) []models.MembershipRequest {
		n := len(earlySecIDs) + 1
		pct := membershipValue(ptype, n)
		var m []models.MembershipRequest
		for _, s := range earlySecIDs {
			m = append(m, models.MembershipRequest{SecurityID: s.id, PercentageOrShares: pct})
		}
		m = append(m, models.MembershipRequest{SecurityID: lateSecID, PercentageOrShares: pct})
		return m
	}

	router := setupDailyValuesTestRouter(pool)

	for _, c := range combos {
		t.Run(c.name, func(t *testing.T) {
			nameA := nextPortfolioName()
			cleanupTestPortfolio(pool, nameA, 1)
			t.Cleanup(func() { cleanupTestPortfolio(pool, nameA, 1) })
			portfolioAID, err := createTestPortfolio(pool, nameA, 1, c.typeA, buildMemberships(c.typeA, lateSecIDA))
			if err != nil {
				t.Fatalf("create portfolio A: %v", err)
			}

			nameB := nextPortfolioName()
			cleanupTestPortfolio(pool, nameB, 1)
			t.Cleanup(func() { cleanupTestPortfolio(pool, nameB, 1) })
			portfolioBID, err := createTestPortfolio(pool, nameB, 1, c.typeB, buildMemberships(c.typeB, lateSecIDB))
			if err != nil {
				t.Fatalf("create portfolio B: %v", err)
			}

			for _, s := range strategies {
				t.Run(s.name, func(t *testing.T) {
					t.Parallel()
					body, _ := json.Marshal(models.CompareRequest{
						PortfolioA:          portfolioAID,
						PortfolioB:          portfolioBID,
						StartPeriod:         models.FlexibleDate{Time: windowStart},
						EndPeriod:           models.FlexibleDate{Time: windowEnd},
						MissingDataStrategy: s.strategy,
					})
					req, _ := http.NewRequest(http.MethodPost, "/portfolios/compare", bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					w := httptest.NewRecorder()
					router.ServeHTTP(w, req)
					if w.Code != s.wantCode {
						t.Errorf("expected %d, got %d: %s", s.wantCode, w.Code, w.Body.String())
					}
				})
			}
		})
	}
}

// TestPreIPOCompare_FullyPreIPOMember runs a matrix of portfolio-type combinations
// (Ideal/Ideal, Active/Active, Ideal/Active) × missing-data strategies to exercise
// the bug where /portfolios/compare with a date window entirely before one member
// security's IPO date returns 500.
//
// Portfolio A holds five test securities. Four have price data covering the requested
// window (2023-05-30 to 2023-06-02). The fifth has an IPO on 2024-01-02 — entirely
// after the window.
//
// ConstrainDateRange: shifts start to the latest effective start (2024-01-02), which
// is after windowEnd, inverting the range. The handler must return 4xx, not 500.
// EJP: This was the original behavior before overlays and missing data strategies. In this case
// we have no substitution strategy, so we have to tell the user to change the date, or
// pick a new subsitution strategy.
//
// Cash substitution strategies: synthesise pre-IPO prices for the late security and
// return 200 with a W4003 warning. Add new substitution strategies to the strategies
// slice below as they are implemented.
func TestPreIPOCompare_FullyPreIPOMember(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	windowStart := time.Date(2023, 5, 30, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2023, 6, 2, 0, 0, 0, 0, time.UTC)
	earlyInception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	lateInception := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC) // entirely after window

	// --- Securities (shared across all combos and strategies) ---

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

	lateTicker := nextTicker()
	lateSecID, err := createTestSecurity(pool, lateTicker, "Pre-IPO Compare Late "+lateTicker, models.SecurityTypeStock, &lateInception)
	if err != nil {
		t.Fatalf("create late security: %v", err)
	}
	defer cleanupTestSecurity(pool, lateTicker)
	lateDataEnd := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(pool, lateSecID, lateInception, lateDataEnd, 50.0); err != nil {
		t.Fatalf("insert prices for late security: %v", err)
	}
	defer func() {
		pool.Exec(context.Background(), `DELETE FROM fact_price WHERE security_id = $1`, lateSecID)
		pool.Exec(context.Background(), `DELETE FROM fact_price_range WHERE security_id = $1`, lateSecID)
	}()

	// --- Matrix dimensions ---

	type comboCase struct {
		name  string
		typeA models.PortfolioType // holds early + late securities
		typeB models.PortfolioType // holds early securities only
	}
	combos := []comboCase{
		{"Ideal_Ideal", models.PortfolioTypeIdeal, models.PortfolioTypeIdeal},
		{"Active_Active", models.PortfolioTypeActive, models.PortfolioTypeActive},
		{"Ideal_Active", models.PortfolioTypeIdeal, models.PortfolioTypeActive},
	}

	type strategyCase struct {
		name     string
		strategy models.MissingDataStrategy
		// wantCode is the expected HTTP status after the pre-IPO fix is applied.
		// ConstrainDateRange with a fully pre-IPO member produces an inverted date
		// range and should return 4xx. Cash strategies synthesise pre-IPO prices
		// and should return 200. Add new substitution strategies here as they land.
		wantCode int
	}
	strategies := []strategyCase{
		{"constrain_date_range", models.MissingDataStrategyConstrainDateRange, http.StatusBadRequest},
		{"cash_flat", models.MissingDataStrategyCashFlat, http.StatusOK},
		{"cash_appreciating", models.MissingDataStrategyCashAppreciating, http.StatusOK},
	}

	// membershipValue returns the PercentageOrShares appropriate for the type:
	// Ideal uses equal fractional allocations summing to 1.0; Active uses share counts.
	membershipValue := func(ptype models.PortfolioType, n int) float64 {
		if ptype == models.PortfolioTypeIdeal {
			return 1.0 / float64(n)
		}
		return 10.0
	}

	buildMemberships := func(ptype models.PortfolioType, includeLate bool) []models.MembershipRequest {
		n := len(earlySecIDs)
		if includeLate {
			n++
		}
		pct := membershipValue(ptype, n)
		var m []models.MembershipRequest
		for _, s := range earlySecIDs {
			m = append(m, models.MembershipRequest{SecurityID: s.id, PercentageOrShares: pct})
		}
		if includeLate {
			m = append(m, models.MembershipRequest{SecurityID: lateSecID, PercentageOrShares: pct})
		}
		return m
	}

	router := setupDailyValuesTestRouter(pool)

	for _, c := range combos {
		t.Run(c.name, func(t *testing.T) {
			// Portfolio A: early + late securities.
			nameA := nextPortfolioName()
			cleanupTestPortfolio(pool, nameA, 1)
			t.Cleanup(func() { cleanupTestPortfolio(pool, nameA, 1) })
			portfolioAID, err := createTestPortfolio(pool, nameA, 1, c.typeA, buildMemberships(c.typeA, true))
			if err != nil {
				t.Fatalf("create portfolio A: %v", err)
			}

			// Portfolio B: early securities only.
			nameB := nextPortfolioName()
			cleanupTestPortfolio(pool, nameB, 1)
			t.Cleanup(func() { cleanupTestPortfolio(pool, nameB, 1) })
			portfolioBID, err := createTestPortfolio(pool, nameB, 1, c.typeB, buildMemberships(c.typeB, false))
			if err != nil {
				t.Fatalf("create portfolio B: %v", err)
			}

			for _, s := range strategies {
				t.Run(s.name, func(t *testing.T) {
					t.Parallel()
					body, _ := json.Marshal(models.CompareRequest{
						PortfolioA:          portfolioAID,
						PortfolioB:          portfolioBID,
						StartPeriod:         models.FlexibleDate{Time: windowStart},
						EndPeriod:           models.FlexibleDate{Time: windowEnd},
						MissingDataStrategy: s.strategy,
					})
					req, _ := http.NewRequest(http.MethodPost, "/portfolios/compare", bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					w := httptest.NewRecorder()
					router.ServeHTTP(w, req)
					if w.Code != s.wantCode {
						t.Errorf("expected %d, got %d: %s", s.wantCode, w.Code, w.Body.String())
					}
				})
			}
		})
	}
}

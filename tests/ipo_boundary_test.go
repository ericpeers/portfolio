package tests

// TestIPOBoundaryEarlyEdge exercises IPO dates at the very beginning of the compare/glance window.
//
// Window: Mon Feb 2 – Fri Feb 6, 2026.  Portfolio created_at = Feb 2 so that glance and
// compare share the same effective start anchor.
//
//   Scenario A — IPO on startDate (Feb 2): effectiveStart == requestedStart, so AnyGaps = false.
//                No synthesis or reallocation should occur; both endpoints return 200 with
//                normal computed values.
//
//   Scenario B — IPO on day after startDate (Feb 3): exactly one pre-IPO trading day (Feb 2).
//                Both strategies must synthesise/reallocate for that single day and return 200.
//
// TestIPOBoundaryLateEdge exercises IPO dates at the very end of the compare window.
//
//   Scenario C — IPO on n-1 day (Feb 5, Thursday): three pre-IPO days (Feb 2–4).
//   Scenario D — IPO on endDate (Feb 6, Friday): four pre-IPO days (Feb 2–5).
//                Only one real trading day exists inside the window.
//
// Both tests drive the /portfolios/compare and /users/:id/glance endpoints with the
// cash_appreciating and reallocate strategies and expect HTTP 200.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
)

func TestIPOBoundaryEarlyEdge(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	windowStart := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC) // Monday
	dayAfterStart := time.Date(2026, 2, 3, 0, 0, 0, 0, time.UTC) // Tuesday — 1 pre-IPO day
	windowEnd := time.Date(2026, 2, 6, 0, 0, 0, 0, time.UTC)   // Friday
	stableInception := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	stableTicker1 := nextTicker()
	stableTicker2 := nextTicker()

	stable1ID, err := createTestSecurity(pool, stableTicker1, "IPO Early Boundary S1", models.SecurityTypeStock, &stableInception)
	if err != nil {
		t.Fatalf("create stable1: %v", err)
	}
	defer cleanupTestSecurity(pool, stableTicker1)

	stable2ID, err := createTestSecurity(pool, stableTicker2, "IPO Early Boundary S2", models.SecurityTypeStock, &stableInception)
	if err != nil {
		t.Fatalf("create stable2: %v", err)
	}
	defer cleanupTestSecurity(pool, stableTicker2)

	insertPriceRows(t, pool, stable1ID, weekdayPrices(windowStart, windowEnd, 100.0))
	insertPriceRows(t, pool, stable2ID, weekdayPrices(windowStart, windowEnd, 100.0))

	compareRouter := setupDailyValuesTestRouter(pool)
	glanceRouter := setupGlanceTestRouter(pool)

	strategies := []models.MissingDataStrategy{
		models.MissingDataStrategyCashAppreciating,
		models.MissingDataStrategyReallocate,
	}

	for _, tc := range []struct {
		name    string
		ipoDate time.Time
	}{
		{"ipo_on_startdate", windowStart},
		{"ipo_on_day_after_startdate", dayAfterStart},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lateTicker := nextTicker()
			lateID, err := createTestSecurity(pool, lateTicker, "IPO Early Boundary Late "+lateTicker, models.SecurityTypeStock, &tc.ipoDate)
			if err != nil {
				t.Fatalf("create late security: %v", err)
			}
			defer cleanupTestSecurity(pool, lateTicker)
			insertPriceRows(t, pool, lateID, weekdayPrices(tc.ipoDate, windowEnd, 200.0))

			portName := nextPortfolioName()
			cleanupTestPortfolio(pool, portName, 1)

			portfolioID, err := createTestPortfolioWithDate(pool, portName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
				{SecurityID: stable1ID, PercentageOrShares: 1.0 / 3.0},
				{SecurityID: stable2ID, PercentageOrShares: 1.0 / 3.0},
				{SecurityID: lateID, PercentageOrShares: 1.0 / 3.0},
			}, windowStart)
			if err != nil {
				t.Fatalf("createTestPortfolioWithDate: %v", err)
			}
			t.Cleanup(func() {
				cleanupGlanceEntries(pool, portfolioID)
				cleanupTestPortfolio(pool, portName, 1)
			})

			pinBody, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: portfolioID})
			wPin := httptest.NewRecorder()
			reqPin, _ := http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(pinBody))
			reqPin.Header.Set("Content-Type", "application/json")
			glanceRouter.ServeHTTP(wPin, reqPin)
			if wPin.Code != http.StatusCreated {
				t.Fatalf("pin portfolio: expected 201, got %d: %s", wPin.Code, wPin.Body.String())
			}

			for _, strategy := range strategies {
				strategy := strategy
				t.Run(string(strategy), func(t *testing.T) {
					t.Parallel()

					body, _ := json.Marshal(models.CompareRequest{
						PortfolioA:          portfolioID,
						PortfolioB:          portfolioID,
						StartPeriod:         models.FlexibleDate{Time: windowStart},
						EndPeriod:           models.FlexibleDate{Time: windowEnd},
						MissingDataStrategy: strategy,
					})
					w := httptest.NewRecorder()
					req, _ := http.NewRequest(http.MethodPost, "/portfolios/compare", bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					compareRouter.ServeHTTP(w, req)
					if w.Code != http.StatusOK {
						t.Errorf("compare: expected 200, got %d: %s", w.Code, w.Body.String())
					}

					wGlance := httptest.NewRecorder()
					reqGlance, _ := http.NewRequest(http.MethodGet,
						fmt.Sprintf("/users/1/glance?missing_data_strategy=%s", strategy), nil)
					glanceRouter.ServeHTTP(wGlance, reqGlance)
					if wGlance.Code != http.StatusOK {
						t.Errorf("glance: expected 200, got %d: %s", wGlance.Code, wGlance.Body.String())
					}
					var glanceResp models.GlanceListResponse
					if err := json.Unmarshal(wGlance.Body.Bytes(), &glanceResp); err != nil {
						t.Fatalf("glance: unmarshal: %v", err)
					}
					var found bool
					for _, p := range glanceResp.Portfolios {
						if p.PortfolioID == portfolioID {
							found = true
							if p.CurrentValue <= 0 {
								t.Errorf("glance: portfolio %d current_value=%.2f, want >0", portfolioID, p.CurrentValue)
							}
							break
						}
					}
					if !found {
						t.Errorf("glance: portfolio %d not found in response", portfolioID)
					}
				})
			}
		})
	}
}

func TestIPOBoundaryLateEdge(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	windowStart := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC) // Monday
	nMinus1 := time.Date(2026, 2, 5, 0, 0, 0, 0, time.UTC)     // Thursday — 3 pre-IPO days
	windowEnd := time.Date(2026, 2, 6, 0, 0, 0, 0, time.UTC)   // Friday   — 4 pre-IPO days
	stableInception := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	stableTicker1 := nextTicker()
	stableTicker2 := nextTicker()

	stable1ID, err := createTestSecurity(pool, stableTicker1, "IPO Late Boundary S1", models.SecurityTypeStock, &stableInception)
	if err != nil {
		t.Fatalf("create stable1: %v", err)
	}
	defer cleanupTestSecurity(pool, stableTicker1)

	stable2ID, err := createTestSecurity(pool, stableTicker2, "IPO Late Boundary S2", models.SecurityTypeStock, &stableInception)
	if err != nil {
		t.Fatalf("create stable2: %v", err)
	}
	defer cleanupTestSecurity(pool, stableTicker2)

	insertPriceRows(t, pool, stable1ID, weekdayPrices(windowStart, windowEnd, 100.0))
	insertPriceRows(t, pool, stable2ID, weekdayPrices(windowStart, windowEnd, 100.0))

	compareRouter := setupDailyValuesTestRouter(pool)
	glanceRouter := setupGlanceTestRouter(pool)

	strategies := []models.MissingDataStrategy{
		models.MissingDataStrategyCashAppreciating,
		models.MissingDataStrategyReallocate,
	}

	for _, tc := range []struct {
		name    string
		ipoDate time.Time
	}{
		{"ipo_on_nMinus1", nMinus1},
		{"ipo_on_enddate", windowEnd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lateTicker := nextTicker()
			lateID, err := createTestSecurity(pool, lateTicker, "IPO Late Boundary Late "+lateTicker, models.SecurityTypeStock, &tc.ipoDate)
			if err != nil {
				t.Fatalf("create late security: %v", err)
			}
			defer cleanupTestSecurity(pool, lateTicker)
			insertPriceRows(t, pool, lateID, weekdayPrices(tc.ipoDate, windowEnd, 200.0))

			portName := nextPortfolioName()
			cleanupTestPortfolio(pool, portName, 1)

			portfolioID, err := createTestPortfolioWithDate(pool, portName, 1, models.PortfolioTypeIdeal, []models.MembershipRequest{
				{SecurityID: stable1ID, PercentageOrShares: 1.0 / 3.0},
				{SecurityID: stable2ID, PercentageOrShares: 1.0 / 3.0},
				{SecurityID: lateID, PercentageOrShares: 1.0 / 3.0},
			}, windowStart)
			if err != nil {
				t.Fatalf("createTestPortfolioWithDate: %v", err)
			}
			t.Cleanup(func() {
				cleanupGlanceEntries(pool, portfolioID)
				cleanupTestPortfolio(pool, portName, 1)
			})

			pinBody, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: portfolioID})
			wPin := httptest.NewRecorder()
			reqPin, _ := http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(pinBody))
			reqPin.Header.Set("Content-Type", "application/json")
			glanceRouter.ServeHTTP(wPin, reqPin)
			if wPin.Code != http.StatusCreated {
				t.Fatalf("pin portfolio: expected 201, got %d: %s", wPin.Code, wPin.Body.String())
			}

			for _, strategy := range strategies {
				strategy := strategy
				t.Run(string(strategy), func(t *testing.T) {
					t.Parallel()

					body, _ := json.Marshal(models.CompareRequest{
						PortfolioA:          portfolioID,
						PortfolioB:          portfolioID,
						StartPeriod:         models.FlexibleDate{Time: windowStart},
						EndPeriod:           models.FlexibleDate{Time: windowEnd},
						MissingDataStrategy: strategy,
					})
					w := httptest.NewRecorder()
					req, _ := http.NewRequest(http.MethodPost, "/portfolios/compare", bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					compareRouter.ServeHTTP(w, req)
					if w.Code != http.StatusOK {
						t.Errorf("compare: expected 200, got %d: %s", w.Code, w.Body.String())
					}

					wGlance := httptest.NewRecorder()
					reqGlance, _ := http.NewRequest(http.MethodGet,
						fmt.Sprintf("/users/1/glance?missing_data_strategy=%s", strategy), nil)
					glanceRouter.ServeHTTP(wGlance, reqGlance)
					if wGlance.Code != http.StatusOK {
						t.Errorf("glance: expected 200, got %d: %s", wGlance.Code, wGlance.Body.String())
					}
					var glanceResp models.GlanceListResponse
					if err := json.Unmarshal(wGlance.Body.Bytes(), &glanceResp); err != nil {
						t.Fatalf("glance: unmarshal: %v", err)
					}
					var found bool
					for _, p := range glanceResp.Portfolios {
						if p.PortfolioID == portfolioID {
							found = true
							if p.CurrentValue <= 0 {
								t.Errorf("glance: portfolio %d current_value=%.2f, want >0", portfolioID, p.CurrentValue)
							}
							break
						}
					}
					if !found {
						t.Errorf("glance: portfolio %d not found in response", portfolioID)
					}
				})
			}
		})
	}
}

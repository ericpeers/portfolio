package tests

// TestGlanceMatchesCompareWithSplits is a permanent end-to-end regression test
// covering the split-handling bugs found and fixed in March 2026.
//
// The central assertion: /glance and /portfolios/compare must report the same
// terminal value for the same portfolio, regardless of which startDate is used.
// Before the fixes, /glance inflated values by ~35% and compare diverged when
// the start date fell on either side of a split.
//
// Three bugs exercised:
//
//  1. Reverse split (coeff < 1.0) in [created_at, snapshotted_at):
//     Old guard `> 1.0` skipped reversal; forward loop halved shares again.
//     Fix: guard changed to `!= 0 && != 1.0`.
//
//  2. Forward split in [snapshotted_at, compareStart):
//     Split fell outside the fetched price window so the forward loop never
//     applied it, leaving shares at the pre-split (lower) count.
//     Fix: gap-apply branch fetches and applies splits in [snapshotted_at, startDate).
//
//  3. Glance uses created_at as startDate (years in the past); compare uses a
//     recent user-supplied startDate. Both must converge to the same end value.
//
// Timeline (Jan 2025):
//
//	Jan  2  created_at
//	Jan 10  TGCE1 reverse split (coeff=0.5): price $50→$100, shares 20→10
//	Jan 15  snapshotted_at  — shares recorded: 10 TGCE1 (post-reverse), 10 TGCE2 (pre-forward)
//	Jan 20  TGCE2 forward split (coeff=2.0): price $100→$50, shares 10→20
//	Jan 25  "late" compare start (both splits are now pre-window)
//	Jan 31  compare end / last price date (glance forward-fills to today)
//
// Expected terminal value at Jan 31:
//
//	TGCE1:  10 shares × $100 = $1000
//	TGCE2:  20 shares × $50  = $1000
//	total                      $2000
//
// All three paths must return $2000:
//   - /glance                    (startDate = created_at = Jan 2)
//   - /compare from Jan 2        (full history, should match glance exactly)
//   - /compare from Jan 25       (late start; both splits are pre-window)
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
)

func TestGlanceMatchesCompareWithSplits(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// --- Securities ---

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// TGCE1: will undergo a 1-for-2 reverse split on Jan 10.
	// Price: $50 Jan 2-9, $100 Jan 10 onward (basePrice / 0.5 = 100).
	sec1, err := createTestSecurity(pool, "TGCE1", "Glance Compare E2E 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create TGCE1: %v", err)
	}
	defer cleanupTestSecurity(pool, "TGCE1")

	// TGCE2: will undergo a 2-for-1 forward split on Jan 20.
	// Price: $100 Jan 2-19, $50 Jan 20 onward (basePrice / 2.0 = 50).
	sec2, err := createTestSecurity(pool, "TGCE2", "Glance Compare E2E 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create TGCE2: %v", err)
	}
	defer cleanupTestSecurity(pool, "TGCE2")

	// --- Price data ---

	priceStart := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	priceEnd := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)
	reverseSplitDate := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	forwardSplitDate := time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC)

	if err := insertPriceDataWithSplit(pool, sec1, priceStart, priceEnd, 50.0, reverseSplitDate, 0.5); err != nil {
		t.Fatalf("insert TGCE1 prices: %v", err)
	}
	if err := insertSplitEvent(pool, sec1, reverseSplitDate, 0.5); err != nil {
		t.Fatalf("insert TGCE1 split event: %v", err)
	}

	if err := insertPriceDataWithSplit(pool, sec2, priceStart, priceEnd, 100.0, forwardSplitDate, 2.0); err != nil {
		t.Fatalf("insert TGCE2 prices: %v", err)
	}
	if err := insertSplitEvent(pool, sec2, forwardSplitDate, 2.0); err != nil {
		t.Fatalf("insert TGCE2 split event: %v", err)
	}

	// --- Portfolio ---
	// Shares reflect the state at snapshotted_at (Jan 15):
	//   TGCE1: 10 (post-reverse-split; the reverse split on Jan 10 has already happened)
	//   TGCE2: 10 (pre-forward-split; the forward split on Jan 20 hasn't happened yet)

	cleanupTestPortfolio(pool, "Glance Compare E2E Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Glance Compare E2E Portfolio", 1)

	createdAt := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	snapshottedAt := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	portfolioID, err := createTestPortfolio(pool, "Glance Compare E2E Portfolio", 1, models.PortfolioTypeActive,
		[]models.MembershipRequest{
			{SecurityID: sec1, PercentageOrShares: 10},
			{SecurityID: sec2, PercentageOrShares: 10},
		})
	if err != nil {
		t.Fatalf("create portfolio: %v", err)
	}

	_, err = pool.Exec(ctx,
		`UPDATE portfolio SET created_at = $1, snapshotted_at = $2 WHERE id = $3`,
		createdAt, snapshottedAt, portfolioID)
	if err != nil {
		t.Fatalf("set created_at / snapshotted_at: %v", err)
	}

	// --- Routers ---

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	glanceRouter := setupGlanceTestRouter(pool, avClient)
	compareRouter := setupDailyValuesTestRouter(pool, avClient)

	// --- Pin portfolio for glance ---
	// Pre-clean in case a previous run left a stale entry, then defer cleanup.
	pool.Exec(ctx, `DELETE FROM portfolio_glance WHERE portfolio_id = $1`, portfolioID)
	defer pool.Exec(ctx, `DELETE FROM portfolio_glance WHERE portfolio_id = $1`, portfolioID)

	pinBody, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: portfolioID})
	wPin := httptest.NewRecorder()
	reqPin, _ := http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(pinBody))
	reqPin.Header.Set("Content-Type", "application/json")
	glanceRouter.ServeHTTP(wPin, reqPin)
	if wPin.Code != http.StatusCreated {
		t.Fatalf("pin portfolio: expected 201, got %d: %s", wPin.Code, wPin.Body.String())
	}

	// --- /glance ---

	wGlance := httptest.NewRecorder()
	reqGlance, _ := http.NewRequest(http.MethodGet, "/users/1/glance", nil)
	glanceRouter.ServeHTTP(wGlance, reqGlance)
	if wGlance.Code != http.StatusOK {
		t.Fatalf("GET /glance: expected 200, got %d: %s", wGlance.Code, wGlance.Body.String())
	}

	var glanceResp models.GlanceListResponse
	if err := json.Unmarshal(wGlance.Body.Bytes(), &glanceResp); err != nil {
		t.Fatalf("unmarshal glance response: %v", err)
	}

	var glancePortfolio *models.GlancePortfolio
	for i := range glanceResp.Portfolios {
		if glanceResp.Portfolios[i].PortfolioID == portfolioID {
			glancePortfolio = &glanceResp.Portfolios[i]
			break
		}
	}
	if glancePortfolio == nil {
		t.Fatalf("portfolio %d not found in /glance response", portfolioID)
	}
	glanceValue := glancePortfolio.CurrentValue
	t.Logf("/glance current_value = %.2f  (valuation_date=%s)", glanceValue, glancePortfolio.ValuationDate)

	// --- /compare from created_at (full history, should match glance) ---

	fullStart := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	fullEnd := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)

	compareFullValue := callCompareEndValue(t, compareRouter, portfolioID, fullStart, fullEnd)
	t.Logf("/compare (Jan 2 start) end_value = %.2f", compareFullValue)

	// --- /compare from late start (both splits are pre-window) ---

	lateStart := time.Date(2025, 1, 25, 0, 0, 0, 0, time.UTC)
	compareLateValue := callCompareEndValue(t, compareRouter, portfolioID, lateStart, fullEnd)
	t.Logf("/compare (Jan 25 start) end_value = %.2f", compareLateValue)

	// --- Assertions ---

	const wantValue = 2000.0 // TGCE1: 10×$100 + TGCE2: 20×$50
	const epsilon = 0.01

	if math.Abs(glanceValue-wantValue) > epsilon {
		t.Errorf("/glance current_value = %.2f, want %.2f\n"+
			"  (if ~1500: forward split on TGCE2 was missed; if ~2500 or ~3000: reverse split on TGCE1 was double-counted)",
			glanceValue, wantValue)
	}
	if math.Abs(compareFullValue-wantValue) > epsilon {
		t.Errorf("/compare (full range) end_value = %.2f, want %.2f", compareFullValue, wantValue)
	}
	if math.Abs(compareLateValue-wantValue) > epsilon {
		t.Errorf("/compare (late start, both splits pre-window) end_value = %.2f, want %.2f\n"+
			"  (if ~1500: forward split on TGCE2 was missed in the gap-apply branch;\n"+
			"   if ~500:  reverse split on TGCE1 was double-applied because the guard was `> 1.0`)",
			compareLateValue, wantValue)
	}

	// All three paths must agree with each other, not just with the constant.
	if math.Abs(glanceValue-compareFullValue) > epsilon {
		t.Errorf("/glance (%.2f) != /compare-full (%.2f): paths diverge", glanceValue, compareFullValue)
	}
	if math.Abs(compareFullValue-compareLateValue) > epsilon {
		t.Errorf("/compare-full (%.2f) != /compare-late (%.2f): start date changes end value", compareFullValue, compareLateValue)
	}
}

// callCompareEndValue posts to /portfolios/compare and returns portfolio A's end value.
// The portfolio is compared against itself to isolate its own value calculation.
func callCompareEndValue(t *testing.T, router interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
}, portfolioID int64, startDate, endDate time.Time) float64 {
	t.Helper()
	body, _ := json.Marshal(models.CompareRequest{
		PortfolioA:  portfolioID,
		PortfolioB:  portfolioID,
		StartPeriod: models.FlexibleDate{Time: startDate},
		EndPeriod:   models.FlexibleDate{Time: endDate},
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /portfolios/compare (%s–%s): got %d: %s",
			startDate.Format("Jan 2"), endDate.Format("Jan 2"), w.Code, w.Body.String())
	}
	var resp models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal compare response: %v", err)
	}
	return resp.PerformanceMetrics.PortfolioAMetrics.EndValue
}

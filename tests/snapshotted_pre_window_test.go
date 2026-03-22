package tests

// TestSnapshottedSplitBeforeCompareWindow exercises the case where a split occurs
// AFTER snapshotted_at but BEFORE the compare date range starts.
//
// Timeline:
//   Jan 6  — created_at (~10 days before end)
//   Jan 10 — snapshotted_at; shares recorded: 10 of TSNCMP1, 20 of TSNCMP2
//   Jan 13 — 2-for-1 split on TSNCMP1 (price: $200→$100; correct shares: 10→20)
//   Jan 15 — compare start  (split is now OUTSIDE the fetched price window)
//   Jan 17 — compare end
//
// Expected end value:
//   TSNCMP1: 20 shares × $100 = $2000
//   TSNCMP2: 20 shares ×  $50 = $1000
//   total                       $3000
//
// Bug (before fix): split on Jan 13 is before startDate so GetDailyPrices never
// fetches it. sharesMap is initialised with snapshot value (10), the split is
// never applied, and value = 10×$100 + 20×$50 = $2000 — off by $1000.

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

func TestSnapshottedSplitBeforeCompareWindow(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// TSNCMP1 will have a 2-for-1 split on Jan 13.
	sec1, err := createTestSecurity(pool, "TSNCMP1", "Snapshot Compare 1", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create TSNCMP1: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSNCMP1")

	// TSNCMP2 is stable — no split.
	sec2, err := createTestSecurity(pool, "TSNCMP2", "Snapshot Compare 2", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("create TSNCMP2: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSNCMP2")

	// Price data covering the full span (created_at through compare end).
	priceStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	priceEnd := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)
	splitDate := time.Date(2025, 1, 13, 0, 0, 0, 0, time.UTC)

	// TSNCMP1: $200 before split, $100 from split date onward.
	if err := insertPriceDataWithSplit(pool, sec1, priceStart, priceEnd, 200.0, splitDate, 2.0); err != nil {
		t.Fatalf("insert TSNCMP1 prices: %v", err)
	}
	if err := insertSplitEvent(pool, sec1, splitDate, 2.0); err != nil {
		t.Fatalf("insert TSNCMP1 split: %v", err)
	}

	// TSNCMP2: constant $50. Use insertPriceDataWithSplit with a no-op split far
	// in the future so the close price is exactly $50 (insertPriceData adds +2).
	if err := insertPriceDataWithSplit(pool, sec2, priceStart, priceEnd, 50.0, priceEnd.AddDate(1, 0, 0), 1.0); err != nil {
		t.Fatalf("insert TSNCMP2 prices: %v", err)
	}

	// Portfolio A: the portfolio under test.
	// Snapshot shares (recorded Jan 10, pre-split): 10 of TSNCMP1, 20 of TSNCMP2.
	cleanupTestPortfolio(pool, "SnapCmp Portfolio A", 1)
	defer cleanupTestPortfolio(pool, "SnapCmp Portfolio A", 1)

	portfolioAID, err := createTestPortfolio(pool, "SnapCmp Portfolio A", 1, models.PortfolioTypeActive,
		[]models.MembershipRequest{
			{SecurityID: sec1, PercentageOrShares: 10}, // pre-split count
			{SecurityID: sec2, PercentageOrShares: 20},
		})
	if err != nil {
		t.Fatalf("create portfolio A: %v", err)
	}

	createdAt := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	snapshottedAt := time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC)
	_, err = pool.Exec(ctx,
		`UPDATE portfolio SET created_at = $1, snapshotted_at = $2 WHERE id = $3`,
		createdAt, snapshottedAt, portfolioAID)
	if err != nil {
		t.Fatalf("set created_at/snapshotted_at on portfolio A: %v", err)
	}

	// Portfolio B: simple reference — 20 shares of the stable TSNCMP2 only.
	// Values are flat at $50×20 = $1000 throughout.
	cleanupTestPortfolio(pool, "SnapCmp Portfolio B", 1)
	defer cleanupTestPortfolio(pool, "SnapCmp Portfolio B", 1)

	portfolioBID, err := createTestPortfolio(pool, "SnapCmp Portfolio B", 1, models.PortfolioTypeActive,
		[]models.MembershipRequest{
			{SecurityID: sec2, PercentageOrShares: 20},
		})
	if err != nil {
		t.Fatalf("create portfolio B: %v", err)
	}

	// Compare range: Jan 15–17 (split on Jan 13 is OUTSIDE this window).
	compareStart := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	compareEnd := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC)

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupDailyValuesTestRouter(pool, avClient)

	reqBody := models.CompareRequest{
		PortfolioA:  portfolioAID,
		PortfolioB:  portfolioBID,
		StartPeriod: models.FlexibleDate{Time: compareStart},
		EndPeriod:   models.FlexibleDate{Time: compareEnd},
	}
	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("compare returned %d: %s", w.Code, w.Body.String())
	}

	var resp models.CompareResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal compare response: %v", err)
	}

	// Portfolio B is easy to verify: flat $1000 throughout.
	const epsilon = 0.01
	wantB := 1000.0
	if math.Abs(resp.PerformanceMetrics.PortfolioBMetrics.EndValue-wantB) > epsilon {
		t.Errorf("portfolio B end value = %.2f, want %.2f", resp.PerformanceMetrics.PortfolioBMetrics.EndValue, wantB)
	}

	// Portfolio A: split on Jan 13 doubled TSNCMP1 shares (10→20) at $100, plus TSNCMP2 at $50×20.
	// Correct end value = 20×$100 + 20×$50 = $3000.
	// Bug end value     = 10×$100 + 20×$50 = $2000 (split missed because it's pre-window).
	wantA := 3000.0
	gotA := resp.PerformanceMetrics.PortfolioAMetrics.EndValue
	if math.Abs(gotA-wantA) > epsilon {
		t.Errorf("portfolio A end value = %.2f, want %.2f\n"+
			"  (if got 2000.00, the split on Jan 13 was not applied because it falls\n"+
			"  between snapshotted_at (Jan 10) and compare start (Jan 15))",
			gotA, wantA)
	}

	t.Logf("Portfolio A end value = %.2f (want %.2f)", gotA, wantA)
	t.Logf("Portfolio B end value = %.2f (want %.2f)", resp.PerformanceMetrics.PortfolioBMetrics.EndValue, wantB)
}

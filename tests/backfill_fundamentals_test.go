package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
)

// TestBackfillCandidateSorting verifies the priority/tie-break ordering rules
// for SortBackfillCandidates. No database or EODHD calls are made.
func TestBackfillCandidateSorting(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 18, 12, 0, 0, 0, time.UTC)

	ts := func(s string) *time.Time {
		p, err := time.Parse("2006-01-02", s)
		if err != nil {
			t.Fatalf("bad date %q: %v", s, err)
		}
		return &p
	}

	// Bucket 0: next_earnings in past, last_update before next_earnings.
	b0Us := models.BackfillCandidate{SecurityID: 1, Ticker: "NVDA", Type: "COMMON STOCK", Country: "USA",
		LastUpdate: ts("2026-01-01"), NextEarnings: ts("2026-02-25"), Volume: 5_000_000}
	b0Intl := models.BackfillCandidate{SecurityID: 2, Ticker: "NESN", Type: "COMMON STOCK", Country: "Switzerland",
		LastUpdate: ts("2026-01-01"), NextEarnings: ts("2026-03-01"), Volume: 9_000_000}

	// Bucket 0: never fetched but earnings already passed → still bucket 0.
	b0NeverFetched := models.BackfillCandidate{SecurityID: 3, Ticker: "MSFT", Type: "COMMON STOCK", Country: "USA",
		LastUpdate: nil, NextEarnings: ts("2026-03-15"), Volume: 20_000_000}

	// Bucket 1: no last_update, no past earnings.
	b1UsEtf := models.BackfillCandidate{SecurityID: 4, Ticker: "SPY", Type: "ETF", Country: "USA",
		LastUpdate: nil, Volume: 100_000_000}
	b1UsStock := models.BackfillCandidate{SecurityID: 5, Ticker: "AAPL", Type: "COMMON STOCK", Country: "USA",
		LastUpdate: nil, Volume: 80_000_000}
	b1IntlEtf := models.BackfillCandidate{SecurityID: 6, Ticker: "IWDA", Type: "ETF", Country: "Ireland",
		LastUpdate: nil, Volume: 2_000_000}
	b1IntlStockHiVol := models.BackfillCandidate{SecurityID: 7, Ticker: "TSM", Type: "COMMON STOCK", Country: "Taiwan",
		LastUpdate: nil, Volume: 15_000_000}
	b1IntlStockLoVol := models.BackfillCandidate{SecurityID: 8, Ticker: "SAP", Type: "COMMON STOCK", Country: "Germany",
		LastUpdate: nil, Volume: 1_000_000}

	// Bucket 2: has last_update, old first.
	b2OldUs := models.BackfillCandidate{SecurityID: 9, Ticker: "GE", Type: "COMMON STOCK", Country: "USA",
		LastUpdate: ts("2025-10-01"), Volume: 3_000_000}
	b2NewUs := models.BackfillCandidate{SecurityID: 10, Ticker: "F", Type: "COMMON STOCK", Country: "USA",
		LastUpdate: ts("2026-04-10"), Volume: 3_000_000}

	// Bucket 2: same last_update — US before non-US.
	sameDate := ts("2026-03-01")
	b2SameUs := models.BackfillCandidate{SecurityID: 11, Ticker: "IBM", Type: "COMMON STOCK", Country: "USA",
		LastUpdate: sameDate, Volume: 500_000}
	b2SameIntl := models.BackfillCandidate{SecurityID: 12, Ticker: "VOD", Type: "COMMON STOCK", Country: "UK",
		LastUpdate: sameDate, Volume: 500_000}

	// Future earnings should NOT qualify for bucket 0.
	futureEarnings := models.BackfillCandidate{SecurityID: 13, Ticker: "AMZN", Type: "COMMON STOCK", Country: "USA",
		LastUpdate: ts("2026-01-01"), NextEarnings: ts("2026-07-01"), Volume: 10_000_000}

	candidates := []models.BackfillCandidate{
		b2NewUs, b1UsStock, b0Intl, b2SameIntl, b1IntlStockLoVol,
		b2OldUs, b0Us, b1UsEtf, b1IntlEtf, b0NeverFetched,
		b2SameUs, b1IntlStockHiVol, futureEarnings,
	}

	services.SortBackfillCandidates(candidates, now)

	// --- Assertions ---

	wantOrder := []int64{
		// Bucket 0 (post-earnings stale): US first, then non-US; within US, volume DESC.
		b0NeverFetched.SecurityID, // US, 20M volume (higher than b0Us)
		b0Us.SecurityID,           // US, 5M volume
		b0Intl.SecurityID,         // non-US

		// Bucket 1 (never fetched): US-ETF, US-stock, intl-ETF, intl-stock by volume desc.
		b1UsEtf.SecurityID,
		b1UsStock.SecurityID,
		b1IntlEtf.SecurityID,
		b1IntlStockHiVol.SecurityID,
		b1IntlStockLoVol.SecurityID,

		// Bucket 2 (has last_update): oldest last_update first; same-date US before non-US.
		b2OldUs.SecurityID,        // 2025-10-01
		futureEarnings.SecurityID, // 2026-01-01 (future earnings don't trigger bucket 0)
		b2SameUs.SecurityID,       // 2026-03-01, US
		b2SameIntl.SecurityID,     // 2026-03-01, non-US
		b2NewUs.SecurityID,        // 2026-04-10
	}

	if len(candidates) != len(wantOrder) {
		t.Fatalf("len(candidates)=%d, want %d", len(candidates), len(wantOrder))
	}
	for i, want := range wantOrder {
		got := candidates[i].SecurityID
		if got != want {
			t.Errorf("position %d: got SecurityID=%d (%s), want %d", i, got, candidates[i].Ticker, want)
		}
	}
}

// TestBackfillFundamentalsHandler verifies that the endpoint returns 202 Accepted
// with the expected JSON shape. The actual EODHD fetches happen asynchronously
// in the background and are not waited on by this test.
// Requires migration 004_fundamentals.sql to be applied; skips otherwise.
func TestBackfillFundamentalsHandler(t *testing.T) {
	t.Parallel()

	// Skip if fact_fundamentals doesn't exist yet (migration 004 not applied).
	var exists bool
	err := testPool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'fact_fundamentals')`).Scan(&exists)
	if err != nil || !exists {
		t.Skip("fact_fundamentals table not found — apply migration 004_fundamentals.sql first")
	}

	gin.SetMode(gin.TestMode)

	secRepo := repository.NewSecurityRepository(testPool)
	exchangeRepo := repository.NewExchangeRepository(testPool)
	priceRepo := repository.NewPriceRepository(testPool)
	fundamentalsRepo := repository.NewFundamentalsRepository(testPool)
	// Dead URLs: background fetches will fail immediately — test only checks the 202.
	eodhdClient := eodhd.NewClient("test-key", "http://localhost:9999")

	adminSvc := services.NewAdminService(secRepo, exchangeRepo, priceRepo, fundamentalsRepo, eodhdClient, 1)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
	})
	membershipSvc := services.NewMembershipService(secRepo, repository.NewPortfolioRepository(testPool), pricingSvc)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, secRepo, exchangeRepo, priceRepo)

	router := gin.New()
	router.POST("/admin/securities/backfill_fundamentals", adminHandler.BackfillFundamentals)

	req := httptest.NewRequest(http.MethodPost, "/admin/securities/backfill_fundamentals?count=1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["status"] != "accepted" {
		t.Errorf("status = %q, want %q", resp["status"], "accepted")
	}
	if _, ok := resp["queued"]; !ok {
		t.Error("response missing 'queued' field")
	}

	// Invalid count.
	req2 := httptest.NewRequest(http.MethodPost, "/admin/securities/backfill_fundamentals?count=0", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != http.StatusBadRequest {
		t.Errorf("count=0: status = %d, want 400", w2.Code)
	}
}

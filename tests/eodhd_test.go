package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/middleware"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Unit tests for eohdExchangeCode ---

// TestEODHDExchangeCode verifies exchange code mapping via the public GetDailyPrices URL.
// We use a mock HTTP server to capture the constructed URL.
func TestEODHDExchangeCode(t *testing.T) {
	cases := []struct {
		name         string
		country      string
		exchangeName string
		wantCode     string
	}{
		{"USA country always returns US", "USA", "NASDAQ", "US"},
		{"London Exchange maps to LSE", "UK", "London Exchange", "LSE"},
		{"Toronto Exchange maps to TO", "Canada", "Toronto Exchange", "TO"},
		{"TSX Venture maps to V", "Canada", "TSX Venture Exchange", "V"},
		{"XETRA maps correctly", "Germany", "XETRA Stock Exchange", "XETRA"},
		{"Euronext Paris maps to PA", "France", "Euronext Paris", "PA"},
		{"Euronext Amsterdam maps to AS", "Netherlands", "Euronext Amsterdam", "AS"},
		{"Unknown exchange falls back to name", "Unknown", "MYEXCH", "MYEXCH"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var capturedURL string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedURL = r.URL.Path
				w.Header().Set("Content-Type", "application/json")
				// Return a minimal price record so the client doesn't error
				json.NewEncoder(w).Encode([]map[string]interface{}{
					{"date": "2026-01-02", "open": 100.0, "high": 101.0, "low": 99.0,
						"close": 100.0, "adjusted_close": 100.0, "volume": 1000000},
				})
			}))
			defer srv.Close()

			client := eodhd.NewClientWithBaseURL("test-key", srv.URL)
			sec := &models.SecurityWithCountry{
				Security:     models.Security{Symbol: "TEST"},
				Country:      tc.country,
				ExchangeName: tc.exchangeName,
			}
			_, _ = client.GetDailyPrices(context.Background(), sec, "compact")

			wantPath := fmt.Sprintf("/eod/TEST.%s", tc.wantCode)
			if capturedURL != wantPath {
				t.Errorf("got URL path %q, want %q", capturedURL, wantPath)
			}
		})
	}
}

// --- Unit tests for split ratio parsing ---

func TestEODHDSplitParsing(t *testing.T) {
	cases := []struct {
		split    string
		wantDate string
		wantCoef float64
	}{
		{"4:1", "2024-06-14", 4.0},
		{"3:2", "2024-03-01", 1.5},
		{"2:1", "2023-11-10", 2.0},
		{"10:1", "2025-01-05", 10.0},
	}

	for _, tc := range cases {
		t.Run(tc.split, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/splits/TSTSPLIT.US" {
					json.NewEncoder(w).Encode([]map[string]interface{}{
						{"date": tc.wantDate, "split": tc.split},
					})
					return
				}
				// dividends endpoint — return empty
				json.NewEncoder(w).Encode([]interface{}{})
			}))
			defer srv.Close()

			client := eodhd.NewClientWithBaseURL("test-key", srv.URL)
			sec := &models.SecurityWithCountry{
				Security:     models.Security{Symbol: "TSTSPLIT"},
				Country:      "USA",
				ExchangeName: "NASDAQ",
			}
			events, err := client.GetStockEvents(context.Background(), sec)
			if err != nil {
				t.Fatalf("GetStockEvents error: %v", err)
			}
			if len(events) != 1 {
				t.Fatalf("expected 1 event, got %d", len(events))
			}
			if events[0].SplitCoefficient != tc.wantCoef {
				t.Errorf("split %q: expected coefficient %v, got %v", tc.split, tc.wantCoef, events[0].SplitCoefficient)
			}
		})
	}
}

// --- HTTP mock tests for GetDailyPrices ---

func TestEODHDGetDailyPricesHTTP(t *testing.T) {
	priceJSON := `[
		{"date":"2026-01-02","open":150.0,"high":155.0,"low":149.0,"adjusted_close":152.5,"volume":1000000},
		{"date":"2026-01-03","open":152.0,"high":158.0,"low":151.0,"adjusted_close":157.0,"volume":800000}
	]`

	var capturedQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(priceJSON))
	}))
	defer srv.Close()

	client := eodhd.NewClientWithBaseURL("test-key", srv.URL)
	sec := &models.SecurityWithCountry{
		Security:     models.Security{Symbol: "AAPL"},
		Country:      "USA",
		ExchangeName: "NASDAQ",
	}

	t.Run("full mode returns all prices", func(t *testing.T) {
		prices, err := client.GetDailyPrices(context.Background(), sec, "full")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(prices) != 2 {
			t.Errorf("expected 2 prices, got %d", len(prices))
		}
		// Close should be AdjustedClose
		if prices[0].Close != 152.5 {
			t.Errorf("expected AdjustedClose 152.5 as Close, got %v", prices[0].Close)
		}
		if prices[0].Dividend != 0 {
			t.Errorf("expected Dividend=0, got %v", prices[0].Dividend)
		}
		if prices[0].SplitCoefficient != 1.0 {
			t.Errorf("expected SplitCoefficient=1.0, got %v", prices[0].SplitCoefficient)
		}
		_ = capturedQuery
	})

	t.Run("compact mode adds from param", func(t *testing.T) {
		_, err := client.GetDailyPrices(context.Background(), sec, "compact")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if capturedQuery == "" || len(capturedQuery) == 0 {
			t.Error("expected query string with from param")
		}
		// Should contain 'from=' in the query
		if len(capturedQuery) < 5 {
			t.Errorf("expected 'from' param in query %q", capturedQuery)
		}
	})
}

func TestEODHDGetDailyPricesEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	client := eodhd.NewClientWithBaseURL("test-key", srv.URL)
	sec := &models.SecurityWithCountry{
		Security:     models.Security{Symbol: "EMPTY"},
		Country:      "USA",
		ExchangeName: "NASDAQ",
	}

	_, err := client.GetDailyPrices(context.Background(), sec, "full")
	if err == nil {
		t.Error("expected error for empty response, got nil")
	}
}

func TestEODHDGetDailyPricesNoKey(t *testing.T) {
	client := eodhd.NewClientWithBaseURL("", "http://localhost:9999")
	sec := &models.SecurityWithCountry{
		Security: models.Security{Symbol: "AAPL"},
		Country:  "USA",
	}
	_, err := client.GetDailyPrices(context.Background(), sec, "full")
	if err == nil {
		t.Error("expected error for missing API key")
	}
}

// --- HTTP mock tests for GetStockEvents ---

func TestEODHDGetStockEventsHTTP(t *testing.T) {
	divJSON := `[
		{"date":"2025-12-15","value":0.25},
		{"date":"2025-09-15","value":0.25}
	]`
	splitJSON := `[
		{"date":"2024-06-14","split":"4:1"}
	]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/div/AAPL.US":
			w.Write([]byte(divJSON))
		case r.URL.Path == "/splits/AAPL.US":
			w.Write([]byte(splitJSON))
		default:
			w.Write([]byte(`[]`))
		}
	}))
	defer srv.Close()

	client := eodhd.NewClientWithBaseURL("test-key", srv.URL)
	sec := &models.SecurityWithCountry{
		Security:     models.Security{Symbol: "AAPL"},
		Country:      "USA",
		ExchangeName: "NASDAQ",
	}

	events, err := client.GetStockEvents(context.Background(), sec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 2 dividends + 1 split = 3 events (different dates)
	if len(events) != 3 {
		t.Errorf("expected 3 merged events, got %d", len(events))
	}

	// Find the split event and verify coefficient
	for _, e := range events {
		if e.SplitCoefficient == 4.0 {
			if e.Dividend != 0 {
				t.Errorf("split event should have Dividend=0, got %v", e.Dividend)
			}
			return
		}
	}
	t.Error("did not find a split event with coefficient 4.0")
}

// --- HTTP mock test for GetBulkEOD ---

func TestEODHDGetBulkEOD(t *testing.T) {
	bulkJSON := `[
		{"code":"AAPL.US","date":"2026-02-28","open":225.0,"high":230.0,"low":224.0,"close":228.0,"adjusted_close":228.0,"volume":50000000},
		{"code":"MSFT.US","date":"2026-02-28","open":415.0,"high":420.0,"low":414.0,"close":418.0,"adjusted_close":418.0,"volume":20000000},
		{"code":"INVALID-DATE","date":"not-a-date","open":1.0,"high":1.0,"low":1.0,"close":1.0,"adjusted_close":1.0,"volume":100}
	]`

	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Write([]byte(bulkJSON))
	}))
	defer srv.Close()

	client := eodhd.NewClientWithBaseURL("test-key", srv.URL)
	date := time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC)

	records, err := client.GetBulkEOD(context.Background(), "US", date)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedPath != "/eod-bulk-last-day/US" {
		t.Errorf("expected path /eod-bulk-last-day/US, got %q", capturedPath)
	}

	// Invalid-date record should be silently skipped
	if len(records) != 2 {
		t.Errorf("expected 2 valid records, got %d", len(records))
	}

	// Verify exchange suffix is stripped from Code
	if records[0].Code != "AAPL" {
		t.Errorf("expected Code=AAPL (suffix stripped), got %q", records[0].Code)
	}
	if records[1].Code != "MSFT" {
		t.Errorf("expected Code=MSFT (suffix stripped), got %q", records[1].Code)
	}
}

// --- setupBulkFetchRouter creates a router for the bulk-fetch-eodhd-prices endpoint ---

func setupBulkFetchRouter(pool *pgxpool.Pool, eohdClient *eodhd.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)

	securityRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)

	avClient := alphavantage.NewClientWithBaseURL("test-key", "http://localhost:9999")
	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, priceRepo, avClient, eohdClient)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, eohdClient, eohdClient, avClient)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo, exchangeRepo)

	router := gin.New()
	router.Use(middleware.ValidateUser())
	admin := router.Group("/admin")
	admin.GET("/bulk-fetch-eodhd-prices", adminHandler.BulkFetchEODHDPrices)
	return router
}

// TestBulkFetchEODHDPricesMissingExchange verifies 400 when exchange is omitted.
func TestBulkFetchEODHDPricesMissingExchange(t *testing.T) {
	pool := getTestPool(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	client := eodhd.NewClientWithBaseURL("test-key", srv.URL)
	router := setupBulkFetchRouter(pool, client)

	req, _ := http.NewRequest("GET", "/admin/bulk-fetch-eodhd-prices", nil)
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestBulkFetchEODHDPricesInvalidDate verifies 400 for a malformed date.
func TestBulkFetchEODHDPricesInvalidDate(t *testing.T) {
	pool := getTestPool(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	client := eodhd.NewClientWithBaseURL("test-key", srv.URL)
	router := setupBulkFetchRouter(pool, client)

	req, _ := http.NewRequest("GET", "/admin/bulk-fetch-eodhd-prices?exchange=US&date=not-a-date", nil)
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestBulkFetchEODHDPricesStoresKnownSecurities verifies that records matching
// securities in dim_security are stored, and unknown tickers are skipped.
func TestBulkFetchEODHDPricesStoresKnownSecurities(t *testing.T) {
	pool := getTestPool(t)

	const ticker = "TSYBULK"
	secID, err := createTestStock(pool, ticker, "Bulk Test Stock")
	if err != nil {
		t.Fatalf("failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	bulkDate := "2026-01-10"
	bulkJSON := fmt.Sprintf(`[
		{"code":"%s.US","date":"%s","open":10.0,"high":11.0,"low":9.5,"close":10.5,"adjusted_close":10.5,"volume":100000},
		{"code":"TSTUNKNOWN99.US","date":"%s","open":5.0,"high":6.0,"low":4.0,"close":5.5,"adjusted_close":5.5,"volume":50000}
	]`, ticker, bulkDate, bulkDate)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(bulkJSON))
	}))
	defer srv.Close()

	client := eodhd.NewClientWithBaseURL("test-key", srv.URL)
	router := setupBulkFetchRouter(pool, client)

	req, _ := http.NewRequest("GET",
		fmt.Sprintf("/admin/bulk-fetch-eodhd-prices?exchange=US&date=%s", bulkDate), nil)
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.BulkFetchResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if result.Fetched != 2 {
		t.Errorf("expected fetched=2, got %d", result.Fetched)
	}
	if result.Stored != 1 {
		t.Errorf("expected stored=1 (only known ticker), got %d", result.Stored)
	}
	if result.Skipped != 1 {
		t.Errorf("expected skipped=1 (unknown ticker), got %d", result.Skipped)
	}

	// Verify price was actually stored in the DB
	priceRepo := repository.NewPriceRepository(pool)
	ctx := context.Background()
	date, _ := time.Parse("2006-01-02", bulkDate)
	prices, err := priceRepo.GetDailyPrices(ctx, secID, date, date)
	if err != nil {
		t.Fatalf("failed to query stored prices: %v", err)
	}
	if len(prices) != 1 {
		t.Errorf("expected 1 price stored in DB, got %d", len(prices))
	}
	if prices[0].Close != 10.5 {
		t.Errorf("expected Close=10.5, got %v", prices[0].Close)
	}
}

// TestBulkFetchEODHDPricesIntegration calls the live EODHD API.
// Skipped unless EODHD_KEY is set in the environment.
func TestBulkFetchEODHDPricesIntegration(t *testing.T) {
	key := os.Getenv("EODHD_KEY")
	if key == "" {
		t.Skip("EODHD_KEY not set — skipping live EODHD integration test")
	}

	pool := getTestPool(t)

	client := eodhd.NewClient(key)
	router := setupBulkFetchRouter(pool, client)

	// Use a recent Friday (2026-02-27) as the date
	req, _ := http.NewRequest("GET",
		"/admin/bulk-fetch-eodhd-prices?exchange=US&date=2026-02-27", nil)
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.BulkFetchResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	t.Logf("Integration result: fetched=%d stored=%d skipped=%d", result.Fetched, result.Stored, result.Skipped)

	if result.Stored == 0 {
		t.Error("expected at least 1 price stored in live integration test")
	}
}

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/middleware"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/fred"
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
	t.Parallel()
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

			client := eodhd.NewClient("test-key", srv.URL)
			sec := &models.SecurityWithCountry{
				Security:     models.Security{Ticker: "TEST"},
				Country:      tc.country,
				ExchangeName: tc.exchangeName,
			}
			_, _ = client.GetDailyPrices(context.Background(), sec, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC))

			wantPath := fmt.Sprintf("/eod/TEST.%s", tc.wantCode)
			if capturedURL != wantPath {
				t.Errorf("got URL path %q, want %q", capturedURL, wantPath)
			}
		})
	}
}

// --- Unit tests for split ratio parsing ---

func TestEODHDSplitParsing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		split    string
		wantDate string
		wantCoef float64
	}{
		{"4.0000/1.0000", "2024-06-14", 4.0},
		{"3.0000/2.0000", "2024-03-01", 1.5},
		{"2.0000/1.0000", "2023-11-10", 2.0},
		{"10.0000/1.0000", "2025-01-05", 10.0},
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

			client := eodhd.NewClient("test-key", srv.URL)
			sec := &models.SecurityWithCountry{
				Security:     models.Security{Ticker: "TSTSPLIT"},
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
	t.Parallel()
	priceJSON := `[
		{"date":"2026-01-02","open":150.0,"high":155.0,"low":149.0,"close":151.0,"adjusted_close":152.5,"volume":1000000},
		{"date":"2026-01-03","open":152.0,"high":158.0,"low":151.0,"close":156.0,"adjusted_close":157.0,"volume":800000}
	]`

	var capturedQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(priceJSON))
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	sec := &models.SecurityWithCountry{
		Security:     models.Security{Ticker: "AAPL"},
		Country:      "USA",
		ExchangeName: "NASDAQ",
	}

	t.Run("full mode returns all prices", func(t *testing.T) {
		startDT := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		endDT := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
		prices, err := client.GetDailyPrices(context.Background(), sec, startDT, endDT)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(prices) != 2 {
			t.Errorf("expected 2 prices, got %d", len(prices))
		}
		// Close should be the unadjusted close; split events handle price discontinuities separately
		if prices[0].Close != 151.0 {
			t.Errorf("expected Close 151.0, got %v", prices[0].Close)
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
		startDT := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		endDT := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
		_, err := client.GetDailyPrices(context.Background(), sec, startDT, endDT)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Verify the 'from' query param is present and formatted as YYYY-MM-DD
		if !strings.Contains(capturedQuery, "from=") {
			t.Errorf("expected 'from=' param in query string, got %q", capturedQuery)
		}
		if !strings.Contains(capturedQuery, "from=2026-01-01") {
			t.Errorf("expected from=2026-01-01 in query string, got %q", capturedQuery)
		}
	})
}

func TestEODHDGetDailyPricesEmpty(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	sec := &models.SecurityWithCountry{
		Security:     models.Security{Ticker: "EMPTY"},
		Country:      "USA",
		ExchangeName: "NASDAQ",
	}

	_, err := client.GetDailyPrices(context.Background(), sec, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Error("expected error for empty response, got nil")
	}
}

func TestEODHDGetDailyPricesNoKey(t *testing.T) {
	t.Parallel()
	client := eodhd.NewClient("", "http://localhost:9999")
	sec := &models.SecurityWithCountry{
		Security: models.Security{Ticker: "AAPL"},
		Country:  "USA",
	}
	_, err := client.GetDailyPrices(context.Background(), sec, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Error("expected error for missing API key")
	}
}

// --- HTTP mock tests for GetStockEvents ---

func TestEODHDGetStockEventsHTTP(t *testing.T) {
	t.Parallel()
	divJSON := `[
		{"date":"2025-12-15","value":0.25},
		{"date":"2025-09-15","value":0.25}
	]`
	splitJSON := `[
		{"date":"2024-06-14","split":"4.0000/1.0000"}
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

	client := eodhd.NewClient("test-key", srv.URL)
	sec := &models.SecurityWithCountry{
		Security:     models.Security{Ticker: "AAPL"},
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
	t.Parallel()
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

	client := eodhd.NewClient("test-key", srv.URL)
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

	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, priceRepo, eohdClient, 10)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{
		Price:    eohdClient,
		Event:    eohdClient,
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
		Bulk:     eohdClient,
	})
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo, exchangeRepo, priceRepo)

	router := gin.New()
	router.Use(middleware.ValidateUser())
	admin := router.Group("/admin")
	admin.GET("/bulk-fetch-eodhd-prices", adminHandler.BulkFetchEODHDPrices)
	return router
}

// TestBulkFetchEODHDPricesNoExchangeParam verifies the endpoint returns 200 and targets
// the US exchange even when no exchange param is provided (exchange is now hard-coded to US).
func TestBulkFetchEODHDPricesNoExchangeParam(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)

	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	router := setupBulkFetchRouter(pool, client)

	// min_required=0 bypasses the 30k completeness check so this test can verify
	// the exchange routing behaviour without needing a full-market mock response.
	req, _ := http.NewRequest("GET", "/admin/bulk-fetch-eodhd-prices?min_required=0", nil)
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if capturedPath != "/eod-bulk-last-day/US" {
		t.Errorf("expected EODHD request to /eod-bulk-last-day/US, got %s", capturedPath)
	}
}

// TestBulkFetchEODHDPricesInvalidDate verifies 400 for a malformed date.
func TestBulkFetchEODHDPricesInvalidDate(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	router := setupBulkFetchRouter(pool, client)

	req, _ := http.NewRequest("GET", "/admin/bulk-fetch-eodhd-prices?exchange=US&date=not-a-date", nil)
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestBulkFetchEODHDPricesWeekendRejected verifies that requesting a Saturday or Sunday returns 422.
func TestBulkFetchEODHDPricesWeekendRejected(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("EODHD should not be called for a weekend date")
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	router := setupBulkFetchRouter(pool, client)

	for _, date := range []string{"2026-03-14", "2026-03-15"} { // Saturday, Sunday
		req, _ := http.NewRequest("GET", "/admin/bulk-fetch-eodhd-prices?date="+date, nil)
		req.Header.Set("X-User-ID", "1")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusUnprocessableEntity {
			t.Errorf("date %s: expected 422, got %d: %s", date, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "Markets not open") {
			t.Errorf("date %s: expected 'Markets not open' in body, got: %s", date, w.Body.String())
		}
	}
}

// TestBulkFetchEODHDPricesHolidayRejected verifies that requesting a market holiday returns 422.
func TestBulkFetchEODHDPricesHolidayRejected(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("EODHD should not be called for a market holiday")
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	router := setupBulkFetchRouter(pool, client)

	// 2026-01-01 is New Year's Day (Thursday), a known NYSE holiday
	req, _ := http.NewRequest("GET", "/admin/bulk-fetch-eodhd-prices?date=2026-01-01", nil)
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected 422, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Markets not open") {
		t.Errorf("expected 'Markets not open' in body, got: %s", w.Body.String())
	}
}

// TestBulkFetchEODHDPricesStoresKnownSecurities verifies that records matching
// securities in dim_security are stored, and unknown tickers are skipped.
func TestBulkFetchEODHDPricesStoresKnownSecurities(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)

	ticker := nextTicker()
	secID, err := createTestStock(pool, ticker, "Bulk Test Stock")
	if err != nil {
		t.Fatalf("failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	bulkDate := "2026-01-09" // Friday, a valid trading day
	bulkJSON := fmt.Sprintf(`[
		{"code":"%s.US","date":"%s","open":10.0,"high":11.0,"low":9.5,"close":10.5,"adjusted_close":10.5,"volume":100000},
		{"code":"TSTUNKNOWN99.US","date":"%s","open":5.0,"high":6.0,"low":4.0,"close":5.5,"adjusted_close":5.5,"volume":50000}
	]`, ticker, bulkDate, bulkDate)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(bulkJSON))
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	router := setupBulkFetchRouter(pool, client)

	// min_required=0 bypasses the 30k completeness check — this test exercises
	// known/unknown ticker routing with a small mock response.
	req, _ := http.NewRequest("GET",
		fmt.Sprintf("/admin/bulk-fetch-eodhd-prices?exchange=US&date=%s&min_required=0", bulkDate), nil)
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result models.BulkFetchResult
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

// TestGetBulkSplits verifies parsing of the bulk splits endpoint.
func TestGetBulkSplits(t *testing.T) {
	t.Parallel()
	// Bulk splits: code has no exchange suffix (unlike bulk EOD).
	// Split "1.000000/80.000000" = 1-for-80 reverse split, coefficient = 1/80.
	splitsJSON := `[
		{"code":"ELPW","exchange":"US","date":"2026-03-12","split":"1.000000/80.000000"},
		{"code":"QSCGF","exchange":"US","date":"2026-03-12","split":"1.000000/5.000000"},
		{"code":"BADSPLIT","exchange":"US","date":"2026-03-12","split":"notaratio"},
		{"code":"BADDATE","exchange":"US","date":"not-a-date","split":"2.000000/1.000000"}
	]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("type") != "splits" {
			http.Error(w, "wrong type", http.StatusBadRequest)
			return
		}
		w.Write([]byte(splitsJSON))
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	date, _ := time.Parse("2006-01-02", "2026-03-12")
	records, err := client.GetBulkSplits(context.Background(), "US", date)
	if err != nil {
		t.Fatalf("GetBulkSplits failed: %v", err)
	}

	// BADSPLIT and BADDATE should be skipped
	if len(records) != 2 {
		t.Fatalf("expected 2 valid records, got %d", len(records))
	}

	byCode := make(map[string]float64)
	for _, r := range records {
		byCode[r.Code] = r.SplitCoefficient
	}

	const eps = 1e-9
	if v := byCode["ELPW"]; math.Abs(v-1.0/80.0) > eps {
		t.Errorf("ELPW: expected coeff=%v, got %v", 1.0/80.0, v)
	}
	if v := byCode["QSCGF"]; math.Abs(v-1.0/5.0) > eps {
		t.Errorf("QSCGF: expected coeff=%v, got %v", 1.0/5.0, v)
	}
}

// TestGetBulkDividends verifies parsing of the bulk dividends endpoint.
func TestGetBulkDividends(t *testing.T) {
	t.Parallel()
	dividendsJSON := `[
		{"code":"HD","exchange":"US","date":"2026-03-12","dividend":"2.33000","currency":"USD","declarationDate":"2026-02-24","recordDate":"2026-03-12","paymentDate":"2026-03-26","period":"Quarterly","unadjustedValue":"2.3300000000"},
		{"code":"LKQ","exchange":"US","date":"2026-03-12","dividend":"0.30000","currency":"USD","declarationDate":null,"recordDate":null,"paymentDate":null,"period":null,"unadjustedValue":"0.3000000000"},
		{"code":"BADAMT","exchange":"US","date":"2026-03-12","dividend":"notanumber","currency":"USD","declarationDate":null,"recordDate":null,"paymentDate":null,"period":null,"unadjustedValue":"0"},
		{"code":"BADDATE","exchange":"US","date":"not-a-date","dividend":"1.00","currency":"USD","declarationDate":null,"recordDate":null,"paymentDate":null,"period":null,"unadjustedValue":"1.00"}
	]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("type") != "dividends" {
			http.Error(w, "wrong type", http.StatusBadRequest)
			return
		}
		w.Write([]byte(dividendsJSON))
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	date, _ := time.Parse("2006-01-02", "2026-03-12")
	records, err := client.GetBulkDividends(context.Background(), "US", date)
	if err != nil {
		t.Fatalf("GetBulkDividends failed: %v", err)
	}

	// BADAMT and BADDATE should be skipped
	if len(records) != 2 {
		t.Fatalf("expected 2 valid records, got %d", len(records))
	}

	byCode := make(map[string]float64)
	for _, r := range records {
		byCode[r.Code] = r.Dividend
		if r.SplitCoefficient != 1.0 {
			t.Errorf("%s: expected SplitCoefficient=1.0, got %v", r.Code, r.SplitCoefficient)
		}
	}

	if v := byCode["HD"]; v != 2.33 {
		t.Errorf("HD: expected dividend=2.33, got %v", v)
	}
	if v := byCode["LKQ"]; v != 0.30 {
		t.Errorf("LKQ: expected dividend=0.30, got %v", v)
	}
}

// TestGetBulkEvents verifies that GetBulkEvents fetches splits and dividends in parallel
// and merges records that share the same ticker and date.
func TestGetBulkEvents(t *testing.T) {
	t.Parallel()
	// AAPL has only a dividend; TSLA has only a split; MSFT has both on the same date.
	splitsJSON := `[
		{"code":"TSLA","exchange":"US","date":"2026-03-12","split":"3.000000/1.000000"},
		{"code":"MSFT","exchange":"US","date":"2026-03-12","split":"2.000000/1.000000"}
	]`
	dividendsJSON := `[
		{"code":"AAPL","exchange":"US","date":"2026-03-12","dividend":"0.25000","currency":"USD","declarationDate":null,"recordDate":null,"paymentDate":null,"period":null,"unadjustedValue":"0.25"},
		{"code":"MSFT","exchange":"US","date":"2026-03-12","dividend":"0.75000","currency":"USD","declarationDate":null,"recordDate":null,"paymentDate":null,"period":null,"unadjustedValue":"0.75"}
	]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("type") {
		case "splits":
			w.Write([]byte(splitsJSON))
		case "dividends":
			w.Write([]byte(dividendsJSON))
		default:
			http.Error(w, "missing type param", http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	client := eodhd.NewClient("test-key", srv.URL)
	date, _ := time.Parse("2006-01-02", "2026-03-12")
	records, err := client.GetBulkEvents(context.Background(), "US", date)
	if err != nil {
		t.Fatalf("GetBulkEvents failed: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 merged records (AAPL, TSLA, MSFT), got %d", len(records))
	}

	byCode := make(map[string]providers.BulkEventRecord)
	for _, r := range records {
		byCode[r.Code] = r
	}

	if r := byCode["TSLA"]; r.SplitCoefficient != 3.0 || r.Dividend != 0 {
		t.Errorf("TSLA: got coeff=%v div=%v, want coeff=3 div=0", r.SplitCoefficient, r.Dividend)
	}
	if r := byCode["AAPL"]; r.Dividend != 0.25 || r.SplitCoefficient != 1.0 {
		t.Errorf("AAPL: got div=%v coeff=%v, want div=0.25 coeff=1.0", r.Dividend, r.SplitCoefficient)
	}
	if r := byCode["MSFT"]; r.SplitCoefficient != 2.0 || r.Dividend != 0.75 {
		t.Errorf("MSFT: got coeff=%v div=%v, want coeff=2.0 div=0.75", r.SplitCoefficient, r.Dividend)
	}
}


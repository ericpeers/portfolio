package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/providers/financialdata"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupPricingTestRouter creates a router with admin endpoints for pricing tests.
// priceClient is used for stock price fetching; eventClient is used for event fetching;
// avClient is used for treasury rates.
func setupPricingTestRouter(pool *pgxpool.Pool, priceClient providers.StockPriceFetcher, eventClient providers.StockEventFetcher, avClient providers.TreasuryRateFetcher) *gin.Engine {
	gin.SetMode(gin.TestMode)

	securityRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)

	avListingClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, priceRepo, avListingClient)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, priceClient, eventClient, avClient, nil)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avListingClient)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo, exchangeRepo, priceRepo)

	router := gin.New()
	admin := router.Group("/admin")
	{
		admin.POST("/sync-securities", adminHandler.SyncSecuritiesFromAV)
		admin.GET("/get_daily_prices", adminHandler.GetDailyPrices)
	}

	return router
}

// setupPricingTestSecurity creates a test security for pricing tests (delegates to createTestSecurity)
func setupPricingTestSecurity(pool *pgxpool.Pool, ticker, name string, inception *time.Time) (int64, error) {
	return createTestSecurity(pool, ticker, name, models.SecurityTypeStock, inception)
}

// TestFetchPricingNoCachedData tests fetching prices for a security with no cached data
func TestFetchPricingNoCachedData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test security with inception in 2020
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	securityID, err := setupPricingTestSecurity(pool, "TSTNOCACHE", "Test No Cache Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTNOCACHE")

	// Generate mock price data
	startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)
	prices := generateFDPriceData(startDate, endDate)

	// Track FD calls
	var callCount int32
	mockServer := createMockFDPriceServer(prices, &callCount)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, fdClient, fdClient, avClient)

	// Call the endpoint
	url := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=2025-01-01&end_date=2025-01-31", securityID)
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.GetDailyPricesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify prices were returned
	if response.DataPoints == 0 {
		t.Error("Expected price data to be returned, got 0 data points")
	}

	// Verify FD (FinancialData) was called
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected FD (FinancialData) to be called for uncached data")
	}

	// Verify data was cached in fact_price
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_price WHERE security_id = $1`, securityID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("Failed to query fact_price: %v", err)
	}
	if priceCount == 0 {
		t.Error("Expected prices to be cached in fact_price")
	}

	// Verify fact_price_range was created
	var rangeCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_price_range WHERE security_id = $1`, securityID).Scan(&rangeCount)
	if err != nil {
		t.Fatalf("Failed to query fact_price_range: %v", err)
	}
	if rangeCount != 1 {
		t.Errorf("Expected 1 fact_price_range record, got %d", rangeCount)
	}
}

// TestFetchPricingPartialFillIn tests filling in newer data when older data is cached
func TestFetchPricingPartialFillIn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test security
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	securityID, err := setupPricingTestSecurity(pool, "TSTPARTIAL", "Test Partial Fill Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTPARTIAL")

	// Pre-populate cache with older data (Jan 1-15, 2025)
	oldStartDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	oldEndDate := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	for d := oldStartDate; !d.After(oldEndDate); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO fact_price (security_id, date, open, high, low, close, volume)
			VALUES ($1, $2, 100, 105, 99, 102, 1000000)
		`, securityID, d)
		if err != nil {
			t.Fatalf("Failed to insert old price data: %v", err)
		}
	}

	// Set the cached range to Jan 1-15 with a past next_update so DetermineFetch checks range coverage
	pastNextUpdate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = pool.Exec(ctx, `
		INSERT INTO fact_price_range (security_id, start_date, end_date, next_update)
		VALUES ($1, $2, $3, $4)
	`, securityID, oldStartDate, oldEndDate, pastNextUpdate)
	if err != nil {
		t.Fatalf("Failed to insert price range: %v", err)
	}

	// Generate mock price data for extended range
	newEndDate := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)
	prices := generateFDPriceData(oldStartDate, newEndDate)

	var callCount int32
	mockServer := createMockFDPriceServer(prices, &callCount)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, fdClient, fdClient, avClient)

	// Request data that extends beyond the cached range (Jan 1 - Jan 31)
	url := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=2025-01-01&end_date=2025-01-31", securityID)
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify FD (FinancialData) was called to fill in the gap
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected FD (FinancialData) to be called to extend cached range")
	}

	// Verify the range was extended
	var endDate time.Time
	err = pool.QueryRow(ctx, `SELECT end_date FROM fact_price_range WHERE security_id = $1`, securityID).Scan(&endDate)
	if err != nil {
		t.Fatalf("Failed to query fact_price_range: %v", err)
	}

	if endDate.Before(newEndDate) {
		t.Errorf("Expected end_date to be extended to at least %s, got %s", newEndDate.Format("2006-01-02"), endDate.Format("2006-01-02"))
	}
}

// TestFetchPricingFromCache tests that data is returned from cache without calling AV
func TestFetchPricingFromCache(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test security
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	securityID, err := setupPricingTestSecurity(pool, "TESTCACHED", "Test Cached Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TESTCACHED")

	// Pre-populate cache with full date range
	startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)

	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO fact_price (security_id, date, open, high, low, close, volume)
			VALUES ($1, $2, 100, 105, 99, 102, 1000000)
		`, securityID, d)
		if err != nil {
			t.Fatalf("Failed to insert price data: %v", err)
		}
	}

	// Set the cached range with a far-future next_update so data is considered fresh
	futureNextUpdate := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = pool.Exec(ctx, `
		INSERT INTO fact_price_range (security_id, start_date, end_date, next_update)
		VALUES ($1, $2, $3, $4)
	`, securityID, startDate, endDate, futureNextUpdate)
	if err != nil {
		t.Fatalf("Failed to insert price range: %v", err)
	}

	// Track AV calls
	var callCount int32
	mockServer := createMockFDPriceServer(nil, &callCount)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, fdClient, fdClient, avClient)

	// Request data within cached range
	url := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=2025-01-01&end_date=2025-01-31", securityID)
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.GetDailyPricesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify prices were returned
	if response.DataPoints == 0 {
		t.Error("Expected cached price data to be returned")
	}

	// Verify AV was NOT called
	if atomic.LoadInt32(&callCount) > 0 {
		t.Error("Expected AlphaVantage NOT to be called for fully cached data")
	}
}

// TestFetchPricingHistoricalNoData tests requesting data from before security inception
func TestFetchPricingHistoricalNoData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create test security with inception in 2020
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	securityID, err := setupPricingTestSecurity(pool, "TSTHIST", "Test Historical Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTHIST")

	// Generate mock price data starting from inception
	prices := generateFDPriceData(inception, time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockFDPriceServer(prices, &callCount)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, fdClient, fdClient, avClient)

	// Request data from 1995 (way before inception)
	url := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=1995-01-01&end_date=1995-12-31", securityID)
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.GetDailyPricesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Should return empty data since the entire requested range is before inception.
	// The effective start is adjusted to inception (2020), but end is 1995-12-31,
	// so the clamped range is empty and no prices exist.
	if response.DataPoints != 0 {
		t.Errorf("Expected 0 data points for dates entirely before inception, got %d", response.DataPoints)
	}
}

// TestFetchPricingBeforeIPO tests requesting data before IPO for VHCP-like scenario
func TestFetchPricingBeforeIPO(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test security with IPO on Dec 18, 2025
	inception := time.Date(2025, 12, 18, 0, 0, 0, 0, time.UTC)
	securityID, err := setupPricingTestSecurity(pool, "TESTVHCP", "Test VHCP-like Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TESTVHCP")

	// Generate mock price data starting from IPO
	prices := generateFDPriceData(inception, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockFDPriceServer(prices, &callCount)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, fdClient, fdClient, avClient)

	// Request data from Jan 1 - Mar 31, 2025 (entirely before IPO)
	url := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=2025-01-01&end_date=2025-03-31", securityID)
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.GetDailyPricesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Should return 0 data points since entire request is before IPO
	if response.DataPoints != 0 {
		t.Errorf("Expected 0 data points for request entirely before IPO, got %d", response.DataPoints)
	}

	// Verify FD (FinancialData) was called and data was cached from inception
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected FD (FinancialData) to be called to fetch available data")
	}

	// Verify fact_price_range starts at inception, not at requested start
	var rangeStart time.Time
	err = pool.QueryRow(ctx, `SELECT start_date FROM fact_price_range WHERE security_id = $1`, securityID).Scan(&rangeStart)
	if err != nil {
		t.Fatalf("Failed to query fact_price_range: %v", err)
	}

	// The range start should be at or after inception
	if rangeStart.Before(inception) {
		t.Errorf("Expected fact_price_range start_date to be at or after inception %s, got %s",
			inception.Format("2006-01-02"), rangeStart.Format("2006-01-02"))
	}
}

// TestFetchPricingBeforeIPONoRefetch tests that subsequent pre-IPO requests don't trigger FD calls.
// Uses two separate PricingService instances to eliminate memory-cache interference so we
// can be certain the second call is served entirely from the DB cache.
func TestFetchPricingBeforeIPONoRefetch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test security with IPO on Dec 18, 2025
	inception := time.Date(2025, 12, 18, 0, 0, 0, 0, time.UTC)
	securityID, err := setupPricingTestSecurity(pool, "TESTVHCP2", "Test VHCP-like Security 2", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TESTVHCP2")

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")

	// Generate mock price data starting from IPO
	prices := generateFDPriceData(inception, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC))

	// --- First service instance: fetch and cache data spanning the IPO ---
	var callCount1 int32
	mockServer1 := createMockFDPriceServer(prices, &callCount1)
	defer mockServer1.Close()

	svc1 := services.NewPricingService(priceRepo, secRepo,
		financialdata.NewClient("test-key", mockServer1.URL),
		financialdata.NewClient("test-key", mockServer1.URL),
		avClient, nil)

	// Dec 1, 2025 → Jan 15, 2026 spans the IPO; should fetch from FD and cache from inception
	firstStart := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	firstEnd := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	_, _, err = svc1.GetDailyPrices(ctx, securityID, firstStart, firstEnd)
	if err != nil {
		t.Fatalf("First GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Error("First request: Expected FD (FinancialData) to be called")
	}

	// Verify prices were cached
	var priceCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_price WHERE security_id = $1`, securityID).Scan(&priceCount); err != nil {
		t.Fatalf("Failed to query fact_price: %v", err)
	}
	if priceCount == 0 {
		t.Error("Expected prices to be cached after first request")
	}

	// --- Second service instance: fresh in-memory state, same DB ---
	// This ensures the second call cannot use a memory-cached result from svc1.
	var callCount2 int32
	mockServer2 := createMockFDPriceServer(prices, &callCount2)
	defer mockServer2.Close()

	svc2 := services.NewPricingService(priceRepo, secRepo,
		financialdata.NewClient("test-key", mockServer2.URL),
		financialdata.NewClient("test-key", mockServer2.URL),
		avClient, nil)

	// Apr 1–30, 2025: entirely before IPO; the DB cache already covers inception onward,
	// so DetermineFetch must recognise the request is within the "before cached start"
	// region and return no data without hitting FD.
	secondStart := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	secondEnd := time.Date(2025, 4, 30, 0, 0, 0, 0, time.UTC)
	prices2, _, err := svc2.GetDailyPrices(ctx, securityID, secondStart, secondEnd)
	if err != nil {
		t.Fatalf("Second GetDailyPrices failed: %v", err)
	}

	// Should return 0 data points since the request is entirely before the IPO
	if len(prices2) != 0 {
		t.Errorf("Second request: Expected 0 data points for pre-IPO request, got %d", len(prices2))
	}

	// The key assertion: FD must NOT have been called for the second request.
	// Since it's a fresh service instance the only way to avoid an FD call is if
	// DetermineFetch correctly reads the DB cache and skips the fetch.
	if atomic.LoadInt32(&callCount2) > 0 {
		t.Errorf("Second request: Expected NO FD calls (DB cache sufficient), got %d", callCount2)
	}
}

// TestFetchPricingByTicker tests fetching prices by ticker instead of security_id
func TestFetchPricingByTicker(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create test security
	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := setupPricingTestSecurity(pool, "TESTTICKER", "Test Ticker Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TESTTICKER")

	// Generate mock price data
	prices := generateFDPriceData(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockFDPriceServer(prices, &callCount)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, fdClient, fdClient, avClient)

	// Request by ticker
	url := "/admin/get_daily_prices?ticker=TESTTICKER&start_date=2025-01-01&end_date=2025-01-31"
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.GetDailyPricesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify the symbol is returned correctly
	if response.Ticker != "TESTTICKER" {
		t.Errorf("Expected ticker 'TESTTICKER', got '%s'", response.Ticker)
	}

	if response.DataPoints == 0 {
		t.Error("Expected price data to be returned")
	}
}

// TestFetchPricingInvalidRequest tests validation of request parameters
func TestFetchPricingInvalidRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	mockServer := createMockFDPriceServer(nil, nil)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, fdClient, fdClient, avClient)

	tests := []struct {
		name           string
		url            string
		expectedStatus int
		expectedError  string
	}{
		{
			name:           "missing ticker and security_id",
			url:            "/admin/get_daily_prices?start_date=2025-01-01&end_date=2025-01-31",
			expectedStatus: http.StatusBadRequest,
			expectedError:  "must provide either ticker or security_id",
		},
		{
			name:           "missing start_date",
			url:            "/admin/get_daily_prices?ticker=AAPL&end_date=2025-01-31",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "missing end_date",
			url:            "/admin/get_daily_prices?ticker=AAPL&start_date=2025-01-01",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "invalid start_date format",
			url:            "/admin/get_daily_prices?ticker=AAPL&start_date=01-01-2025&end_date=2025-01-31",
			expectedStatus: http.StatusBadRequest,
			expectedError:  "YYYY-MM-DD",
		},
		{
			name:           "invalid end_date format",
			url:            "/admin/get_daily_prices?ticker=AAPL&start_date=2025-01-01&end_date=31-01-2025",
			expectedStatus: http.StatusBadRequest,
			expectedError:  "YYYY-MM-DD",
		},
		{
			name:           "non-existent ticker",
			url:            "/admin/get_daily_prices?ticker=NONEXISTENT999&start_date=2025-01-01&end_date=2025-01-31",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", tt.url, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d: %s", tt.expectedStatus, w.Code, w.Body.String())
			}

			if tt.expectedError != "" {
				var errResp models.ErrorResponse
				json.Unmarshal(w.Body.Bytes(), &errResp)
				if errResp.Message == "" || !contains(errResp.Message, tt.expectedError) {
					t.Errorf("Expected error message containing '%s', got '%s'", tt.expectedError, errResp.Message)
				}
			}
		})
	}
}

// TestMoneyMarketFundSyntheticPrices verifies that money market funds (FUND type with
// "money market" in the name) receive synthetic $1.00 prices without calling EODHD.
func TestMoneyMarketFundSyntheticPrices(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	securityID, err := createTestSecurity(pool, "TSTMMFUND", "TST Fidelity Government Money Market Fund", models.SecurityTypeFund, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTMMFUND")

	// Set up mock FD server with a call counter — it must NOT be called
	var callCount int32
	mockServer := createMockFDPriceServer(nil, &callCount)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, fdClient, fdClient, avClient)

	url := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=2025-01-01&end_date=2025-01-31", securityID)
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.GetDailyPricesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// All returned prices must be $1.00
	if response.DataPoints == 0 {
		t.Error("Expected synthetic price data to be returned, got 0 data points")
	}
	for _, p := range response.Prices {
		if p.Close != 1.0 {
			t.Errorf("Expected close = 1.0 for money market fund, got %f on %s", p.Close, p.Date.Format("2006-01-02"))
		}
	}

	// EODHD must never have been called
	if atomic.LoadInt32(&callCount) != 0 {
		t.Errorf("Expected EODHD NOT to be called for money market fund, got %d calls", callCount)
	}

	// Prices must be cached in fact_price with close = 1.0
	var minClose, maxClose float64
	err = pool.QueryRow(ctx,
		`SELECT MIN(close), MAX(close) FROM fact_price WHERE security_id = $1`,
		securityID,
	).Scan(&minClose, &maxClose)
	if err != nil {
		t.Fatalf("Failed to query fact_price: %v", err)
	}
	if minClose != 1.0 || maxClose != 1.0 {
		t.Errorf("Expected all cached close prices = 1.0, got min=%f max=%f", minClose, maxClose)
	}
}

// TestMoneyMarketFundNotMatchedForOtherFunds verifies that a FUND-type security whose
// name does NOT contain "money market" falls through to the normal EODHD path.
func TestMoneyMarketFundNotMatchedForOtherFunds(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := createTestSecurity(pool, "TSTBNDFUND", "TST Bond Fund", models.SecurityTypeFund, &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTBNDFUND")

	// Provide mock prices so the FD call succeeds
	prices := generateFDPriceData(inception, time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC))
	var callCount int32
	mockServer := createMockFDPriceServer(prices, &callCount)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, fdClient, fdClient, avClient)

	url := "/admin/get_daily_prices?ticker=TSTBNDFUND&start_date=2025-01-01&end_date=2025-01-31"
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// EODHD must have been called — non-money-market FUNDs go through normal path
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected EODHD to be called for non-money-market FUND type")
	}
}

// TestGetPriceAtDateWeekend verifies that requesting a price for a Sunday returns
// the prior Friday's cached price without triggering any provider call.
func TestGetPriceAtDateWeekend(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	securityID, err := createTestStock(pool, "TSTPADT", "Price At Date Test Stock")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTPADT")

	// Insert Mon-Fri prices for week of 2026-02-23 to 2026-02-27 (basePrice=100)
	// insertPriceData uses close = basePrice + 2 * dayOffset, so Friday close = 108.
	priceStart := time.Date(2026, 2, 23, 0, 0, 0, 0, time.UTC)
	priceEnd := time.Date(2026, 2, 27, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(pool, securityID, priceStart, priceEnd, 100.0); err != nil {
		t.Fatalf("Failed to insert price data: %v", err)
	}

	// Track provider calls — they must NOT fire for a cached weekend date.
	var callCount int32
	mockServer := createMockFDPriceServer(nil, &callCount)
	defer mockServer.Close()

	fdClient := financialdata.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, fdClient, fdClient, avClient, nil)

	// Request price for Sunday 2026-03-01 — falls back to Friday 2026-02-27.
	sunday := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	price, err := pricingSvc.GetPriceAtDate(ctx, securityID, sunday)
	if err != nil {
		t.Fatalf("GetPriceAtDate failed: %v", err)
	}

	// insertPriceData sets close = basePrice + 2 (constant), so Friday close = 102.
	expectedClose := 102.0
	if price != expectedClose {
		t.Errorf("Expected Friday close %.2f for Sunday request, got %.2f", expectedClose, price)
	}

	if atomic.LoadInt32(&callCount) > 0 {
		t.Errorf("Expected NO provider calls (data served from cache), got %d", callCount)
	}
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestPriceRangeNoGaps verifies that fact_price_range never claims to cover a date
// range that has gaps in fact_price.
//
// The fix lives in DetermineFetch: when a new fetch would leave a gap with the
// existing cached range, adjStartDT/adjEndDT are extended to close the gap before
// fetchAndStore is called. This test exercises that path through the full service
// layer — two non-contiguous GetDailyPrices calls should produce a contiguous cache.
func TestPriceRangeNoGaps(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	securityID, err := createTestSecurity(pool, "TSTGAP", "Gap Test Security", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTGAP")

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")

	rangeAStart := time.Date(2023, 1, 3, 0, 0, 0, 0, time.UTC)
	rangeAEnd := time.Date(2023, 6, 30, 0, 0, 0, 0, time.UTC)
	rangeBStart := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	rangeBEnd := time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC)

	// First call: fetch and cache range A (2023-01-03 to 2023-06-30).
	pricesA := generateFDPriceData(rangeAStart, rangeAEnd)
	var callCount1 int32
	mockServerA := createMockFDPriceServer(pricesA, &callCount1)
	defer mockServerA.Close()
	svc1 := services.NewPricingService(priceRepo, secRepo,
		financialdata.NewClient("test-key", mockServerA.URL),
		financialdata.NewClient("test-key", mockServerA.URL),
		avClient, nil)

	if _, _, err := svc1.GetDailyPrices(ctx, securityID, rangeAStart, rangeAEnd); err != nil {
		t.Fatalf("GetDailyPrices A: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Fatal("Expected provider to be called for range A")
	}

	// Second call: request range B (2025-01-02 to 2025-06-30).
	// There is an 18-month gap between rangeAEnd and rangeBStart.
	// DetermineFetch extends adjStartDT to rangeAEnd+1 (2023-07-01) to close the gap,
	// so the provider is called with 2023-07-01 → 2025-06-30.
	// The mock server must cover the full extended range.
	pricesGapAndB := generateFDPriceData(rangeAEnd.AddDate(0, 0, 1), rangeBEnd)
	var callCount2 int32
	mockServerB := createMockFDPriceServer(pricesGapAndB, &callCount2)
	defer mockServerB.Close()
	svc2 := services.NewPricingService(priceRepo, secRepo,
		financialdata.NewClient("test-key", mockServerB.URL),
		financialdata.NewClient("test-key", mockServerB.URL),
		avClient, nil)

	if _, _, err := svc2.GetDailyPrices(ctx, securityID, rangeBStart, rangeBEnd); err != nil {
		t.Fatalf("GetDailyPrices B: %v", err)
	}
	if atomic.LoadInt32(&callCount2) == 0 {
		t.Fatal("Expected provider to be called for range B (gap fill)")
	}

	// Read what fact_price_range now claims.
	var claimedStart, claimedEnd time.Time
	if err := pool.QueryRow(ctx,
		`SELECT start_date, end_date FROM fact_price_range WHERE security_id = $1`,
		securityID).Scan(&claimedStart, &claimedEnd); err != nil {
		t.Fatalf("Read fact_price_range: %v", err)
	}
	t.Logf("fact_price_range: %s → %s", claimedStart.Format("2006-01-02"), claimedEnd.Format("2006-01-02"))

	// Invariant 1: claimed start/end must match the actual extremes in fact_price.
	var minDate, maxDate time.Time
	if err := pool.QueryRow(ctx,
		`SELECT MIN(date), MAX(date) FROM fact_price WHERE security_id = $1`,
		securityID).Scan(&minDate, &maxDate); err != nil {
		t.Fatalf("Query MIN/MAX fact_price: %v", err)
	}
	if !claimedStart.Equal(minDate) {
		t.Errorf("start_date %s ≠ MIN(fact_price.date) %s",
			claimedStart.Format("2006-01-02"), minDate.Format("2006-01-02"))
	}
	if !claimedEnd.Equal(maxDate) {
		t.Errorf("end_date %s ≠ MAX(fact_price.date) %s",
			claimedEnd.Format("2006-01-02"), maxDate.Format("2006-01-02"))
	}

	// Invariant 2: no weekday gaps inside the claimed range.
	var gapDays int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM generate_series($1::date, $2::date, '1 day'::interval) AS d(date)
		WHERE EXTRACT(DOW FROM d) NOT IN (0, 6)
		  AND NOT EXISTS (
		        SELECT 1 FROM fact_price WHERE security_id = $3 AND date = d::date
		  )
	`, claimedStart, claimedEnd, securityID).Scan(&gapDays); err != nil {
		t.Fatalf("Gap check query: %v", err)
	}
	if gapDays > 0 {
		t.Errorf("fact_price_range claims continuous data %s → %s, "+
			"but %d weekdays in that range have no row in fact_price",
			claimedStart.Format("2006-01-02"), claimedEnd.Format("2006-01-02"), gapDays)
	}
}

// TestDetermineFetch is a pure unit test for the DetermineFetch function.
// It does not require a database connection.
func TestDetermineFetch(t *testing.T) {
	now := time.Date(2026, 2, 12, 14, 0, 0, 0, time.UTC)               // "now" = Feb 12 2026 2pm
	futureNextUpdate := time.Date(2026, 2, 12, 21, 30, 0, 0, time.UTC) // 4:30pm ET = 9:30pm UTC
	pastNextUpdate := time.Date(2026, 2, 11, 21, 30, 0, 0, time.UTC)   // yesterday

	cacheStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cacheEnd := time.Date(2026, 2, 11, 0, 0, 0, 0, time.UTC) // yesterday at midnight

	tests := []struct {
		name           string
		priceRange     *repository.PriceRange
		currentDT      time.Time
		effectiveStart time.Time
		endDate        time.Time
		wantFetch      bool
		// wantAdjStart/wantAdjEnd: zero value means "same as effectiveStart/endDate" (no adjustment).
		wantAdjStart time.Time
		wantAdjEnd   time.Time
	}{
		{
			name:           "no cache - needs full fetch",
			priceRange:     nil,
			currentDT:      now,
			effectiveStart: cacheStart,
			endDate:        cacheEnd,
			wantFetch:      true,
		},
		{
			name: "cache covers range, NextUpdate in future - no fetch",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart,
				EndDate:    cacheEnd,
				NextUpdate: futureNextUpdate,
			},
			currentDT:      now,
			effectiveStart: cacheStart,
			endDate:        cacheEnd,
			wantFetch:      false,
		},
		{
			// Cache already covers the requested range — serve from cache even if NextUpdate
			// has elapsed. Proactive refresh happens only when endDate exceeds cache end.
			name: "cache covers range, NextUpdate in past - no fetch (cache sufficient)",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart,
				EndDate:    cacheEnd,
				NextUpdate: pastNextUpdate,
			},
			currentDT:      now,
			effectiveStart: cacheStart,
			endDate:        cacheEnd,
			wantFetch:      false,
		},
		{
			name: "end not covered, NextUpdate in future - no fetch (data not yet available)",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart,
				EndDate:    cacheEnd, // cache ends yesterday
				NextUpdate: futureNextUpdate,
			},
			currentDT:      now,
			effectiveStart: cacheStart,
			endDate:        time.Date(2026, 2, 12, 0, 0, 0, 0, time.UTC), // requesting today
			wantFetch:      false,
		},
		{
			name: "cache does NOT cover range, NextUpdate in past - needs fetch",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart,
				EndDate:    cacheEnd,
				NextUpdate: pastNextUpdate,
			},
			currentDT:      now,
			effectiveStart: cacheStart,
			endDate:        time.Date(2026, 2, 12, 0, 0, 0, 0, time.UTC),
			wantFetch:      true,
		},
		{
			name: "start NOT covered, gap > 140 days - must fetch historical data",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart, // 2025-01-01
				EndDate:    cacheEnd,   // 2026-02-11
				NextUpdate: futureNextUpdate,
			},
			currentDT:      now,
			effectiveStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), // 365 days before cache start
			endDate:        cacheEnd,
			wantFetch:      true,
		},
		{
			name: "start NOT covered, gap <= 140 days - fetch needed",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart, // 2025-01-01
				EndDate:    cacheEnd,   // 2026-02-11
				NextUpdate: pastNextUpdate,
			},
			currentDT:      now,
			effectiveStart: time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC), // 122 days before cache start
			endDate:        cacheEnd,
			wantFetch:      true,
		},
		{
			name: "start covered, NextUpdate past, cache very stale - needs fetch",
			priceRange: &repository.PriceRange{
				StartDate:  time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				EndDate:    time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC), // >100 days ago
				NextUpdate: pastNextUpdate,
			},
			currentDT:      now,
			effectiveStart: time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC),
			endDate:        time.Date(2026, 2, 12, 0, 0, 0, 0, time.UTC),
			wantFetch:      true,
		},
		{
			name: "endDate with 23:59:59 matches cache endDate at midnight - no fetch",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart,
				EndDate:    cacheEnd, // Feb 11 midnight
				NextUpdate: futureNextUpdate,
			},
			currentDT:      now,
			effectiveStart: cacheStart,
			// Feb 11 with 23:59:59 - same calendar day as cache end
			endDate:   time.Date(2026, 2, 11, 23, 59, 59, 0, time.UTC),
			wantFetch: false,
		},
		{
			// startDT is after priceRange.EndDate — a forward gap exists.
			// adjStartDT must be pulled back to cacheEnd+1 to close it.
			name: "forward gap: startDT after cache end — adjStartDT extended to fill gap",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart,    // 2025-01-01
				EndDate:    cacheEnd,      // 2026-02-11
				NextUpdate: pastNextUpdate,
			},
			currentDT:      now,
			effectiveStart: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
			endDate:        time.Date(2027, 6, 30, 0, 0, 0, 0, time.UTC),
			wantFetch:      true,
			wantAdjStart:   cacheEnd.AddDate(0, 0, 1), // 2026-02-12
			// wantAdjEnd zero → same as endDate (2027-06-30), no adjustment
		},
		{
			// endDT is before priceRange.StartDate — a backward gap exists.
			// adjEndDT must be pushed forward to cacheStart-1 to close it.
			name: "backward gap: endDT before cache start — adjEndDT extended to fill gap",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart,    // 2025-01-01
				EndDate:    cacheEnd,      // 2026-02-11
				NextUpdate: pastNextUpdate,
			},
			currentDT:      now,
			effectiveStart: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			endDate:        time.Date(2023, 6, 30, 0, 0, 0, 0, time.UTC),
			wantFetch:      true,
			// wantAdjStart zero → same as effectiveStart (2023-01-01), no adjustment
			wantAdjEnd: cacheStart.AddDate(0, 0, -1), // 2024-12-31
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFetch, gotAdjStart, gotAdjEnd := services.DetermineFetch(
				tt.priceRange, tt.currentDT, tt.effectiveStart, tt.endDate)

			if gotFetch != tt.wantFetch {
				t.Errorf("needsFetch = %v, want %v", gotFetch, tt.wantFetch)
			}

			expectedAdjStart := tt.effectiveStart
			if !tt.wantAdjStart.IsZero() {
				expectedAdjStart = tt.wantAdjStart
			}
			expectedAdjEnd := tt.endDate
			if !tt.wantAdjEnd.IsZero() {
				expectedAdjEnd = tt.wantAdjEnd
			}
			if !gotAdjStart.Equal(expectedAdjStart) {
				t.Errorf("adjStartDT = %s, want %s",
					gotAdjStart.Format("2006-01-02"), expectedAdjStart.Format("2006-01-02"))
			}
			if !gotAdjEnd.Equal(expectedAdjEnd) {
				t.Errorf("adjEndDT = %s, want %s",
					gotAdjEnd.Format("2006-01-02"), expectedAdjEnd.Format("2006-01-02"))
			}
		})
	}
}

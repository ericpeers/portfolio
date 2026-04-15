package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/providers/eodhd"
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
	eodhdAdminClient := eodhd.NewClient("test-key", "http://localhost:9999")
	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, priceRepo, eodhdAdminClient, 10)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{Price: priceClient, Event: eventClient, Treasury: avClient})
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avListingClient)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo, exchangeRepo, priceRepo)

	router := gin.New()
	admin := router.Group("/admin")
	{
		admin.POST("/sync-securities", adminHandler.SyncSecuritiesFromProvider)
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
	t.Parallel()
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
	prices := generatePriceData(startDate, endDate)

	// Track provider calls
	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, priceClient, nil, avClient)

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

	// Verify provider was called
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected provider to be called for uncached data")
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
	t.Parallel()
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
	prices := generatePriceData(oldStartDate, newEndDate)

	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, priceClient, nil, avClient)

	// Request data that extends beyond the cached range (Jan 1 - Jan 31)
	url := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=2025-01-01&end_date=2025-01-31", securityID)
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify provider was called to fill in the gap
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected provider to be called to extend cached range")
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
	t.Parallel()
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
	mockServer := createMockPriceServer(nil, &callCount)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, priceClient, nil, avClient)

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
	t.Parallel()
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
	prices := generatePriceData(inception, time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, priceClient, nil, avClient)

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
	t.Parallel()
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
	prices := generatePriceData(inception, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, priceClient, nil, avClient)

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

	// Verify provider was called and data was cached from inception
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected provider to be called to fetch available data")
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

// TestFetchPricingBeforeIPONoRefetch tests that subsequent pre-IPO requests don't trigger provider calls.
// Uses two separate PricingService instances to eliminate memory-cache interference so we
// can be certain the second call is served entirely from the DB cache.
func TestFetchPricingBeforeIPONoRefetch(t *testing.T) {
	t.Parallel()
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
	prices := generatePriceData(inception, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC))

	// --- First service instance: fetch and cache data spanning the IPO ---
	var callCount1 int32
	mockServer1 := createMockPriceServer(prices, &callCount1)
	defer mockServer1.Close()

	svc1 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mockServer1.URL),
		Treasury: avClient,
	})

	// Dec 1, 2025 → Jan 15, 2026 spans the IPO; should fetch from the provider and cache from inception
	firstStart := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	firstEnd := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	_, _, err = svc1.GetDailyPrices(ctx, securityID, firstStart, firstEnd)
	if err != nil {
		t.Fatalf("First GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Error("First request: Expected provider to be called")
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
	mockServer2 := createMockPriceServer(prices, &callCount2)
	defer mockServer2.Close()

	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mockServer2.URL),
		Treasury: avClient,
	})

	// Apr 1–30, 2025: entirely before IPO; the DB cache already covers inception onward,
	// so DetermineFetch must recognise the request is within the "before cached start"
	// region and return no data without hitting the provider.
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

	// The key assertion: the provider must NOT have been called for the second request.
	// Since it's a fresh service instance the only way to avoid a provider call is if
	// DetermineFetch correctly reads the DB cache and skips the fetch.
	if atomic.LoadInt32(&callCount2) > 0 {
		t.Errorf("Second request: Expected NO provider calls (DB cache sufficient), got %d", callCount2)
	}
}

// TestFetchPricingByTicker tests fetching prices by ticker instead of security_id
func TestFetchPricingByTicker(t *testing.T) {
	t.Parallel()
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
	prices := generatePriceData(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, priceClient, nil, avClient)

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
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, priceClient, nil, avClient)

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
	t.Parallel()
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

	// Set up mock price server with a call counter — it must NOT be called
	var callCount int32
	mockServer := createMockPriceServer(nil, &callCount)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, priceClient, nil, avClient)

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
	t.Parallel()
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

	// Provide mock prices so the provider call succeeds
	prices := generatePriceData(inception, time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC))
	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupPricingTestRouter(pool, priceClient, nil, avClient)

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
	t.Parallel()
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
	mockServer := createMockPriceServer(nil, &callCount)
	defer mockServer.Close()

	priceClient := alphavantage.NewClient("test-key", mockServer.URL)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{Price: priceClient, Treasury: avClient})

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
	t.Parallel()
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
	pricesA := generatePriceData(rangeAStart, rangeAEnd)
	var callCount1 int32
	mockServerA := createMockPriceServer(pricesA, &callCount1)
	defer mockServerA.Close()
	svc1 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mockServerA.URL),
		Treasury: avClient,
	})

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
	pricesGapAndB := generatePriceData(rangeAEnd.AddDate(0, 0, 1), rangeBEnd)
	var callCount2 int32
	mockServerB := createMockPriceServer(pricesGapAndB, &callCount2)
	defer mockServerB.Close()
	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mockServerB.URL),
		Treasury: avClient,
	})

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

// TestPriceRangeNoGaps_BackwardFill is the backward-fill counterpart of TestPriceRangeNoGaps.
// It caches a LATER range first (2025-01-02 → 2025-06-30), then requests an EARLIER range
// (2023-01-01 → 2023-02-28). DetermineFetch must extend adjEndDT to close the gap (up to
// 2025-01-01) so the provider is called with the full intermediate span, not just range A.
//
// Eric is paranoid that the unit tests might have a gap based on fetched-data-adjustments:
// TestDetermineFetch only verifies the adjusted date values returned by DetermineFetch, but
// does not verify that the data actually fetched and written to fact_price is contiguous.
// This integration test walks the actual stored rows to prove no weekday gaps exist.
func TestPriceRangeNoGaps_BackwardFill(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	securityID, err := createTestSecurity(pool, "TSTBGAP", "Backward Gap Test Security", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTBGAP")

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")

	// rangeBStart/End is fetched FIRST (the later range).
	rangeBStart := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	rangeBEnd := time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC)
	// rangeA is the earlier range requested SECOND; triggers backward gap fill.
	rangeAStart := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeAEnd := time.Date(2023, 2, 28, 0, 0, 0, 0, time.UTC)
	// DetermineFetch will set adjEndDT = rangeBStart - 1 day = 2025-01-01 to close the gap.
	adjEndDT := rangeBStart.AddDate(0, 0, -1)

	// First call: fetch and cache range B (the later, larger range).
	pricesB := generatePriceData(rangeBStart, rangeBEnd)
	var callCount1 int32
	mockServerB := createMockPriceServer(pricesB, &callCount1)
	defer mockServerB.Close()
	svc1 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mockServerB.URL),
		Treasury: avClient,
	})
	if _, _, err := svc1.GetDailyPrices(ctx, securityID, rangeBStart, rangeBEnd); err != nil {
		t.Fatalf("GetDailyPrices B: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Fatal("Expected provider to be called for range B")
	}

	// Second call: request range A (the earlier range).
	// DetermineFetch detects a backward gap and extends adjEndDT to 2025-01-01,
	// so the provider is called with 2023-01-01 → 2025-01-01 (not just rangeAEnd).
	// The mock must cover the full extended range.
	pricesGapAndA := generatePriceData(rangeAStart, adjEndDT)
	var callCount2 int32
	mockServerA := createMockPriceServer(pricesGapAndA, &callCount2)
	defer mockServerA.Close()
	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mockServerA.URL),
		Treasury: avClient,
	})
	if _, _, err := svc2.GetDailyPrices(ctx, securityID, rangeAStart, rangeAEnd); err != nil {
		t.Fatalf("GetDailyPrices A: %v", err)
	}
	if atomic.LoadInt32(&callCount2) == 0 {
		t.Fatal("Expected provider to be called for range A (backward gap fill)")
	}

	// Read what fact_price_range now claims.
	var claimedStart, claimedEnd time.Time
	if err := pool.QueryRow(ctx,
		`SELECT start_date, end_date FROM fact_price_range WHERE security_id = $1`,
		securityID).Scan(&claimedStart, &claimedEnd); err != nil {
		t.Fatalf("Read fact_price_range: %v", err)
	}
	t.Logf("fact_price_range: %s → %s", claimedStart.Format("2006-01-02"), claimedEnd.Format("2006-01-02"))

	// Claimed start must be rangeAStart (2023-01-01), not minDate from the fetched data.
	if !claimedStart.Equal(rangeAStart) {
		t.Errorf("start_date = %s, want %s (requested rangeAStart, not data minDate)",
			claimedStart.Format("2006-01-02"), rangeAStart.Format("2006-01-02"))
	}

	// Claimed end must span to at least rangeBEnd (the later range dominates).
	if claimedEnd.Before(rangeBEnd) {
		t.Errorf("end_date = %s, want >= %s", claimedEnd.Format("2006-01-02"), rangeBEnd.Format("2006-01-02"))
	}

	// Invariant: no weekday gaps inside the claimed range.
	// (The mock omits weekends but not NYSE holidays; weekday coverage is what matters here.)
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

// TestPriceRangeBackwardFill_MissingEndDay tests the scenario where:
//  1. A later range (2025-01-01 → 2025-01-31) is cached first.
//  2. An earlier range (2024-12-01 → 2024-12-31) is requested second.
//  3. DetermineFetch extends adjEndDT to 2024-12-31 to close the backward gap.
//  4. The provider returns no data for 2024-12-31 (lightly traded; that day is simply absent).
//
// The service should store the range starting at 2024-12-01 (the requested start, not the data
// start) and ending at 2024-12-30 (the last actual data day). The GREATEST upsert then extends
// the claimed range end to 2025-01-31. A subsequent request for [2024-12-01, 2024-12-31] must
// NOT re-fetch even though 2024-12-31 has no row in fact_price — the cache correctly knows it
// checked that date and found nothing.
func TestPriceRangeBackwardFill_MissingEndDay(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	securityID, err := createTestSecurity(pool, "TSTMISSED", "Missing End Day Security", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTMISSED")

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")

	// First fetch: 2025-01-01 to 2025-01-31 (the later range).
	firstStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	firstEnd := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)

	pricesFirst := generatePriceData(firstStart, firstEnd)
	var callCount1 int32
	mock1 := createMockPriceServer(pricesFirst, &callCount1)
	defer mock1.Close()
	svc1 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock1.URL),
		Treasury: avClient,
	})
	if _, _, err := svc1.GetDailyPrices(ctx, securityID, firstStart, firstEnd); err != nil {
		t.Fatalf("First GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Fatal("Expected provider call for first fetch")
	}

	// Second fetch request: 2024-12-01 to 2024-12-31.
	// DetermineFetch will extend adjEndDT to 2024-12-31 (firstStart - 1) to close the gap.
	// The mock deliberately omits 2024-12-31 — that day had no trading data.
	secondStart := time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC)
	secondEnd := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
	lastDataDay := time.Date(2024, 12, 30, 0, 0, 0, 0, time.UTC)

	pricesSecond := generatePriceData(secondStart, lastDataDay) // Dec 31 intentionally absent
	var callCount2 int32
	mock2 := createMockPriceServer(pricesSecond, &callCount2)
	defer mock2.Close()
	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock2.URL),
		Treasury: avClient,
	})
	if _, _, err := svc2.GetDailyPrices(ctx, securityID, secondStart, secondEnd); err != nil {
		t.Fatalf("Second GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount2) == 0 {
		t.Fatal("Expected provider call for backward gap fill")
	}

	// Verify fact_price_range covers the combined range.
	var claimedStart, claimedEnd time.Time
	if err := pool.QueryRow(ctx,
		`SELECT start_date, end_date FROM fact_price_range WHERE security_id = $1`,
		securityID).Scan(&claimedStart, &claimedEnd); err != nil {
		t.Fatalf("Read fact_price_range: %v", err)
	}
	t.Logf("fact_price_range: %s → %s", claimedStart.Format("2006-01-02"), claimedEnd.Format("2006-01-02"))

	// Start must be the requested secondStart (2024-12-01), not minDate.
	if !claimedStart.Equal(secondStart) {
		t.Errorf("start_date = %s, want %s", claimedStart.Format("2006-01-02"), secondStart.Format("2006-01-02"))
	}
	// End must be at least firstEnd (2025-01-31) after GREATEST expansion.
	if claimedEnd.Before(firstEnd) {
		t.Errorf("end_date = %s, want >= %s (first-fetch end)", claimedEnd.Format("2006-01-02"), firstEnd.Format("2006-01-02"))
	}

	// Dec 31 must not have a fact_price row (provider returned no data for it).
	var dec31Count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_price WHERE security_id = $1 AND date = $2`,
		securityID, secondEnd).Scan(&dec31Count); err != nil {
		t.Fatalf("Failed to count fact_price for Dec 31: %v", err)
	}
	if dec31Count != 0 {
		t.Errorf("Expected no fact_price row for 2024-12-31 (absent from provider response), got %d", dec31Count)
	}

	// Dec 1–30 must have data (21 trading weekdays, minus any NYSE holidays in that window).
	var dec1To30Count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_price WHERE security_id = $1 AND date >= $2 AND date <= $3`,
		securityID, secondStart, lastDataDay).Scan(&dec1To30Count); err != nil {
		t.Fatalf("Failed to count fact_price for Dec 1-30: %v", err)
	}
	if dec1To30Count == 0 {
		t.Error("Expected fact_price rows for 2024-12-01 through 2024-12-30")
	}

	// Third request for the same range must NOT call the provider — the cache knows it
	// already fetched [Dec 1, Dec 31] and found no data for Dec 31.
	var callCount3 int32
	mock3 := createMockPriceServer(pricesSecond, &callCount3)
	defer mock3.Close()
	svc3 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock3.URL),
		Treasury: avClient,
	})
	if _, _, err := svc3.GetDailyPrices(ctx, securityID, secondStart, secondEnd); err != nil {
		t.Fatalf("Third GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount3) > 0 {
		t.Errorf("Third request: expected NO provider call (cache covers the range), got %d", callCount3)
	}
}

// TestDetermineFetch is a pure unit test for the DetermineFetch function.
// It does not require a database connection.
func TestDetermineFetch(t *testing.T) {
	t.Parallel()
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
			// Cache covers the requested range. NextUpdate has elapsed but we are past the
			// data-settling window (after 4 AM ET the day after cached end). Historical cache
			// is trusted; no proactive re-fetch until the end date extends beyond cached end.
			name: "cache covers range, NextUpdate in past, past settling window - no fetch",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart,
				EndDate:    cacheEnd, // Feb 11
				NextUpdate: pastNextUpdate,
			},
			currentDT:      now, // Feb 12 2pm UTC = 9am ET — past 4am ET settling window
			effectiveStart: cacheStart,
			endDate:        cacheEnd,
			wantFetch:      false,
		},
		{
			// Same-day end: cache was populated before market close (before 4:30pm ET).
			// We're now within the overnight settling window (before 4am ET next day) and
			// nextUpdate has elapsed — the provider likely has updated closing prices.
			name: "end == cached end, NextUpdate past, within settling window - needs fetch",
			priceRange: &repository.PriceRange{
				StartDate:  cacheStart,
				EndDate:    cacheEnd, // Feb 11
				NextUpdate: pastNextUpdate,
			},
			currentDT:      time.Date(2026, 2, 12, 7, 0, 0, 0, time.UTC), // Feb 12 2am ET — inside settling window
			effectiveStart: cacheStart,
			endDate:        cacheEnd, // same as cached end
			wantFetch:      true,
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

// TestConcurrentFetchDeduplication verifies that when multiple goroutines simultaneously
// request prices for the same security with a cold cache, the singleflight group fires
// exactly one provider call — not one per goroutine.
//
// This is a regression test for the thundering-herd observed in /portfolios/compare,
// where 8 goroutines (Sharpe A/B, Sortino A/B, AlphaBeta A×GSPC/DIA, B×GSPC/DIA) all
// independently called computeRiskFreeRates → GetDailyPrices(US10Y) simultaneously,
// triggering 8 identical FRED HTTP requests.
func TestConcurrentFetchDeduplication(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	securityID, err := createTestSecurity(pool, "TSTDEDUP", "Dedup Test Security", models.SecurityTypeStock, &inception)
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTDEDUP")

	startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)
	prices := generatePriceData(startDate, endDate)

	var callCount int32
	// The mock server sleeps briefly so all goroutines arrive at the singleflight check
	// before the first provider response returns, making deduplication deterministic.
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "text/csv")
		fmt.Fprint(w, "timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient\n")
		for _, p := range prices {
			fmt.Fprintf(w, "%s,%.5f,%.5f,%.5f,%.5f,%.5f,%d,0.0000,1.0000\n",
				p.Date.Format("2006-01-02"), p.Open, p.High, p.Low, p.Close, p.Close, p.Volume)
		}
	}))
	defer mockServer.Close()

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")

	// WithConcurrency(8) mirrors production: the semaphore alone does not prevent
	// duplicate fetches (it only serialises them, still firing one per goroutine).
	// The singleflight group is the fix; this test confirms it works.
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mockServer.URL),
		Treasury: avClient,
	}).WithConcurrency(8)

	const numGoroutines = 8
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// A closed channel acts as a starting gun so all goroutines fire simultaneously,
	// maximising the TOCTOU window where each sees a cold cache before any writes to it.
	ready := make(chan struct{})
	errs := make([]error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			<-ready
			_, _, errs[idx] = pricingSvc.GetDailyPrices(context.Background(), securityID, startDate, endDate)
		}(i)
	}
	close(ready)
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d: GetDailyPrices failed: %v", i, e)
		}
	}

	got := atomic.LoadInt32(&callCount)
	if got != 1 {
		t.Errorf("provider called %d times for %d concurrent cache-miss goroutines; want exactly 1 (singleflight deduplication)",
			got, numGoroutines)
	}
}

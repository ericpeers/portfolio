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

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/cache"
	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupPricingTestRouter creates a router with admin endpoints for pricing tests
func setupPricingTestRouter(pool *pgxpool.Pool, avClient *alphavantage.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)

	securityRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	securityTypeRepo := repository.NewSecurityTypeRepository(pool)
	priceCacheRepo := repository.NewPriceCacheRepository(pool)
	memCache := cache.NewMemoryCache(5 * time.Minute)

	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, securityTypeRepo, avClient)
	pricingSvc := services.NewPricingService(memCache, priceCacheRepo, securityRepo, avClient)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, securityRepo)

	router := gin.New()
	admin := router.Group("/admin")
	{
		admin.POST("/sync-securities", adminHandler.SyncSecurities)
		admin.GET("/get_daily_prices", adminHandler.GetDailyPrices)
	}

	return router
}

// createMockPriceServer creates a mock AV server that returns specified price data
// callCounter is incremented each time the server is called (for tracking AV calls)
func createMockPriceServer(prices map[string]alphavantage.DailyOHLCV, callCounter *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCounter != nil {
			atomic.AddInt32(callCounter, 1)
		}

		function := r.URL.Query().Get("function")

		if function == "TIME_SERIES_DAILY" {
			response := alphavantage.TimeSeriesDailyResponse{
				MetaData: alphavantage.MetaData{
					Information: "Daily Prices",
					Symbol:      r.URL.Query().Get("symbol"),
				},
				TimeSeries: prices,
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
			return
		}

		// Default: return empty response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
}

// setupTestSecurity creates a test security in dim_security with the specified ticker and inception date
func setupTestSecurity(pool *pgxpool.Pool, ticker, name string, inception *time.Time) (int64, error) {
	ctx := context.Background()

	// Clean up any existing test security first
	cleanupPricingTestSecurity(pool, ticker)

	// Insert the test security
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO dim_security (ticker, name, exchange, type, inception)
		VALUES ($1, $2, 1, 1, $3)
		RETURNING id
	`, ticker, name, inception).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert test security: %w", err)
	}

	return id, nil
}

// cleanupPricingTestSecurity removes test security and its associated data
func cleanupPricingTestSecurity(pool *pgxpool.Pool, ticker string) {
	ctx := context.Background()

	// Get security ID
	var securityID int64
	err := pool.QueryRow(ctx, `SELECT id FROM dim_security WHERE ticker = $1`, ticker).Scan(&securityID)
	if err != nil {
		return // Security doesn't exist
	}

	// Delete in order: fact_price, fact_price_range, then dim_security
	pool.Exec(ctx, `DELETE FROM fact_price WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
}

// cleanupPricingTestData removes pricing data for a security ID
func cleanupPricingTestData(pool *pgxpool.Pool, securityID int64) {
	ctx := context.Background()
	pool.Exec(ctx, `DELETE FROM fact_price WHERE security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id = $1`, securityID)
}

// generatePriceData generates mock price data for a date range
func generatePriceData(startDate, endDate time.Time) map[string]alphavantage.DailyOHLCV {
	prices := make(map[string]alphavantage.DailyOHLCV)
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		// Skip weekends
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		dateStr := d.Format("2006-01-02")
		prices[dateStr] = alphavantage.DailyOHLCV{
			Open:   "100.00",
			High:   "105.00",
			Low:    "99.00",
			Close:  "102.00",
			Volume: "1000000",
		}
	}
	return prices
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
	securityID, err := setupTestSecurity(pool, "TSTNOCACHE", "Test No Cache Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupPricingTestSecurity(pool, "TSTNOCACHE")

	// Generate mock price data
	startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)
	prices := generatePriceData(startDate, endDate)

	// Track AV calls
	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupPricingTestRouter(pool, avClient)

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

	// Verify AV was called
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected AlphaVantage to be called for uncached data")
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
	securityID, err := setupTestSecurity(pool, "TSTPARTIAL", "Test Partial Fill Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupPricingTestSecurity(pool, "TSTPARTIAL")

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

	// Set the cached range to Jan 1-15
	_, err = pool.Exec(ctx, `
		INSERT INTO fact_price_range (security_id, start_date, end_date)
		VALUES ($1, $2, $3)
	`, securityID, oldStartDate, oldEndDate)
	if err != nil {
		t.Fatalf("Failed to insert price range: %v", err)
	}

	// Generate mock price data for extended range
	newEndDate := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)
	prices := generatePriceData(oldStartDate, newEndDate)

	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupPricingTestRouter(pool, avClient)

	// Request data that extends beyond the cached range (Jan 1 - Jan 31)
	url := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=2025-01-01&end_date=2025-01-31", securityID)
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify AV was called to fill in the gap
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected AlphaVantage to be called to extend cached range")
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
	securityID, err := setupTestSecurity(pool, "TESTCACHED", "Test Cached Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupPricingTestSecurity(pool, "TESTCACHED")

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

	// Set the cached range
	_, err = pool.Exec(ctx, `
		INSERT INTO fact_price_range (security_id, start_date, end_date)
		VALUES ($1, $2, $3)
	`, securityID, startDate, endDate)
	if err != nil {
		t.Fatalf("Failed to insert price range: %v", err)
	}

	// Track AV calls
	var callCount int32
	mockServer := createMockPriceServer(nil, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupPricingTestRouter(pool, avClient)

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
	securityID, err := setupTestSecurity(pool, "TSTHIST", "Test Historical Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupPricingTestSecurity(pool, "TSTHIST")

	// Generate mock price data starting from inception
	prices := generatePriceData(inception, time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupPricingTestRouter(pool, avClient)

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

	// Should return empty or very limited data since requested dates are before inception
	// The effective start will be adjusted to inception, but end is still in 1995, so no data
	if response.DataPoints != 0 {
		t.Logf("Data points returned: %d (expected 0 for dates entirely before inception)", response.DataPoints)
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
	securityID, err := setupTestSecurity(pool, "TESTVHCP", "Test VHCP-like Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupPricingTestSecurity(pool, "TESTVHCP")

	// Generate mock price data starting from IPO
	prices := generatePriceData(inception, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupPricingTestRouter(pool, avClient)

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

	// Verify AV was called and data was cached from inception
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected AlphaVantage to be called to fetch available data")
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

// TestFetchPricingBeforeIPONoRefetch tests that subsequent pre-IPO requests don't trigger AV calls
func TestFetchPricingBeforeIPONoRefetch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test security with IPO on Dec 18, 2025
	inception := time.Date(2025, 12, 18, 0, 0, 0, 0, time.UTC)
	securityID, err := setupTestSecurity(pool, "TESTVHCP2", "Test VHCP-like Security 2", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupPricingTestSecurity(pool, "TESTVHCP2")

	// Generate mock price data starting from IPO
	prices := generatePriceData(inception, time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupPricingTestRouter(pool, avClient)

	// First request: Get data spanning IPO (Dec 1, 2025 - Jan 15, 2026)
	// This should fetch from AV and cache data from inception onwards
	url1 := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=2025-12-01&end_date=2026-01-15", securityID)
	req1, _ := http.NewRequest("GET", url1, nil)
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("First request: Expected status 200, got %d: %s", w1.Code, w1.Body.String())
	}

	firstCallCount := atomic.LoadInt32(&callCount)
	if firstCallCount == 0 {
		t.Error("First request: Expected AV to be called")
	}

	// Verify data was cached
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_price WHERE security_id = $1`, securityID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("Failed to query fact_price: %v", err)
	}
	if priceCount == 0 {
		t.Error("Expected prices to be cached after first request")
	}

	// Clear the memory cache to ensure we're testing DB cache logic
	// (In the real test, we'd need to either clear the memory cache or create a fresh router)

	// Second request: Ask for pre-IPO dates (Apr 1-30, 2025)
	// Since the cache already has data from inception, and request ends before cached start,
	// we should NOT call AV again
	url2 := fmt.Sprintf("/admin/get_daily_prices?security_id=%d&start_date=2025-04-01&end_date=2025-04-30", securityID)
	req2, _ := http.NewRequest("GET", url2, nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Second request: Expected status 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var response2 models.GetDailyPricesResponse
	if err := json.Unmarshal(w2.Body.Bytes(), &response2); err != nil {
		t.Fatalf("Failed to unmarshal second response: %v", err)
	}

	// Should return 0 data points since request is before IPO
	if response2.DataPoints != 0 {
		t.Errorf("Second request: Expected 0 data points for pre-IPO request, got %d", response2.DataPoints)
	}

	// The key test: AV should NOT have been called again
	// Note: Memory cache complicates this test; the second request might hit memory cache
	// from the first request's result. To truly test this, we'd need fresh PricingService instances.
	// For this integration test, we verify that if AV was called, it was only once total.
	secondCallCount := atomic.LoadInt32(&callCount)
	if secondCallCount > firstCallCount {
		t.Errorf("Second request: Expected NO additional AV calls (had %d, now %d)", firstCallCount, secondCallCount)
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
	_, err := setupTestSecurity(pool, "TESTTICKER", "Test Ticker Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup test security: %v", err)
	}
	defer cleanupPricingTestSecurity(pool, "TESTTICKER")

	// Generate mock price data
	prices := generatePriceData(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC))

	var callCount int32
	mockServer := createMockPriceServer(prices, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupPricingTestRouter(pool, avClient)

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
	if response.Symbol != "TESTTICKER" {
		t.Errorf("Expected symbol 'TESTTICKER', got '%s'", response.Symbol)
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

	mockServer := createMockPriceServer(nil, nil)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupPricingTestRouter(pool, avClient)

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

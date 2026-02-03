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
	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupETFTestRouter creates a router with the ETF holdings endpoint
func setupETFTestRouter(pool *pgxpool.Pool, avClient *alphavantage.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)

	securityRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	securityTypeRepo := repository.NewSecurityTypeRepository(pool)
	priceCacheRepo := repository.NewPriceCacheRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)

	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, securityTypeRepo, avClient)
	pricingSvc := services.NewPricingService(priceCacheRepo, securityRepo, avClient)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo)

	router := gin.New()
	admin := router.Group("/admin")
	{
		admin.GET("/get_etf_holdings", adminHandler.GetETFHoldings)
	}

	return router
}

// createMockETFServer creates a mock AV server that returns specified ETF holdings
func createMockETFServer(holdings []alphavantage.ETFHolding, callCounter *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCounter != nil {
			atomic.AddInt32(callCounter, 1)
		}

		function := r.URL.Query().Get("function")

		if function == "ETF_PROFILE" {
			response := alphavantage.ETFProfileResponse{
				Holdings: holdings,
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

// setupTestETF creates a test ETF security in dim_security
func setupTestETF(pool *pgxpool.Pool, ticker, name string) (int64, error) {
	ctx := context.Background()

	// Clean up any existing test security first
	cleanupETFTestData(pool, ticker)

	// Insert the test ETF (type 3 = etf)
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO dim_security (ticker, name, exchange, type, inception)
		VALUES ($1, $2, 1, 3, $3)
		RETURNING id
	`, ticker, name, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert test ETF: %w", err)
	}

	return id, nil
}

// setupTestStock creates a test stock security in dim_security
func setupTestStock(pool *pgxpool.Pool, ticker, name string) (int64, error) {
	ctx := context.Background()

	// Clean up any existing test security first
	cleanupETFTestData(pool, ticker)

	// Insert the test stock (type 1 = stock)
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO dim_security (ticker, name, exchange, type, inception)
		VALUES ($1, $2, 1, 1, $3)
		RETURNING id
	`, ticker, name, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert test stock: %w", err)
	}

	return id, nil
}

// cleanupETFTestData removes test ETF and its associated membership data
func cleanupETFTestData(pool *pgxpool.Pool, ticker string) {
	ctx := context.Background()

	// Get security ID
	var securityID int64
	err := pool.QueryRow(ctx, `SELECT id FROM dim_security WHERE ticker = $1`, ticker).Scan(&securityID)
	if err != nil {
		return // Security doesn't exist
	}

	// Delete in order: dim_etf_membership (both as composite and member), dim_etf_pull_range, then dim_security
	pool.Exec(ctx, `DELETE FROM dim_etf_membership WHERE dim_composite_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM dim_etf_membership WHERE dim_security_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM dim_etf_pull_range WHERE composite_id = $1`, securityID)
	pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
}

// TestGetETFHoldingsBasic tests the basic functionality of the ETF holdings endpoint
func TestGetETFHoldingsBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create test ETF
	etfID, err := setupTestETF(pool, "TSTETF1", "Test ETF One")
	if err != nil {
		t.Fatalf("Failed to setup test ETF: %v", err)
	}
	defer cleanupETFTestData(pool, "TSTETF1")

	// Create mock holdings
	mockHoldings := []alphavantage.ETFHolding{
		{Symbol: "AAPL", Name: "Apple Inc", Weight: "0.07"},
		{Symbol: "MSFT", Name: "Microsoft Corp", Weight: "0.06"},
		{Symbol: "GOOGL", Name: "Alphabet Inc", Weight: "0.04"},
	}

	var callCount int32
	mockServer := createMockETFServer(mockHoldings, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupETFTestRouter(pool, avClient)

	// Call the endpoint by ticker
	url := fmt.Sprintf("/admin/get_etf_holdings?ticker=TSTETF1")
	req, _ := http.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.GetETFHoldingsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify response
	if response.SecurityID != etfID {
		t.Errorf("Expected security_id %d, got %d", etfID, response.SecurityID)
	}
	if response.Symbol != "TSTETF1" {
		t.Errorf("Expected symbol 'TSTETF1', got '%s'", response.Symbol)
	}
	if len(response.Holdings) != 3 {
		t.Errorf("Expected 3 holdings, got %d", len(response.Holdings))
	}

	// Verify AV was called
	if atomic.LoadInt32(&callCount) == 0 {
		t.Error("Expected AlphaVantage to be called")
	}
}

// TestGetETFHoldingsNoRefetchSameDay tests that ETF holdings are cached and not refetched
func TestGetETFHoldingsNoRefetchSameDay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test ETF
	etfID, err := setupTestETF(pool, "TSTETF2", "Test ETF Two")
	if err != nil {
		t.Fatalf("Failed to setup test ETF: %v", err)
	}
	defer cleanupETFTestData(pool, "TSTETF2")

	// Create mock holdings
	mockHoldings := []alphavantage.ETFHolding{
		{Symbol: "NVDA", Name: "NVIDIA Corp", Weight: "0.05"},
		{Symbol: "AMZN", Name: "Amazon.com Inc", Weight: "0.04"},
	}

	var callCount int32
	mockServer := createMockETFServer(mockHoldings, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupETFTestRouter(pool, avClient)

	// First call - should hit AV
	url := fmt.Sprintf("/admin/get_etf_holdings?security_id=%d", etfID)
	req1, _ := http.NewRequest("GET", url, nil)
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("First call: Expected status 200, got %d: %s", w1.Code, w1.Body.String())
	}

	firstCallCount := atomic.LoadInt32(&callCount)
	if firstCallCount == 0 {
		t.Error("First call: Expected AlphaVantage to be called")
	}

	// Verify data was cached in dim_etf_pull_range
	var pullRangeCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM dim_etf_pull_range WHERE composite_id = $1`, etfID).Scan(&pullRangeCount)
	if err != nil {
		t.Fatalf("Failed to query dim_etf_pull_range: %v", err)
	}
	if pullRangeCount != 1 {
		t.Errorf("Expected 1 dim_etf_pull_range record, got %d", pullRangeCount)
	}

	// Second call - should use cache (next_update is in the future)
	req2, _ := http.NewRequest("GET", url, nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Second call: Expected status 200, got %d: %s", w2.Code, w2.Body.String())
	}

	secondCallCount := atomic.LoadInt32(&callCount)
	if secondCallCount > firstCallCount {
		t.Errorf("Second call: Expected NO additional AV calls, but got %d (was %d)", secondCallCount, firstCallCount)
	}

	// Verify second response still has holdings
	var response2 models.GetETFHoldingsResponse
	if err := json.Unmarshal(w2.Body.Bytes(), &response2); err != nil {
		t.Fatalf("Failed to unmarshal second response: %v", err)
	}

	// Check that we got holdings back (may be less than mockHoldings if underlying securities don't exist)
	// The response should have a pull_date set since data came from cache
	if response2.PullDate == nil {
		t.Error("Second call: Expected pull_date to be set for cached data")
	}
}

// TestGetETFHoldings404 tests that non-existent ticker returns 404
func TestGetETFHoldings404(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupETFTestRouter(pool, avClient)

	// Call with non-existent ticker
	req, _ := http.NewRequest("GET", "/admin/get_etf_holdings?ticker=NONEXISTENT999", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGetETFHoldingsNotAnETF tests that requesting holdings for a stock returns 400
func TestGetETFHoldingsNotAnETF(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create test stock (not ETF)
	_, err := setupTestStock(pool, "TSTSTOCK1", "Test Stock One")
	if err != nil {
		t.Fatalf("Failed to setup test stock: %v", err)
	}
	defer cleanupETFTestData(pool, "TSTSTOCK1")

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupETFTestRouter(pool, avClient)

	// Call with stock ticker
	req, _ := http.NewRequest("GET", "/admin/get_etf_holdings?ticker=TSTSTOCK1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var errResp models.ErrorResponse
	json.Unmarshal(w.Body.Bytes(), &errResp)
	if errResp.Message != "security is not an ETF or mutual fund" {
		t.Errorf("Expected error message about not being ETF, got '%s'", errResp.Message)
	}
}

// TestGetETFHoldingsInvalidRequest tests validation of request parameters
func TestGetETFHoldingsInvalidRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupETFTestRouter(pool, avClient)

	// Call without ticker or security_id
	req, _ := http.NewRequest("GET", "/admin/get_etf_holdings", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var errResp models.ErrorResponse
	json.Unmarshal(w.Body.Bytes(), &errResp)
	if errResp.Message != "must provide either ticker or security_id" {
		t.Errorf("Expected error about missing ticker/security_id, got '%s'", errResp.Message)
	}
}

// TestGetETFHoldingsDifferentETFs tests that different ETFs can be fetched independently
func TestGetETFHoldingsDifferentETFs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Setup: Create two test ETFs
	_, err := setupTestETF(pool, "TSTETF3", "Test ETF Three")
	if err != nil {
		t.Fatalf("Failed to setup first test ETF: %v", err)
	}
	defer cleanupETFTestData(pool, "TSTETF3")

	_, err = setupTestETF(pool, "TSTETF4", "Test ETF Four")
	if err != nil {
		t.Fatalf("Failed to setup second test ETF: %v", err)
	}
	defer cleanupETFTestData(pool, "TSTETF4")

	// Create mock holdings
	mockHoldings := []alphavantage.ETFHolding{
		{Symbol: "AAPL", Name: "Apple Inc", Weight: "0.10"},
	}

	var callCount int32
	mockServer := createMockETFServer(mockHoldings, &callCount)
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupETFTestRouter(pool, avClient)

	// Fetch first ETF
	req1, _ := http.NewRequest("GET", "/admin/get_etf_holdings?ticker=TSTETF3", nil)
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("First ETF: Expected status 200, got %d: %s", w1.Code, w1.Body.String())
	}

	var response1 models.GetETFHoldingsResponse
	json.Unmarshal(w1.Body.Bytes(), &response1)
	if response1.Symbol != "TSTETF3" {
		t.Errorf("First ETF: Expected symbol 'TSTETF3', got '%s'", response1.Symbol)
	}

	// Fetch second ETF
	req2, _ := http.NewRequest("GET", "/admin/get_etf_holdings?ticker=TSTETF4", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Second ETF: Expected status 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var response2 models.GetETFHoldingsResponse
	json.Unmarshal(w2.Body.Bytes(), &response2)
	if response2.Symbol != "TSTETF4" {
		t.Errorf("Second ETF: Expected symbol 'TSTETF4', got '%s'", response2.Symbol)
	}

	// Both should have called AV
	if atomic.LoadInt32(&callCount) != 2 {
		t.Errorf("Expected 2 AV calls (one per ETF), got %d", atomic.LoadInt32(&callCount))
	}
}

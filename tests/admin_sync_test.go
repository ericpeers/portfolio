package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupAdminTestRouter creates a router with admin endpoints using a custom AV client
func setupAdminTestRouter(pool *pgxpool.Pool, avClient *alphavantage.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)

	securityRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	priceCacheRepo := repository.NewPriceCacheRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)

	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, avClient)
	pricingSvc := services.NewPricingService(priceCacheRepo, securityRepo, avClient)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo)

	router := gin.New()
	admin := router.Group("/admin")
	{
		admin.POST("/sync-securities", adminHandler.SyncSecurities)
	}

	return router
}

// TestSyncSecuritiesBasic tests basic sync functionality with known securities
func TestSyncSecuritiesBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Clean up test securities before test
	cleanupTestSecurities(pool, []string{"TESTSYNC1", "TESTSYNC2", "TESTSYNC3"})

	// Create mock AlphaVantage server
	csvResponse := `symbol,name,exchange,assetType,ipoDate,delistingDate,status
TESTSYNC1,Test Security One,NASDAQ,Stock,2020-01-15,null,Active
TESTSYNC2,Test Security Two,NYSE,ETF,2019-06-01,null,Active
TESTSYNC3,Test Security Three,NASDAQ,Stock,2021-03-20,null,Active`

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(csvResponse))
	}))
	defer mockServer.Close()

	// Create AV client pointing to mock server
	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)

	router := setupAdminTestRouter(pool, avClient)

	// Call sync endpoint
	req, _ := http.NewRequest("POST", "/admin/sync-securities", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify results
	if result.SecuritiesInserted != 3 {
		t.Errorf("Expected 3 securities inserted, got %d", result.SecuritiesInserted)
		t.Errorf("Err: %s", w.Body.String())
	}

	if result.SecuritiesSkipped != 0 {
		t.Errorf("Expected 0 securities skipped, got %d", result.SecuritiesSkipped)
	}

	// Verify securities exist in database
	var count int
	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM dim_security WHERE ticker IN ('TESTSYNC1', 'TESTSYNC2', 'TESTSYNC3')`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query securities: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 securities in database, got %d", count)
	}

	// Clean up
	cleanupTestSecurities(pool, []string{"TESTSYNC1", "TESTSYNC2", "TESTSYNC3"})
}

// TestSyncSecuritiesIdempotency tests that running sync twice skips existing securities
func TestSyncSecuritiesIdempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Clean up test securities before test
	cleanupTestSecurities(pool, []string{"IDEM1", "IDEM2", "IDEM3", "NEW1", "NEW2"})

	// First sync: 3 securities
	csvResponse1 := `symbol,name,exchange,assetType,ipoDate,delistingDate,status
IDEM1,Idempotent Test One,NASDAQ,Stock,2020-01-15,null,Active
IDEM2,Idempotent Test Two,NYSE,ETF,2019-06-01,null,Active
IDEM3,Idempotent Test Three,NASDAQ,Stock,2021-03-20,null,Active`

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(csvResponse1))
	}))

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupAdminTestRouter(pool, avClient)

	// First sync
	req1, _ := http.NewRequest("POST", "/admin/sync-securities", nil)
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("First sync failed: %d - %s", w1.Code, w1.Body.String())
	}

	var result1 services.SyncSecuritiesResult
	if err := json.Unmarshal(w1.Body.Bytes(), &result1); err != nil {
		t.Fatalf("Failed to unmarshal first result: %v", err)
	}

	if result1.SecuritiesInserted != 3 {
		t.Errorf("First sync: expected 3 inserted, got %d", result1.SecuritiesInserted)
	}

	mockServer.Close()

	// Second sync: same 3 securities same exch (skipped) + 2 new ones + 1 across exchange (allowed)
	csvResponse2 := `symbol,name,exchange,assetType,ipoDate,delistingDate,status
IDEM1,Idempotent Test One,NASDAQ,Stock,2020-01-15,null,Active
IDEM2,Idempotent Test Two,NYSE,ETF,2019-06-01,null,Active
IDEM3,Idempotent Test Three,NASDAQ,Stock,2021-03-20,null,Active
NEW1,New Security One,NYSE,Stock,2022-01-01,null,Active
NEW2,New Security Two,NASDAQ,ETF,2022-06-15,null,Active
IDEM2,Idempotent Test Two In Nasdaq,NASDAQ,ETF,2021-03-19,null,Active`

	mockServer2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(csvResponse2))
	}))
	defer mockServer2.Close()

	avClient2 := alphavantage.NewClientWithBaseURL("test-key", mockServer2.URL)
	router2 := setupAdminTestRouter(pool, avClient2)

	// Second sync
	req2, _ := http.NewRequest("POST", "/admin/sync-securities", nil)
	w2 := httptest.NewRecorder()
	router2.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Second sync failed: %d - %s", w2.Code, w2.Body.String())
	}

	var result2 services.SyncSecuritiesResult
	if err := json.Unmarshal(w2.Body.Bytes(), &result2); err != nil {
		t.Fatalf("Failed to unmarshal second result: %v", err)
	}

	// Should have 3 new insertions and 3 skipped
	if result2.SecuritiesInserted != 3 {
		t.Errorf("Second sync: expected 3 inserted, got %d", result2.SecuritiesInserted)
	}

	if result2.SecuritiesSkipped != 3 {
		t.Errorf("Second sync: expected 3 skipped, got %d", result2.SecuritiesSkipped)
	}

	// Clean up
	cleanupTestSecurities(pool, []string{"IDEM1", "IDEM2", "IDEM3", "NEW1", "NEW2"})
}

// TestSyncSecuritiesNewExchange tests that a new exchange is created when encountered
func TestSyncSecuritiesNewExchange(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Clean up test data
	cleanupTestSecurities(pool, []string{"NEWEXCH1"})
	cleanupTestExchange(pool, "TEST_EXCHANGE_XYZ")

	// CSV with a new exchange that doesn't exist
	csvResponse := `symbol,name,exchange,assetType,ipoDate,delistingDate,status
NEWEXCH1,New Exchange Security,TEST_EXCHANGE_XYZ,Stock,2020-01-15,null,Active`

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(csvResponse))
	}))
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupAdminTestRouter(pool, avClient)

	// Call sync endpoint
	req, _ := http.NewRequest("POST", "/admin/sync-securities", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify new exchange was created
	if len(result.ExchangesCreated) != 1 {
		t.Errorf("Expected 1 exchange created, got %d", len(result.ExchangesCreated))
	}

	if len(result.ExchangesCreated) > 0 && result.ExchangesCreated[0] != "TEST_EXCHANGE_XYZ" {
		t.Errorf("Expected exchange 'TEST_EXCHANGE_XYZ', got '%s'", result.ExchangesCreated[0])
	}

	// Verify exchange exists in database with country="USA"
	var exchangeName, country string
	err := pool.QueryRow(ctx, `SELECT name, country FROM dim_exchanges WHERE name = $1`, "TEST_EXCHANGE_XYZ").Scan(&exchangeName, &country)
	if err != nil {
		t.Fatalf("Failed to find created exchange: %v", err)
	}

	if country != "USA" {
		t.Errorf("Expected country 'USA', got '%s'", country)
	}

	// Verify security was inserted with correct exchange reference
	var securityExchangeID, dbExchangeID int
	err = pool.QueryRow(ctx, `SELECT exchange FROM dim_security WHERE ticker = $1`, "NEWEXCH1").Scan(&securityExchangeID)
	if err != nil {
		t.Fatalf("Failed to find security: %v", err)
	}

	err = pool.QueryRow(ctx, `SELECT id FROM dim_exchanges WHERE name = $1`, "TEST_EXCHANGE_XYZ").Scan(&dbExchangeID)
	if err != nil {
		t.Fatalf("Failed to find exchange id: %v", err)
	}

	if securityExchangeID != dbExchangeID {
		t.Errorf("Security exchange ID %d doesn't match created exchange ID %d", securityExchangeID, dbExchangeID)
	}

	// Clean up
	cleanupTestSecurities(pool, []string{"NEWEXCH1"})
	cleanupTestExchange(pool, "TEST_EXCHANGE_XYZ")
}

// TestSyncSecuritiesFiltersInactive tests that inactive securities are not inserted
func TestSyncSecuritiesFiltersInactive(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Clean up test securities before test
	cleanupTestSecurities(pool, []string{"ACTIVE1", "DELISTED1"})

	// CSV with one active and one delisted security
	csvResponse := `symbol,name,exchange,assetType,ipoDate,delistingDate,status
ACTIVE1,Active Security,NASDAQ,Stock,2020-01-15,null,Active
DELISTED1,Delisted Security,NYSE,Stock,2015-01-01,2023-06-01,Delisted`

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(csvResponse))
	}))
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupAdminTestRouter(pool, avClient)

	req, _ := http.NewRequest("POST", "/admin/sync-securities", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	json.Unmarshal(w.Body.Bytes(), &result)

	// Only the active security should be inserted
	if result.SecuritiesInserted != 1 {
		t.Errorf("Expected 1 security inserted (active only), got %d", result.SecuritiesInserted)
	}

	// Clean up
	cleanupTestSecurities(pool, []string{"ACTIVE1", "DELISTED1"})
}

// TestSyncSecuritiesUnknownAssetType tests that unknown asset types are logged as errors
func TestSyncSecuritiesUnknownAssetType(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Clean up test securities before test
	cleanupTestSecurities(pool, []string{"GOODTYPE", "BADTYPE"})

	// CSV with a known and unknown asset type
	csvResponse := `symbol,name,exchange,assetType,ipoDate,delistingDate,status
GOODTYPE,Good Type Security,NASDAQ,Stock,2020-01-15,null,Active
BADTYPE,Bad Type Security,NYSE,Warrant,2020-01-15,null,Active`

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(csvResponse))
	}))
	defer mockServer.Close()

	avClient := alphavantage.NewClientWithBaseURL("test-key", mockServer.URL)
	router := setupAdminTestRouter(pool, avClient)

	req, _ := http.NewRequest("POST", "/admin/sync-securities", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	json.Unmarshal(w.Body.Bytes(), &result)

	// Only the stock should be inserted
	if result.SecuritiesInserted != 1 {
		t.Errorf("Expected 1 security inserted, got %d", result.SecuritiesInserted)
	}

	// Should have an error for the unknown asset type
	if len(result.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(result.Errors))
	}

	// Clean up
	cleanupTestSecurities(pool, []string{"GOODTYPE", "BADTYPE"})
}

// Helper functions

func cleanupTestSecurities(pool *pgxpool.Pool, tickers []string) {
	ctx := context.Background()
	for _, ticker := range tickers {
		pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
	}
}

func cleanupTestExchange(pool *pgxpool.Pool, name string) {
	ctx := context.Background()
	pool.Exec(ctx, `DELETE FROM dim_exchanges WHERE name = $1`, name)
}

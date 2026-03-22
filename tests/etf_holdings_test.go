package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- ParseETFHoldingsCSV unit tests ---

func TestParseETFHoldingsCSV_HappyPath(t *testing.T) {
	csv := "Symbol,Company,Weight\nAAPL,Apple Inc,7.83\nMSFT,Microsoft Corp,5.39\n"
	holdings, err := handlers.ParseETFHoldingsCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(holdings) != 2 {
		t.Fatalf("expected 2 holdings, got %d", len(holdings))
	}
	if holdings[0].Ticker != "AAPL" {
		t.Errorf("expected AAPL, got %s", holdings[0].Ticker)
	}
	// Weight 7.83 should be stored as 0.0783
	if diff := holdings[0].Percentage - 0.0783; diff > 0.0001 || diff < -0.0001 {
		t.Errorf("expected 0.0783, got %f", holdings[0].Percentage)
	}
	if holdings[1].Ticker != "MSFT" {
		t.Errorf("expected MSFT, got %s", holdings[1].Ticker)
	}
	if diff := holdings[1].Percentage - 0.0539; diff > 0.0001 || diff < -0.0001 {
		t.Errorf("expected 0.0539, got %f", holdings[1].Percentage)
	}
}

func TestParseETFHoldingsCSV_MissingColumn(t *testing.T) {
	csv := "Symbol,Weight\nAAPL,7.83\n"
	_, err := handlers.ParseETFHoldingsCSV(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for missing 'company' column, got nil")
	}
}

func TestParseETFHoldingsCSV_EmptySymbol(t *testing.T) {
	// Empty symbols are allowed — they represent cash/swap rows that the
	// resolver pipeline will drop with a warning rather than fail on.
	csv := "Symbol,Company,Weight\n,Ssc Government Mm Gvmxx,0.05\n"
	holdings, err := handlers.ParseETFHoldingsCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error for empty symbol: %v", err)
	}
	if len(holdings) != 1 {
		t.Fatalf("expected 1 holding, got %d", len(holdings))
	}
	if holdings[0].Ticker != "" {
		t.Errorf("expected empty ticker, got %q", holdings[0].Ticker)
	}
	if holdings[0].Name != "Ssc Government Mm Gvmxx" {
		t.Errorf("expected name %q, got %q", "Ssc Government Mm Gvmxx", holdings[0].Name)
	}
}

func TestParseETFHoldingsCSV_InvalidWeight(t *testing.T) {
	csv := "Symbol,Company,Weight\nAAPL,Apple Inc,not-a-number\n"
	_, err := handlers.ParseETFHoldingsCSV(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for invalid weight, got nil")
	}
}

func TestParseETFHoldingsCSV_HeaderOnly(t *testing.T) {
	csv := "Symbol,Company,Weight\n"
	holdings, err := handlers.ParseETFHoldingsCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(holdings) != 0 {
		t.Errorf("expected 0 holdings, got %d", len(holdings))
	}
}

func TestParseETFHoldingsCSV_CaseInsensitiveHeaders(t *testing.T) {
	csv := "SYMBOL,COMPANY,WEIGHT\nAAPL,Apple Inc,7.83\n"
	holdings, err := handlers.ParseETFHoldingsCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(holdings) != 1 {
		t.Errorf("expected 1 holding, got %d", len(holdings))
	}
}

// --- Integration tests for LoadETFHoldings handler ---

func setupFidelityTestRouter(pool *pgxpool.Pool, avClient *alphavantage.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)

	securityRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)

	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, priceRepo, avClient)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{Price: avClient, Treasury: avClient})
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo, exchangeRepo, priceRepo)

	router := gin.New()
	admin := router.Group("/admin")
	{
		admin.POST("/load_etf_holdings", adminHandler.LoadETFHoldings)
		admin.GET("/get_etf_holdings", adminHandler.GetETFHoldings)
	}
	return router
}

// buildFidelityMultipart creates a multipart request body with the given CSV and form fields.
func buildFidelityMultipart(fields map[string]string, csvContent string) (*bytes.Buffer, string) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for k, v := range fields {
		_ = writer.WriteField(k, v)
	}
	part, _ := writer.CreateFormFile("file", "holdings.csv")
	part.Write([]byte(csvContent))
	writer.Close()
	return body, writer.FormDataContentType()
}

func TestLoadETFHoldings_BasicPersist(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	etfID, err := createTestETF(pool, "TSTFID1", "Fidelity Test ETF One")
	if err != nil {
		t.Fatalf("Failed to setup test ETF: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTFID1")

	// Use real securities that exist in the database
	csvContent := "Symbol,Company,Weight\nAAPL,Apple Inc,60.00\nMSFT,Microsoft Corp,40.00\n"

	mockServer := createMockETFServer(nil, nil)
	defer mockServer.Close()
	avClient := alphavantage.NewClient("test-key", mockServer.URL)
	router := setupFidelityTestRouter(pool, avClient)

	body, contentType := buildFidelityMultipart(map[string]string{"ticker": "TSTFID1"}, csvContent)
	req, _ := http.NewRequest("POST", "/admin/load_etf_holdings", body)
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.GetETFHoldingsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if response.SecurityID != etfID {
		t.Errorf("Expected security_id %d, got %d", etfID, response.SecurityID)
	}
	if response.Ticker != "TSTFID1" {
		t.Errorf("Expected ticker TSTFID1, got %s", response.Ticker)
	}
	// Holdings normalized to sum to 1.0; both AAPL and MSFT must be present
	if len(response.Holdings) != 2 {
		t.Errorf("Expected 2 holdings, got %d: %+v", len(response.Holdings), response.Holdings)
	}

	// Verify data was actually written to dim_etf_membership
	var count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM dim_etf_membership WHERE dim_composite_id = $1`, etfID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query dim_etf_membership: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows in dim_etf_membership, got %d", count)
	}

	// Verify pull range was recorded
	var pullRangeCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM dim_etf_pull_range WHERE composite_id = $1`, etfID).Scan(&pullRangeCount)
	if err != nil {
		t.Fatalf("Failed to query dim_etf_pull_range: %v", err)
	}
	if pullRangeCount != 1 {
		t.Errorf("Expected 1 dim_etf_pull_range record, got %d", pullRangeCount)
	}
}

func TestLoadETFHoldings_NoPullDateInResponse(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	_, err := createTestETF(pool, "TSTFID2", "Fidelity Test ETF Two")
	if err != nil {
		t.Fatalf("Failed to setup test ETF: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTFID2")

	csvContent := "Symbol,Company,Weight\nAAPL,Apple Inc,100.00\n"

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupFidelityTestRouter(pool, avClient)

	body, contentType := buildFidelityMultipart(map[string]string{"ticker": "TSTFID2"}, csvContent)
	req, _ := http.NewRequest("POST", "/admin/load_etf_holdings", body)
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response models.GetETFHoldingsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// LoadETFHoldings always ingests fresh data — pull_date should not be set
	if response.PullDate != nil {
		t.Errorf("Expected pull_date to be nil for freshly loaded data, got %v", *response.PullDate)
	}
}

func TestLoadETFHoldings_NotAnETF(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	_, err := createTestStock(pool, "TSTFIDS1", "Fidelity Test Stock")
	if err != nil {
		t.Fatalf("Failed to setup test stock: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTFIDS1")

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupFidelityTestRouter(pool, avClient)

	body, contentType := buildFidelityMultipart(map[string]string{"ticker": "TSTFIDS1"}, "Symbol,Company,Weight\nAAPL,Apple Inc,100\n")
	req, _ := http.NewRequest("POST", "/admin/load_etf_holdings", body)
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestLoadETFHoldings_InvalidCSV(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	_, err := createTestETF(pool, "TSTFID3", "Fidelity Test ETF Three")
	if err != nil {
		t.Fatalf("Failed to setup test ETF: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTFID3")

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupFidelityTestRouter(pool, avClient)

	// Missing 'company' column
	body, contentType := buildFidelityMultipart(map[string]string{"ticker": "TSTFID3"}, "Symbol,Weight\nAAPL,100\n")
	req, _ := http.NewRequest("POST", "/admin/load_etf_holdings", body)
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestLoadETFHoldings_MissingFileField(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	_, err := createTestETF(pool, "TSTFID4", "Fidelity Test ETF Four")
	if err != nil {
		t.Fatalf("Failed to setup test ETF: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTFID4")

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupFidelityTestRouter(pool, avClient)

	// No file — just a ticker field
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("ticker", "TSTFID4")
	writer.Close()

	req, _ := http.NewRequest("POST", "/admin/load_etf_holdings", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestLoadETFHoldings_MissingTickerAndID(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupFidelityTestRouter(pool, avClient)

	body, contentType := buildFidelityMultipart(map[string]string{}, "Symbol,Company,Weight\nAAPL,Apple Inc,100\n")
	req, _ := http.NewRequest("POST", "/admin/load_etf_holdings", body)
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d: %s", w.Code, w.Body.String())
	}
	var errResp models.ErrorResponse
	json.Unmarshal(w.Body.Bytes(), &errResp)
	if errResp.Message != "must provide either ticker or security_id" {
		t.Errorf("Unexpected error message: %s", errResp.Message)
	}
}

func TestLoadETFHoldings_ThenGetViaAVEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	etfID, err := createTestETF(pool, "TSTFID5", "Fidelity Test ETF Five")
	if err != nil {
		t.Fatalf("Failed to setup test ETF: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTFID5")

	// Load via Fidelity endpoint
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	router := setupFidelityTestRouter(pool, avClient)

	csvContent := "Symbol,Company,Weight\nAAPL,Apple Inc,100.00\n"
	body, contentType := buildFidelityMultipart(map[string]string{"ticker": "TSTFID5"}, csvContent)
	req1, _ := http.NewRequest("POST", "/admin/load_etf_holdings", body)
	req1.Header.Set("Content-Type", contentType)
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("Load step: Expected status 200, got %d: %s", w1.Code, w1.Body.String())
	}

	// Now check the AV holdings endpoint returns cached data (pull_date set, no AV call needed)
	req2, _ := http.NewRequest("GET", fmt.Sprintf("/admin/get_etf_holdings?security_id=%d", etfID), nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Get step: Expected status 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var response models.GetETFHoldingsResponse
	if err := json.Unmarshal(w2.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Data was persisted by Fidelity load, so GetETFHoldings should serve from cache
	if response.PullDate == nil {
		t.Error("Expected pull_date to be set when serving from cache after Fidelity load")
	}

	// Verify holdings are present
	if len(response.Holdings) == 0 {
		t.Error("Expected at least 1 holding after Fidelity load")
	}

	// Verify the ETF membership table has data
	var count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM dim_etf_membership WHERE dim_composite_id = $1`, etfID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query dim_etf_membership: %v", err)
	}
	if count == 0 {
		t.Error("Expected dim_etf_membership to have rows after Fidelity load")
	}
}

// TestResolveAndPersistETFHoldings_PipelineIntegration tests the unified pipeline
// directly via the service layer with a mock AV server.
func TestResolveAndPersistETFHoldings_PipelineIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	etfID, err := createTestETF(pool, "TSTFID6", "Fidelity Test ETF Six")
	if err != nil {
		t.Fatalf("Failed to setup test ETF: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTFID6")

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")

	secRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{Price: avClient, Treasury: avClient})
	membershipSvc := services.NewMembershipService(secRepo, portfolioRepo, pricingSvc, avClient)

	_, prefetchedBySymbol, err := secRepo.GetAllSecurities(ctx)
	if err != nil {
		t.Fatalf("GetAllSecurities failed: %v", err)
	}

	// Raw holdings with real symbols (AAPL, MSFT) and an unknown symbol
	rawHoldings := []providers.ParsedETFHolding{
		{Ticker: "AAPL", Name: "Apple Inc", Percentage: 0.60},
		{Ticker: "MSFT", Name: "Microsoft Corp", Percentage: 0.40},
		{Ticker: "TSTFGXXXTST", Name: "Unknown Fund", Percentage: 0.10},
	}

	var etfSec *models.SecurityWithCountry
	for _, c := range prefetchedBySymbol["TSTFID6"] {
		if c.ID == etfID {
			etfSec = c
			break
		}
	}

	warnCtx, wc := services.NewWarningContext(ctx)
	resolved, err := membershipSvc.ResolveAndPersistETFHoldings(warnCtx, etfSec, rawHoldings, prefetchedBySymbol)
	if err != nil {
		t.Fatalf("ResolveAndPersistETFHoldings failed: %v", err)
	}

	// Unknown symbol should be dropped with a warning
	if len(resolved) != 2 {
		t.Errorf("Expected 2 resolved holdings (unknown dropped), got %d: %+v", len(resolved), resolved)
	}

	warnings := wc.GetWarnings()
	hasUnknownWarning := false
	for _, w := range warnings {
		if w.Code == models.WarnUnresolvedETFHolding {
			hasUnknownWarning = true
			break
		}
	}
	if !hasUnknownWarning {
		t.Error("Expected a WarnUnresolvedETFHolding warning for the unknown symbol")
	}

	// Weights should sum to 1.0 after normalization
	sum := 0.0
	for _, h := range resolved {
		sum += h.Percentage
	}
	if diff := sum - 1.0; diff > 0.0001 || diff < -0.0001 {
		t.Errorf("Expected resolved weights to sum to 1.0, got %f", sum)
	}

	// Verify persistence
	var count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM dim_etf_membership WHERE dim_composite_id = $1`, etfID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query dim_etf_membership: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 rows persisted, got %d", count)
	}
}

// TestPersistETFHoldings_NameDisambiguates verifies that when multiple securities
// share the same ticker (different exchanges), the CSV holding name is used to
// select the correct one. Uses NASDAQ (exchange 2) and NYSE (exchange 3) since
// both are guaranteed to exist in the schema.
func TestPersistETFHoldings_NameDisambiguates(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	const ticker = "TSTDNAME"
	const etfTicker = "TSTDNAMETF"

	// Clean up at start and end (handles previous failed runs too).
	cleanupNameDisambigTest := func() {
		pool.Exec(ctx, `DELETE FROM dim_etf_membership WHERE dim_security_id IN (SELECT id FROM dim_security WHERE ticker = $1)`, ticker)
		pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
		cleanupTestSecurity(pool, etfTicker)
	}
	cleanupNameDisambigTest()
	defer cleanupNameDisambigTest()

	// Create the ETF.
	etfID, err := createTestETF(pool, etfTicker, "Name Disambig Test ETF")
	if err != nil {
		t.Fatalf("Failed to create test ETF: %v", err)
	}

	// Insert two securities with the same ticker on different exchanges.
	// Exchange 2 = NASDAQ (USA), Exchange 3 = NYSE (USA).
	var idNasdaq, idNyse int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO dim_security (ticker, name, exchange, type) VALUES ($1, $2, 2, 'COMMON STOCK') RETURNING id`,
		ticker, "TSTDNAME Technologies USA").Scan(&idNasdaq); err != nil {
		t.Fatalf("insert NASDAQ security: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO dim_security (ticker, name, exchange, type) VALUES ($1, $2, 3, 'COMMON STOCK') RETURNING id`,
		ticker, "TSTDNAME Electronics Thailand Public Company").Scan(&idNyse); err != nil {
		t.Fatalf("insert NYSE security: %v", err)
	}

	// Create services after inserting test securities so the cache includes them.
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	secRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{Price: avClient, Treasury: avClient})
	membershipSvc := services.NewMembershipService(secRepo, portfolioRepo, pricingSvc, avClient)

	_, prefetchedBySymbol, err := secRepo.GetAllSecurities(ctx)
	if err != nil {
		t.Fatalf("GetAllSecurities failed: %v", err)
	}

	// The holding name matches "TSTDNAME Electronics Thailand Public Company" (NYSE/idNyse),
	// not "TSTDNAME Technologies USA" (NASDAQ/idNasdaq).
	rawHoldings := []providers.ParsedETFHolding{
		{Ticker: ticker, Name: "TSTDNAME Electronics Thailand PCL", Percentage: 1.0},
	}

	var etfSec *models.SecurityWithCountry
	for _, c := range prefetchedBySymbol[etfTicker] {
		if c.ID == etfID {
			etfSec = c
			break
		}
	}

	warnCtx, _ := services.NewWarningContext(ctx)
	_, err = membershipSvc.ResolveAndPersistETFHoldings(warnCtx, etfSec, rawHoldings, prefetchedBySymbol)
	if err != nil {
		t.Fatalf("ResolveAndPersistETFHoldings failed: %v", err)
	}

	// Verify the stored security ID is the one with the matching name (idNyse).
	var storedID int64
	if err := pool.QueryRow(ctx,
		`SELECT dim_security_id FROM dim_etf_membership WHERE dim_composite_id = $1`,
		etfID).Scan(&storedID); err != nil {
		t.Fatalf("query dim_etf_membership: %v", err)
	}
	if storedID != idNyse {
		t.Errorf("name disambiguation: expected security id %d (NYSE/Thailand-named), got %d (want the one matching the holding name)", idNyse, storedID)
	}
}

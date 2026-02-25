package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupLoadSecuritiesRouter creates a minimal router with the load_securities endpoint.
func setupLoadSecuritiesRouter(pool *pgxpool.Pool) *gin.Engine {
	gin.SetMode(gin.TestMode)

	secRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	// load_securities does not call the AV API; use a dead URL so any accidental call fails fast.
	avClient := alphavantage.NewClientWithBaseURL("test-key", "http://localhost:9999")

	adminSvc := services.NewAdminService(secRepo, exchangeRepo, avClient)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, avClient)
	membershipSvc := services.NewMembershipService(secRepo, portfolioRepo, pricingSvc, avClient)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, secRepo, exchangeRepo)

	router := gin.New()
	admin := router.Group("/admin")
	admin.POST("/load_securities", adminHandler.LoadSecurities)
	return router
}

// buildLoadSecuritiesRequest creates a multipart POST to /admin/load_securities
// with the given CSV content in the "file" field.
func buildLoadSecuritiesRequest(t *testing.T, csvContent string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("file", "securities.csv")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	if _, err := part.Write([]byte(csvContent)); err != nil {
		t.Fatalf("failed to write CSV content: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}
	req, err := http.NewRequest("POST", "/admin/load_securities", &buf)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req
}

// cleanupLoadSecuritiesTestData removes test securities and exchanges created by these tests.
func cleanupLoadSecuritiesTestData(pool *pgxpool.Pool, tickers []string, exchangeNames []string) {
	ctx := context.Background()
	for _, ticker := range tickers {
		pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
	}
	for _, name := range exchangeNames {
		pool.Exec(ctx, `DELETE FROM dim_exchanges WHERE name = $1`, name)
	}
}

// TestLoadSecurities_Success uploads a small valid CSV and checks counts.
func TestLoadSecurities_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesRouter(pool)

	tickers := []string{"TSTSEC1TST", "TSTSEC2TST"}
	cleanupLoadSecuritiesTestData(pool, tickers, nil)
	t.Cleanup(func() { cleanupLoadSecuritiesTestData(pool, tickers, nil) })

	csv := "ticker,name,exchange,type,currency\n" +
		"TSTSEC1TST,Test Security One TST,NASDAQ,COMMON STOCK,USD\n" +
		"TSTSEC2TST,Test Security Two TST,NYSE,ETF,USD\n"

	req := buildLoadSecuritiesRequest(t, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp models.LoadSecuritiesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Inserted != 2 {
		t.Errorf("expected inserted=2, got %d", resp.Inserted)
	}
	if resp.SkippedExisting != 0 {
		t.Errorf("expected skipped_existing=0, got %d", resp.SkippedExisting)
	}
	if resp.SkippedBadType != 0 {
		t.Errorf("expected skipped_bad_type=0, got %d", resp.SkippedBadType)
	}

	// Verify rows exist in DB
	ctx := context.Background()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM dim_security WHERE ticker = ANY($1)`, tickers).Scan(&count); err != nil {
		t.Fatalf("failed to count securities: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows in dim_security, got %d", count)
	}
}

// TestLoadSecurities_MissingColumn returns 400 when a required column is absent.
func TestLoadSecurities_MissingColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesRouter(pool)

	// CSV missing the required "exchange" column
	csv := "ticker,name,type\nTSTBADCOLTST,Bad Col TST,COMMON STOCK\n"

	req := buildLoadSecuritiesRequest(t, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestLoadSecurities_BadType skips rows with unknown type and reports the count.
func TestLoadSecurities_BadType(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesRouter(pool)

	tickers := []string{"TSTGOODTST", "TSTBADTTST"}
	cleanupLoadSecuritiesTestData(pool, tickers, nil)
	t.Cleanup(func() { cleanupLoadSecuritiesTestData(pool, tickers, nil) })

	csv := "ticker,name,exchange,type\n" +
		"TSTGOODTST,Good Type TST,NASDAQ,COMMON STOCK\n" +
		"TSTBADTTST,Bad Type TST,NYSE,SPACESHIP\n"

	req := buildLoadSecuritiesRequest(t, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp models.LoadSecuritiesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Inserted != 1 {
		t.Errorf("expected inserted=1, got %d", resp.Inserted)
	}
	if resp.SkippedBadType != 1 {
		t.Errorf("expected skipped_bad_type=1, got %d", resp.SkippedBadType)
	}
}

// TestLoadSecurities_LongTicker skips a ticker longer than 30 chars.
func TestLoadSecurities_LongTicker(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesRouter(pool)

	longTicker := strings.Repeat("X", 31) // 31 chars
	t.Cleanup(func() { cleanupLoadSecuritiesTestData(pool, []string{longTicker}, nil) })

	csv := "ticker,name,exchange,type\n" +
		longTicker + ",Long Ticker TST,NASDAQ,COMMON STOCK\n"

	req := buildLoadSecuritiesRequest(t, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp models.LoadSecuritiesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.SkippedLongTicker != 1 {
		t.Errorf("expected skipped_long_ticker=1, got %d", resp.SkippedLongTicker)
	}
	if resp.Inserted != 0 {
		t.Errorf("expected inserted=0, got %d", resp.Inserted)
	}
}

// TestLoadSecurities_AutoCreateExchange creates a new exchange when one is not in the DB.
func TestLoadSecurities_AutoCreateExchange(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesRouter(pool)

	ticker := "TSTNEWEXTST"
	exchangeName := "TST_NEW_EXCHANGE_TST"
	cleanupLoadSecuritiesTestData(pool, []string{ticker}, []string{exchangeName})
	t.Cleanup(func() { cleanupLoadSecuritiesTestData(pool, []string{ticker}, []string{exchangeName}) })

	csv := "ticker,name,exchange,type,country\n" +
		ticker + ",New Exchange TST," + exchangeName + ",COMMON STOCK,Testland\n"

	req := buildLoadSecuritiesRequest(t, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp models.LoadSecuritiesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Inserted != 1 {
		t.Errorf("expected inserted=1, got %d", resp.Inserted)
	}
	if len(resp.NewExchanges) != 1 || resp.NewExchanges[0] != exchangeName {
		t.Errorf("expected new_exchanges=[%q], got %v", exchangeName, resp.NewExchanges)
	}

	// Verify exchange exists with correct country
	ctx := context.Background()
	var country string
	if err := pool.QueryRow(ctx, `SELECT country FROM dim_exchanges WHERE name = $1`, exchangeName).Scan(&country); err != nil {
		t.Fatalf("failed to find created exchange: %v", err)
	}
	if country != "Testland" {
		t.Errorf("expected country='Testland', got %q", country)
	}
}

// TestLoadSecurities_DuplicateInFile: second row with same (ticker, exchange) is skipped.
func TestLoadSecurities_DuplicateInFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesRouter(pool)

	ticker := "TSTDUPTST"
	cleanupLoadSecuritiesTestData(pool, []string{ticker}, nil)
	t.Cleanup(func() { cleanupLoadSecuritiesTestData(pool, []string{ticker}, nil) })

	csv := "ticker,name,exchange,type\n" +
		ticker + ",Dup First TST,NASDAQ,COMMON STOCK\n" +
		ticker + ",Dup Second TST,NASDAQ,COMMON STOCK\n"

	req := buildLoadSecuritiesRequest(t, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp models.LoadSecuritiesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Inserted != 1 {
		t.Errorf("expected inserted=1, got %d", resp.Inserted)
	}
	if resp.SkippedDupInFile != 1 {
		t.Errorf("expected skipped_dup_in_file=1, got %d", resp.SkippedDupInFile)
	}
}

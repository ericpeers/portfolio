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
	"time"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Router / request helpers ---

func setupLoadSecuritiesIPORouter(pool *pgxpool.Pool) *gin.Engine {
	gin.SetMode(gin.TestMode)
	secRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	eodhdAdminClient := eodhd.NewClient("test-key", "http://localhost:9999")
	adminSvc := services.NewAdminService(secRepo, exchangeRepo, priceRepo, eodhdAdminClient, 10)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
	})
	membershipSvc := services.NewMembershipService(secRepo, portfolioRepo, pricingSvc)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, secRepo, exchangeRepo, priceRepo)

	router := gin.New()
	securities := router.Group("/admin/securities")
	securities.POST("/load_ipo_csv", adminHandler.LoadSecuritiesIPO)
	return router
}

func buildLoadIPORequest(t *testing.T, csvContent string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("file", "ipo.csv")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	if _, err := part.Write([]byte(csvContent)); err != nil {
		t.Fatalf("failed to write CSV: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}
	req, err := http.NewRequest("POST", "/admin/securities/load_ipo_csv", &buf)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req
}

// --- ParseIPODatesCSV unit tests (no DB) ---

func TestParseIPODatesCSV_HappyPath(t *testing.T) {
	t.Parallel()
	csv := "Ticker,Name,Exchange,IPO date\nAAPL,Apple,US,2020-01-15\nMSFT,Microsoft,US,20210301\n"
	rows, err := handlers.ParseIPODatesCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Ticker != "AAPL" {
		t.Errorf("expected AAPL, got %q", rows[0].Ticker)
	}
	if !rows[0].IPODate.Equal(time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("unexpected IPO date for AAPL: %v", rows[0].IPODate)
	}
	if !rows[1].IPODate.Equal(time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("unexpected IPO date for MSFT: %v", rows[1].IPODate)
	}
}

func TestParseIPODatesCSV_MissingColumn(t *testing.T) {
	t.Parallel()
	// Missing "ipo date" column
	csv := "Ticker,Name,Exchange\nAAPL,Apple,US\n"
	_, err := handlers.ParseIPODatesCSV(strings.NewReader(csv))
	if err == nil {
		t.Fatal("expected error for missing ipo date column")
	}
	if !strings.Contains(err.Error(), "ipo date") {
		t.Errorf("expected error to mention missing column, got: %s", err.Error())
	}
}

func TestParseIPODatesCSV_SkipsBadDate(t *testing.T) {
	t.Parallel()
	csv := "Ticker,Name,Exchange,IPO date\nAAPL,Apple,US,not-a-date\nMSFT,Microsoft,US,2021-03-01\n"
	rows, err := handlers.ParseIPODatesCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// AAPL silently skipped; only MSFT returned
	if len(rows) != 1 || rows[0].Ticker != "MSFT" {
		t.Errorf("expected only MSFT, got %v", rows)
	}
}

func TestParseIPODatesCSV_SkipsEmptyTicker(t *testing.T) {
	t.Parallel()
	csv := "Ticker,Name,Exchange,IPO date\n,Apple,US,2020-01-15\nMSFT,Microsoft,US,2021-03-01\n"
	rows, err := handlers.ParseIPODatesCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 || rows[0].Ticker != "MSFT" {
		t.Errorf("expected only MSFT, got %v", rows)
	}
}

func TestParseIPODatesCSV_YYYYMMDDFormat(t *testing.T) {
	t.Parallel()
	csv := "Ticker,Name,Exchange,IPO date\nGOOG,Google,US,20040819\n"
	rows, err := handlers.ParseIPODatesCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if !rows[0].IPODate.Equal(time.Date(2004, 8, 19, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("unexpected date: %v", rows[0].IPODate)
	}
}

// --- Endpoint integration tests ---

// TestLoadSecuritiesIPO_Insert: security with null inception gets its date set.
func TestLoadSecuritiesIPO_Insert(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesIPORouter(pool)

	ticker := nextTicker()
	_, err := createTestSecurity(pool, ticker, "IPO Insert Test", models.SecurityTypeStock, nil)
	if err != nil {
		t.Fatalf("failed to create test security: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(pool, ticker) })

	ipoDate := "2018-06-15"
	csv := "Ticker,Name,Exchange,IPO date\n" + ticker + ",Test Co,US," + ipoDate + "\n"

	w := httptest.NewRecorder()
	router.ServeHTTP(w, buildLoadIPORequest(t, csv))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.LoadIPODatesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Inserted != 1 {
		t.Errorf("expected inserted=1, got %d", resp.Inserted)
	}
	if resp.Skipped != 0 || resp.NoMatch != 0 || len(resp.Mismatches) != 0 {
		t.Errorf("unexpected counts: %+v", resp)
	}

	// Verify DB was updated
	ctx := context.Background()
	var stored time.Time
	if err := pool.QueryRow(ctx,
		`SELECT inception FROM dim_security WHERE ticker = $1`, ticker,
	).Scan(&stored); err != nil {
		t.Fatalf("failed to read back inception: %v", err)
	}
	if stored.Format("2006-01-02") != ipoDate {
		t.Errorf("expected inception=%s, got %s", ipoDate, stored.Format("2006-01-02"))
	}
}

// TestLoadSecuritiesIPO_Skip: security already has the same date → skipped, not re-inserted.
func TestLoadSecuritiesIPO_Skip(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesIPORouter(pool)

	ipoDate := time.Date(2018, 6, 15, 0, 0, 0, 0, time.UTC)
	ticker := nextTicker()
	_, err := createTestSecurity(pool, ticker, "IPO Skip Test", models.SecurityTypeStock, &ipoDate)
	if err != nil {
		t.Fatalf("failed to create test security: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(pool, ticker) })

	csv := "Ticker,Name,Exchange,IPO date\n" + ticker + ",Test Co,US,2018-06-15\n"

	w := httptest.NewRecorder()
	router.ServeHTTP(w, buildLoadIPORequest(t, csv))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.LoadIPODatesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.Skipped != 1 {
		t.Errorf("expected skipped=1, got %d", resp.Skipped)
	}
	if resp.Inserted != 0 || resp.NoMatch != 0 || len(resp.Mismatches) != 0 {
		t.Errorf("unexpected counts: %+v", resp)
	}
}

// TestLoadSecuritiesIPO_Mismatch: existing date differs from CSV → appears in mismatches, DB unchanged.
func TestLoadSecuritiesIPO_Mismatch(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesIPORouter(pool)

	dbDate := time.Date(2010, 3, 1, 0, 0, 0, 0, time.UTC)
	ticker := nextTicker()
	_, err := createTestSecurity(pool, ticker, "IPO Mismatch Test", models.SecurityTypeStock, &dbDate)
	if err != nil {
		t.Fatalf("failed to create test security: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(pool, ticker) })

	csv := "Ticker,Name,Exchange,IPO date\n" + ticker + ",Test Co,US,2015-07-20\n"

	w := httptest.NewRecorder()
	router.ServeHTTP(w, buildLoadIPORequest(t, csv))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.LoadIPODatesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(resp.Mismatches) != 1 {
		t.Fatalf("expected 1 mismatch, got %d", len(resp.Mismatches))
	}
	m := resp.Mismatches[0]
	if m.Ticker != ticker {
		t.Errorf("mismatch ticker: expected %s, got %s", ticker, m.Ticker)
	}
	if m.CSVDate != "2015-07-20" {
		t.Errorf("mismatch csv_date: expected 2015-07-20, got %s", m.CSVDate)
	}
	if m.DBDate != "2010-03-01" {
		t.Errorf("mismatch db_date: expected 2010-03-01, got %s", m.DBDate)
	}
	if resp.Inserted != 0 || resp.Skipped != 0 {
		t.Errorf("unexpected counts: %+v", resp)
	}

	// DB must be unchanged
	ctx := context.Background()
	var stored time.Time
	if err := pool.QueryRow(ctx,
		`SELECT inception FROM dim_security WHERE ticker = $1`, ticker,
	).Scan(&stored); err != nil {
		t.Fatalf("failed to read inception: %v", err)
	}
	if !stored.Equal(dbDate) {
		t.Errorf("DB inception should be unchanged %s, got %s", dbDate.Format("2006-01-02"), stored.Format("2006-01-02"))
	}
}

// TestLoadSecuritiesIPO_NoMatch: unknown ticker counts as no_match.
func TestLoadSecuritiesIPO_NoMatch(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesIPORouter(pool)

	csv := "Ticker,Name,Exchange,IPO date\nTSTNONEXISTENTTST,Ghost Co,US,2020-01-01\n"

	w := httptest.NewRecorder()
	router.ServeHTTP(w, buildLoadIPORequest(t, csv))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.LoadIPODatesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.NoMatch != 1 {
		t.Errorf("expected no_match=1, got %d", resp.NoMatch)
	}
	if resp.Inserted != 0 || resp.Skipped != 0 || len(resp.Mismatches) != 0 {
		t.Errorf("unexpected counts: %+v", resp)
	}
}

// TestLoadSecuritiesIPO_FileDuplicate_KeepsLatest: two CSV rows for same ticker;
// later date is used for the update, earlier is counted as file_duplicate.
func TestLoadSecuritiesIPO_FileDuplicate_KeepsLatest(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesIPORouter(pool)

	ticker := nextTicker()
	_, err := createTestSecurity(pool, ticker, "IPO Dup Test", models.SecurityTypeStock, nil)
	if err != nil {
		t.Fatalf("failed to create test security: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(pool, ticker) })

	// Earlier date first, later date second
	csv := "Ticker,Name,Exchange,IPO date\n" +
		ticker + ",Test Co,US,2010-01-01\n" +
		ticker + ",Test Co Reused,US,2022-06-15\n"

	w := httptest.NewRecorder()
	router.ServeHTTP(w, buildLoadIPORequest(t, csv))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.LoadIPODatesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.FileDuplicates != 1 {
		t.Errorf("expected file_duplicates=1, got %d", resp.FileDuplicates)
	}
	if resp.Inserted != 1 {
		t.Errorf("expected inserted=1, got %d", resp.Inserted)
	}
	if resp.Skipped != 0 || resp.NoMatch != 0 || len(resp.Mismatches) != 0 {
		t.Errorf("unexpected counts: %+v", resp)
	}

	// Verify the later date was written
	ctx := context.Background()
	var stored time.Time
	if err := pool.QueryRow(ctx,
		`SELECT inception FROM dim_security WHERE ticker = $1`, ticker,
	).Scan(&stored); err != nil {
		t.Fatalf("failed to read inception: %v", err)
	}
	if stored.Format("2006-01-02") != "2022-06-15" {
		t.Errorf("expected later date 2022-06-15 to be stored, got %s", stored.Format("2006-01-02"))
	}
}

// TestLoadSecuritiesIPO_FileDuplicate_LaterMatchesDB: duplicate where the surviving
// (later) date matches what's already in DB → skipped, not mismatch.
func TestLoadSecuritiesIPO_FileDuplicate_LaterMatchesDB(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesIPORouter(pool)

	dbDate := time.Date(2022, 6, 15, 0, 0, 0, 0, time.UTC)
	ticker := nextTicker()
	_, err := createTestSecurity(pool, ticker, "IPO Dup Match Test", models.SecurityTypeStock, &dbDate)
	if err != nil {
		t.Fatalf("failed to create test security: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(pool, ticker) })

	csv := "Ticker,Name,Exchange,IPO date\n" +
		ticker + ",Old Name,US,2010-01-01\n" +
		ticker + ",New Name,US,2022-06-15\n" // later date matches DB

	w := httptest.NewRecorder()
	router.ServeHTTP(w, buildLoadIPORequest(t, csv))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.LoadIPODatesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if resp.FileDuplicates != 1 {
		t.Errorf("expected file_duplicates=1, got %d", resp.FileDuplicates)
	}
	if resp.Skipped != 1 {
		t.Errorf("expected skipped=1, got %d", resp.Skipped)
	}
	if resp.Inserted != 0 || len(resp.Mismatches) != 0 {
		t.Errorf("unexpected counts: %+v", resp)
	}
}

// TestLoadSecuritiesIPO_MissingFile returns 400 when no file is provided.
func TestLoadSecuritiesIPO_MissingFile(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesIPORouter(pool)

	req, _ := http.NewRequest("POST", "/admin/securities/load_ipo_csv", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestLoadSecuritiesIPO_BadCSV returns 400 when the CSV is missing a required column.
func TestLoadSecuritiesIPO_BadCSV(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesIPORouter(pool)

	// CSV missing "ipo date" column
	csv := "Ticker,Name,Exchange\nAAPL,Apple,US\n"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, buildLoadIPORequest(t, csv))

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

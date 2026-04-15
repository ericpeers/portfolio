package tests

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupExportImportRouter creates a minimal router with the export-prices and import-prices endpoints.
func setupExportImportRouter(pool *pgxpool.Pool) *gin.Engine {
	gin.SetMode(gin.TestMode)

	secRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	eodhdAdminClient := eodhd.NewClient("test-key", "http://localhost:9999")

	adminSvc := services.NewAdminService(secRepo, exchangeRepo, priceRepo, eodhdAdminClient)
	pricingSvc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{Price: avClient, Treasury: avClient})
	membershipSvc := services.NewMembershipService(secRepo, portfolioRepo, pricingSvc, avClient)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, secRepo, exchangeRepo, priceRepo)

	router := gin.New()
	admin := router.Group("/admin")
	admin.GET("/export-prices", adminHandler.ExportPrices)
	admin.POST("/import-prices", adminHandler.ImportPrices)
	return router
}

// createTestSecurityOnExchange inserts a security on a specific exchange (by ID).
func createTestSecurityOnExchange(pool *pgxpool.Pool, ticker, name string, exchangeID int) (int64, error) {
	ctx := context.Background()
	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO dim_security (ticker, name, exchange, type, inception)
		VALUES ($1, $2, $3, 'COMMON STOCK', '2020-01-01')
		RETURNING id
	`, ticker, name, exchangeID).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert test security on exchange %d: %w", exchangeID, err)
	}
	return id, nil
}

// createTestExchange inserts a temporary exchange and returns its ID.
func createTestExchange(pool *pgxpool.Pool, name, country string) (int, error) {
	ctx := context.Background()
	var id int
	err := pool.QueryRow(ctx,
		`INSERT INTO dim_exchanges (name, country) VALUES ($1, $2) RETURNING id`,
		name, country,
	).Scan(&id)
	return id, err
}

// cleanupAllSecuritiesWithTicker removes all dim_security rows (and their dependents) for a ticker,
// handling the case where the same ticker appears on multiple exchanges.
func cleanupAllSecuritiesWithTicker(pool *pgxpool.Pool, ticker string) {
	ctx := context.Background()
	pool.Exec(ctx, `DELETE FROM fact_price WHERE security_id IN (SELECT id FROM dim_security WHERE ticker = $1)`, ticker)
	pool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id IN (SELECT id FROM dim_security WHERE ticker = $1)`, ticker)
	pool.Exec(ctx, `DELETE FROM portfolio_membership WHERE security_id IN (SELECT id FROM dim_security WHERE ticker = $1)`, ticker)
	pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
}

// buildImportRequest creates a multipart POST to /admin/import-prices with the given CSV and optional dry_run flag.
func buildImportRequest(t *testing.T, csvContent string, dryRun bool) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, err := w.CreateFormFile("file", "prices.csv")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	if _, err := part.Write([]byte(csvContent)); err != nil {
		t.Fatalf("failed to write CSV content: %v", err)
	}
	if dryRun {
		if err := w.WriteField("dry_run", "true"); err != nil {
			t.Fatalf("failed to write dry_run field: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}
	req, err := http.NewRequest("POST", "/admin/import-prices", &buf)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req
}

// exportCSV calls GET /admin/export-prices with the given query string and returns the response body.
func exportCSV(t *testing.T, router *gin.Engine, query string) *httptest.ResponseRecorder {
	t.Helper()
	url := "/admin/export-prices"
	if query != "" {
		url += "?" + query
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatalf("failed to create export request: %v", err)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// parseCSVBody parses a CSV response body into header + records.
func parseCSVBody(t *testing.T, body string) ([]string, [][]string) {
	t.Helper()
	r := csv.NewReader(strings.NewReader(body))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to parse CSV response: %v", err)
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], records[1:]
}

// countDBPriceRows counts fact_price rows for a security.
func countDBPriceRows(pool *pgxpool.Pool, securityID int64) int {
	var n int
	pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM fact_price WHERE security_id = $1`, securityID).Scan(&n)
	return n
}

// --- Tests ---

func TestExportPrices_Empty(t *testing.T) {
	t.Parallel()
	router := setupExportImportRouter(testPool)
	w := exportCSV(t, router, "ticker=TSTEXPEMPTYTST")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	header, rows := parseCSVBody(t, w.Body.String())
	if len(header) != 10 {
		t.Fatalf("expected 10 header columns, got %v", header)
	}
	if header[0] != "ticker" || header[1] != "exchange" {
		t.Errorf("unexpected header: %v", header)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 data rows for unknown ticker, got %d", len(rows))
	}
}

func TestExportPrices_WithData(t *testing.T) {
	t.Parallel()
	secID, err := createTestStock(testPool, "TSTEXPW1TST", "TST Export Security 1")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTEXPW1TST") })

	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(testPool, secID, start, end, 100.0); err != nil {
		t.Fatalf("setup: %v", err)
	}

	router := setupExportImportRouter(testPool)
	w := exportCSV(t, router, "ticker=TSTEXPW1TST")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	header, rows := parseCSVBody(t, w.Body.String())
	if header[0] != "ticker" || header[1] != "exchange" || header[2] != "date" {
		t.Errorf("unexpected header: %v", header)
	}
	if len(rows) == 0 {
		t.Fatal("expected price rows, got none")
	}
	for _, row := range rows {
		if row[0] != "TSTEXPW1TST" {
			t.Errorf("expected ticker TSTEXPW1TST, got %q", row[0])
		}
		if row[1] == "" {
			t.Errorf("exchange column is empty")
		}
	}
	// Verify rows are ordered by date ascending.
	for i := 1; i < len(rows); i++ {
		if rows[i][2] < rows[i-1][2] {
			t.Errorf("rows not ordered by date: %q before %q", rows[i-1][2], rows[i][2])
		}
	}
}

func TestExportPrices_FilterByTicker(t *testing.T) {
	t.Parallel()
	secID1, err := createTestStock(testPool, "TSTEXPF1TST", "TST Filter Security 1")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	secID2, err := createTestStock(testPool, "TSTEXPF2TST", "TST Filter Security 2")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() {
		cleanupTestSecurity(testPool, "TSTEXPF1TST")
		cleanupTestSecurity(testPool, "TSTEXPF2TST")
	})

	date := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(testPool, secID1, date, date, 50.0); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := insertPriceData(testPool, secID2, date, date, 75.0); err != nil {
		t.Fatalf("setup: %v", err)
	}

	router := setupExportImportRouter(testPool)
	w := exportCSV(t, router, "ticker=TSTEXPF1TST")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	_, rows := parseCSVBody(t, w.Body.String())
	for _, row := range rows {
		if row[0] != "TSTEXPF1TST" {
			t.Errorf("filter failed: got ticker %q", row[0])
		}
	}
}

func TestExportPrices_FilterByDateRange(t *testing.T) {
	t.Parallel()
	secID, err := createTestStock(testPool, "TSTEXPDR1TST", "TST Date Range Security")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTEXPDR1TST") })

	start := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
	end := time.Date(2023, 12, 29, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(testPool, secID, start, end, 120.0); err != nil {
		t.Fatalf("setup: %v", err)
	}

	router := setupExportImportRouter(testPool)
	w := exportCSV(t, router, "ticker=TSTEXPDR1TST&start_date=2023-06-01&end_date=2023-06-30")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	_, rows := parseCSVBody(t, w.Body.String())
	if len(rows) == 0 {
		t.Fatal("expected rows in June 2023, got none")
	}
	for _, row := range rows {
		if row[2] < "2023-06-01" || row[2] > "2023-06-30" {
			t.Errorf("date %q is outside filter range", row[2])
		}
	}
}

func TestExportPrices_InvalidDate(t *testing.T) {
	t.Parallel()
	router := setupExportImportRouter(testPool)
	w := exportCSV(t, router, "start_date=not-a-date")
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid date, got %d", w.Code)
	}
}

func TestImportPrices_RoundTrip(t *testing.T) {
	t.Parallel()
	secID, err := createTestStock(testPool, "TSTIMPRTTST", "TST RoundTrip Security")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTIMPRTTST") })

	start := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 2, 9, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(testPool, secID, start, end, 200.0); err != nil {
		t.Fatalf("setup: %v", err)
	}

	router := setupExportImportRouter(testPool)

	// Export.
	w := exportCSV(t, router, "ticker=TSTIMPRTTST")
	if w.Code != http.StatusOK {
		t.Fatalf("export failed: %d %s", w.Code, w.Body.String())
	}
	csvBody := w.Body.String()
	_, rows := parseCSVBody(t, csvBody)
	originalCount := len(rows)
	if originalCount == 0 {
		t.Fatal("expected price rows from export")
	}

	// Wipe prices and price range.
	ctx := context.Background()
	testPool.Exec(ctx, `DELETE FROM fact_price WHERE security_id = $1`, secID)
	testPool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id = $1`, secID)

	if countDBPriceRows(testPool, secID) != 0 {
		t.Fatal("expected fact_price to be empty after wipe")
	}

	// Import.
	req := buildImportRequest(t, csvBody, false)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusOK {
		t.Fatalf("import failed: %d %s", wr.Code, wr.Body.String())
	}

	var result models.ImportPricesResult
	if err := json.Unmarshal(wr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse import result: %v", err)
	}
	if result.Inserted != originalCount {
		t.Errorf("expected Inserted=%d, got %d", originalCount, result.Inserted)
	}
	if result.Failed != 0 {
		t.Errorf("expected Failed=0, got %d (unknown: %v)", result.Failed, result.UnknownTickers)
	}
	if result.DryRun {
		t.Error("expected DryRun=false")
	}

	// Verify fact_price is repopulated.
	if got := countDBPriceRows(testPool, secID); got != originalCount {
		t.Errorf("expected %d rows in fact_price after import, got %d", originalCount, got)
	}

	// Verify fact_price_range was created.
	var rangeCount int
	testPool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_price_range WHERE security_id = $1`, secID).Scan(&rangeCount)
	if rangeCount != 1 {
		t.Errorf("expected fact_price_range entry after import, got %d", rangeCount)
	}
}

func TestImportPrices_UnknownTicker(t *testing.T) {
	t.Parallel()
	secID, err := createTestStock(testPool, "TSTIMPKNTST", "TST Known Security")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTIMPKNTST") })

	// Export to get the exchange name for the known ticker.
	date := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(testPool, secID, date, date, 50.0); err != nil {
		t.Fatalf("setup: %v", err)
	}
	router := setupExportImportRouter(testPool)
	w := exportCSV(t, router, "ticker=TSTIMPKNTST")
	if w.Code != http.StatusOK {
		t.Fatalf("export failed: %d", w.Code)
	}
	_, knownRows := parseCSVBody(t, w.Body.String())
	if len(knownRows) == 0 {
		t.Fatal("expected at least one exported row")
	}
	exchange := knownRows[0][1]

	// Build CSV with both the known ticker and an unknown one.
	csvContent := "ticker,exchange,date,open,high,low,close,volume\n"
	csvContent += fmt.Sprintf("TSTIMPKNTST,%s,2024-04-01,50,55,49,52,1000000\n", exchange)
	csvContent += "TSTUNKNWNTST,TSTFAKEEXCHANGETST,2024-04-01,10,11,9,10.5,500000\n"

	req := buildImportRequest(t, csvContent, false)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", wr.Code, wr.Body.String())
	}

	var result models.ImportPricesResult
	if err := json.Unmarshal(wr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if len(result.UnknownTickers) == 0 {
		t.Error("expected at least one unknown ticker in response")
	}
	if result.Failed == 0 {
		t.Error("expected Failed > 0 for unknown ticker rows")
	}
	if result.Inserted == 0 {
		t.Error("expected known ticker rows to be inserted")
	}
}

func TestImportPrices_DryRun(t *testing.T) {
	t.Parallel()
	secID, err := createTestStock(testPool, "TSTIMPDRTST", "TST DryRun Security")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTIMPDRTST") })

	// Get exchange name via export (no prices needed, just need the security to exist).
	// We'll hard-code NASDAQ (exchange 2) since createTestStock uses it.
	router := setupExportImportRouter(testPool)
	csvContent := "ticker,exchange,date,open,high,low,close,volume\nTSTIMPDRTST,NASDAQ,2024-05-01,100,105,99,102,1000000\n"

	before := countDBPriceRows(testPool, secID)

	req := buildImportRequest(t, csvContent, true)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", wr.Code, wr.Body.String())
	}

	var result models.ImportPricesResult
	if err := json.Unmarshal(wr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if !result.DryRun {
		t.Error("expected DryRun=true in response")
	}
	if result.Inserted == 0 {
		t.Error("expected Inserted > 0 in dry run report")
	}

	// DB should be unchanged.
	after := countDBPriceRows(testPool, secID)
	if after != before {
		t.Errorf("dry run wrote to DB: before=%d after=%d", before, after)
	}
}

func TestImportPrices_BadCSV(t *testing.T) {
	t.Parallel()
	router := setupExportImportRouter(testPool)
	// Missing required columns (only has ticker).
	csvContent := "ticker\nTSTIMPBADTST\n"
	req := buildImportRequest(t, csvContent, false)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bad CSV, got %d", wr.Code)
	}
}

func TestExportPrices_Foreign(t *testing.T) {
	t.Parallel()
	exchID, err := createTestExchange(testPool, "TSTLONDONTST", "UK")
	if err != nil {
		t.Fatalf("setup exchange: %v", err)
	}
	t.Cleanup(func() { cleanupTestExchange(testPool, "TSTLONDONTST") })

	secID, err := createTestSecurityOnExchange(testPool, "TSTFRGN1TST", "TST Foreign Security", exchID)
	if err != nil {
		t.Fatalf("setup security: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTFRGN1TST") })

	date := time.Date(2024, 6, 3, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(testPool, secID, date, date, 300.0); err != nil {
		t.Fatalf("setup prices: %v", err)
	}

	router := setupExportImportRouter(testPool)
	w := exportCSV(t, router, "ticker=TSTFRGN1TST")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	_, rows := parseCSVBody(t, w.Body.String())
	if len(rows) == 0 {
		t.Fatal("expected price rows for foreign security")
	}
	if rows[0][1] != "TSTLONDONTST" {
		t.Errorf("expected exchange TSTLONDONTST, got %q", rows[0][1])
	}
}

func TestImportPrices_Foreign(t *testing.T) {
	t.Parallel()
	exchID, err := createTestExchange(testPool, "TSTTOKYOTST", "JP")
	if err != nil {
		t.Fatalf("setup exchange: %v", err)
	}
	t.Cleanup(func() { cleanupTestExchange(testPool, "TSTTOKYOTST") })

	// Domestic security on NASDAQ (exchange 2)
	domID, err := createTestStock(testPool, "TSTDOM1TST", "TST Domestic Security")
	if err != nil {
		t.Fatalf("setup domestic: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTDOM1TST") })

	// Foreign security on test exchange
	forID, err := createTestSecurityOnExchange(testPool, "TSTFOR1TST", "TST Foreign Security", exchID)
	if err != nil {
		t.Fatalf("setup foreign: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTFOR1TST") })

	date := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(testPool, domID, date, date, 150.0); err != nil {
		t.Fatalf("setup domestic prices: %v", err)
	}
	if err := insertPriceData(testPool, forID, date, date, 2500.0); err != nil {
		t.Fatalf("setup foreign prices: %v", err)
	}

	router := setupExportImportRouter(testPool)

	// Export both, wipe, re-import.
	w1 := exportCSV(t, router, "ticker=TSTDOM1TST")
	w2 := exportCSV(t, router, "ticker=TSTFOR1TST")
	if w1.Code != http.StatusOK || w2.Code != http.StatusOK {
		t.Fatalf("export failed: %d / %d", w1.Code, w2.Code)
	}

	// Combine into one CSV (drop duplicate header).
	_, domRows := parseCSVBody(t, w1.Body.String())
	_, forRows := parseCSVBody(t, w2.Body.String())
	csvContent := "ticker,exchange,date,open,high,low,close,volume,dividend,split_coefficient\n"
	for _, row := range domRows {
		csvContent += strings.Join(row, ",") + "\n"
	}
	for _, row := range forRows {
		csvContent += strings.Join(row, ",") + "\n"
	}

	ctx := context.Background()
	testPool.Exec(ctx, `DELETE FROM fact_price WHERE security_id IN ($1, $2)`, domID, forID)
	testPool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id IN ($1, $2)`, domID, forID)

	req := buildImportRequest(t, csvContent, false)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusOK {
		t.Fatalf("import failed: %d %s", wr.Code, wr.Body.String())
	}

	var result models.ImportPricesResult
	if err := json.Unmarshal(wr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if result.Inserted != len(domRows)+len(forRows) {
		t.Errorf("expected Inserted=%d, got %d", len(domRows)+len(forRows), result.Inserted)
	}
	if result.Failed != 0 {
		t.Errorf("expected Failed=0, got %d (unknown: %v)", result.Failed, result.UnknownTickers)
	}

	// Both securities should have prices restored.
	if got := countDBPriceRows(testPool, domID); got != len(domRows) {
		t.Errorf("domestic: expected %d rows, got %d", len(domRows), got)
	}
	if got := countDBPriceRows(testPool, forID); got != len(forRows) {
		t.Errorf("foreign: expected %d rows, got %d", len(forRows), got)
	}
}

func TestImportPrices_ForeignOverlap(t *testing.T) {
	t.Parallel()
	// Same ticker on two different exchanges — each must import to the correct security_id.
	exchID, err := createTestExchange(testPool, "TSTHKEXCHTST", "HK")
	if err != nil {
		t.Fatalf("setup exchange: %v", err)
	}
	t.Cleanup(func() { cleanupTestExchange(testPool, "TSTHKEXCHTST") })

	const sharedTicker = "TSTOVLPTST"
	t.Cleanup(func() { cleanupAllSecuritiesWithTicker(testPool, sharedTicker) })

	// Insert same ticker on two exchanges.
	nasdaqID, err := createTestSecurityOnExchange(testPool, sharedTicker, "TST Overlap NASDAQ", 2)
	if err != nil {
		t.Fatalf("setup NASDAQ security: %v", err)
	}
	hkID, err := createTestSecurityOnExchange(testPool, sharedTicker, "TST Overlap HK", exchID)
	if err != nil {
		t.Fatalf("setup HK security: %v", err)
	}

	date := time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(testPool, nasdaqID, date, date, 100.0); err != nil {
		t.Fatalf("setup NASDAQ prices: %v", err)
	}
	if err := insertPriceData(testPool, hkID, date, date, 780.0); err != nil {
		t.Fatalf("setup HK prices: %v", err)
	}

	router := setupExportImportRouter(testPool)

	// Export both via unfiltered export (they share a ticker so export-by-ticker returns both).
	w := exportCSV(t, router, "ticker="+sharedTicker)
	if w.Code != http.StatusOK {
		t.Fatalf("export failed: %d %s", w.Code, w.Body.String())
	}
	_, rows := parseCSVBody(t, w.Body.String())
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (one per exchange), got %d", len(rows))
	}

	// Verify both exchanges are represented.
	exchanges := map[string]bool{}
	for _, row := range rows {
		exchanges[row[1]] = true
	}
	if !exchanges["NASDAQ"] {
		t.Error("expected NASDAQ in export rows")
	}
	if !exchanges["TSTHKEXCHTST"] {
		t.Error("expected TSTHKEXCHTST in export rows")
	}

	// Wipe prices, re-import.
	ctx := context.Background()
	testPool.Exec(ctx, `DELETE FROM fact_price WHERE security_id IN ($1, $2)`, nasdaqID, hkID)
	testPool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id IN ($1, $2)`, nasdaqID, hkID)

	req := buildImportRequest(t, w.Body.String(), false)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusOK {
		t.Fatalf("import failed: %d %s", wr.Code, wr.Body.String())
	}

	var result models.ImportPricesResult
	if err := json.Unmarshal(wr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if result.Inserted != 2 {
		t.Errorf("expected Inserted=2, got %d", result.Inserted)
	}
	if result.Failed != 0 {
		t.Errorf("expected Failed=0, got %d (unknown: %v)", result.Failed, result.UnknownTickers)
	}

	// Each security should have exactly 1 price row pointing to the right security_id.
	if got := countDBPriceRows(testPool, nasdaqID); got != 1 {
		t.Errorf("NASDAQ security: expected 1 row, got %d", got)
	}
	if got := countDBPriceRows(testPool, hkID); got != 1 {
		t.Errorf("HK security: expected 1 row, got %d", got)
	}
}

// countDBEventRows counts fact_event rows for a security.
func countDBEventRows(pool *pgxpool.Pool, securityID int64) int {
	var n int
	pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM fact_event WHERE security_id = $1`, securityID).Scan(&n)
	return n
}

// getEventRow fetches dividend and split_coefficient for a specific security+date.
// Returns found=false if no row exists.
func getEventRow(pool *pgxpool.Pool, securityID int64, date time.Time) (dividend float64, splitCoeff float64, found bool) {
	err := pool.QueryRow(context.Background(),
		`SELECT dividend, split_coefficient FROM fact_event WHERE security_id = $1 AND date = $2`,
		securityID, date,
	).Scan(&dividend, &splitCoeff)
	return dividend, splitCoeff, err == nil
}

// TestExportImport_RoundTrip_WithEvents verifies that dividend and split events survive
// a full export → wipe → import cycle.
func TestExportImport_RoundTrip_WithEvents(t *testing.T) {
	t.Parallel()
	secID, err := createTestStock(testPool, "TSTRTEVTTST", "TST RoundTrip Events Security")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTRTEVTTST") })

	priceStart := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	priceEnd := time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC)
	if err := insertPriceData(testPool, secID, priceStart, priceEnd, 100.0); err != nil {
		t.Fatalf("setup prices: %v", err)
	}

	divDate := time.Date(2024, 3, 4, 0, 0, 0, 0, time.UTC)   // Monday
	splitDate := time.Date(2024, 3, 6, 0, 0, 0, 0, time.UTC) // Wednesday
	if err := insertDividendEvent(testPool, secID, divDate, 0.42); err != nil {
		t.Fatalf("setup dividend: %v", err)
	}
	if err := insertSplitEvent(testPool, secID, splitDate, 4.0); err != nil {
		t.Fatalf("setup split: %v", err)
	}

	router := setupExportImportRouter(testPool)

	// Export.
	w := exportCSV(t, router, "ticker=TSTRTEVTTST")
	if w.Code != http.StatusOK {
		t.Fatalf("export failed: %d %s", w.Code, w.Body.String())
	}
	csvBody := w.Body.String()
	_, rows := parseCSVBody(t, csvBody)
	if len(rows) == 0 {
		t.Fatal("expected rows from export")
	}

	// Verify the exported CSV has the event columns.
	header, _ := parseCSVBody(t, csvBody)
	if len(header) != 10 {
		t.Fatalf("expected 10 header columns, got %d: %v", len(header), header)
	}

	// Wipe fact_price, fact_event, fact_price_range.
	ctx := context.Background()
	testPool.Exec(ctx, `DELETE FROM fact_price WHERE security_id = $1`, secID)
	testPool.Exec(ctx, `DELETE FROM fact_event WHERE security_id = $1`, secID)
	testPool.Exec(ctx, `DELETE FROM fact_price_range WHERE security_id = $1`, secID)

	// Re-import.
	req := buildImportRequest(t, csvBody, false)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusOK {
		t.Fatalf("import failed: %d %s", wr.Code, wr.Body.String())
	}

	var result models.ImportPricesResult
	if err := json.Unmarshal(wr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse import result: %v", err)
	}
	if result.Failed != 0 {
		t.Errorf("expected Failed=0, got %d (unknown: %v)", result.Failed, result.UnknownTickers)
	}

	// Prices should be restored.
	if got := countDBPriceRows(testPool, secID); got != len(rows) {
		t.Errorf("expected %d fact_price rows after re-import, got %d", len(rows), got)
	}

	// Events should be restored with correct values.
	div, sc, found := getEventRow(testPool, secID, divDate)
	if !found {
		t.Errorf("dividend event not restored for %s", divDate.Format("2006-01-02"))
	} else {
		if div != 0.42 {
			t.Errorf("dividend: expected 0.42, got %v", div)
		}
		if sc != 1.0 {
			t.Errorf("split_coefficient on dividend day: expected 1.0, got %v", sc)
		}
	}

	splitDiv, splitSC, found := getEventRow(testPool, secID, splitDate)
	if !found {
		t.Errorf("split event not restored for %s", splitDate.Format("2006-01-02"))
	} else {
		if splitDiv != 0 {
			t.Errorf("dividend on split day: expected 0, got %v", splitDiv)
		}
		if splitSC != 4.0 {
			t.Errorf("split_coefficient: expected 4.0, got %v", splitSC)
		}
	}

	// Trivial-event rows (dividend=0, split=1.0) must NOT create fact_event rows.
	if got := countDBEventRows(testPool, secID); got != 2 {
		t.Errorf("expected exactly 2 fact_event rows (div + split), got %d", got)
	}
}

// TestImportPrices_DividendOnly imports a CSV that contains a dividend column
// and verifies fact_event is populated correctly.
func TestImportPrices_DividendOnly(t *testing.T) {
	t.Parallel()
	secID, err := createTestStock(testPool, "TSTDIVONLYTST", "TST Dividend Only Security")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTDIVONLYTST") })

	csvContent := "ticker,exchange,date,open,high,low,close,volume,dividend,split_coefficient\n" +
		"TSTDIVONLYTST,NASDAQ,2024-09-05,50,55,49,52,800000,0.35,1.0\n" +
		"TSTDIVONLYTST,NASDAQ,2024-09-06,52,56,51,54,700000,0,1.0\n"

	router := setupExportImportRouter(testPool)
	req := buildImportRequest(t, csvContent, false)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusOK {
		t.Fatalf("import failed: %d %s", wr.Code, wr.Body.String())
	}

	var result models.ImportPricesResult
	if err := json.Unmarshal(wr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if result.Inserted != 2 {
		t.Errorf("expected Inserted=2, got %d", result.Inserted)
	}

	divDate := time.Date(2024, 9, 5, 0, 0, 0, 0, time.UTC)
	div, sc, found := getEventRow(testPool, secID, divDate)
	if !found {
		t.Fatal("expected fact_event row for dividend date")
	}
	if div != 0.35 {
		t.Errorf("dividend: expected 0.35, got %v", div)
	}
	if sc != 1.0 {
		t.Errorf("split_coefficient: expected 1.0, got %v", sc)
	}

	// Row with dividend=0 must not create a fact_event row.
	noEventDate := time.Date(2024, 9, 6, 0, 0, 0, 0, time.UTC)
	if _, _, found := getEventRow(testPool, secID, noEventDate); found {
		t.Error("expected no fact_event row for trivial row (dividend=0, split=1.0)")
	}

	if got := countDBEventRows(testPool, secID); got != 1 {
		t.Errorf("expected exactly 1 fact_event row, got %d", got)
	}
}

// TestImportPrices_MultipleSplits imports a CSV with multiple split events and
// verifies each split coefficient is stored in fact_event.
func TestImportPrices_MultipleSplits(t *testing.T) {
	t.Parallel()
	secID, err := createTestStock(testPool, "TSTSPLITMTST", "TST Multi-Split Security")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTSPLITMTST") })

	csvContent := "ticker,exchange,date,open,high,low,close,volume,dividend,split_coefficient\n" +
		"TSTSPLITMTST,NASDAQ,2024-10-01,100,105,99,102,1000000,0,1.0\n" +  // normal — no event
		"TSTSPLITMTST,NASDAQ,2024-10-02,50,53,49,51,2000000,0,4.0\n" +    // 4-for-1 split
		"TSTSPLITMTST,NASDAQ,2024-10-03,100,104,99,103,1500000,0,1.0\n" + // normal — no event
		"TSTSPLITMTST,NASDAQ,2024-10-04,50,52,49,51,1800000,0,2.0\n"     // 2-for-1 split

	router := setupExportImportRouter(testPool)
	req := buildImportRequest(t, csvContent, false)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusOK {
		t.Fatalf("import failed: %d %s", wr.Code, wr.Body.String())
	}

	var result models.ImportPricesResult
	if err := json.Unmarshal(wr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if result.Inserted != 4 {
		t.Errorf("expected Inserted=4, got %d", result.Inserted)
	}

	split1Date := time.Date(2024, 10, 2, 0, 0, 0, 0, time.UTC)
	split2Date := time.Date(2024, 10, 4, 0, 0, 0, 0, time.UTC)
	normalDate := time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC)

	_, sc1, found := getEventRow(testPool, secID, split1Date)
	if !found {
		t.Fatal("expected fact_event row for first split")
	}
	if sc1 != 4.0 {
		t.Errorf("first split: expected coefficient 4.0, got %v", sc1)
	}

	_, sc2, found := getEventRow(testPool, secID, split2Date)
	if !found {
		t.Fatal("expected fact_event row for second split")
	}
	if sc2 != 2.0 {
		t.Errorf("second split: expected coefficient 2.0, got %v", sc2)
	}

	if _, _, found := getEventRow(testPool, secID, normalDate); found {
		t.Error("expected no fact_event row for normal (no-event) row")
	}

	if got := countDBEventRows(testPool, secID); got != 2 {
		t.Errorf("expected exactly 2 fact_event rows, got %d", got)
	}
}

// TestImportPrices_SplitAndDividendMixed imports a CSV that mixes:
//   - a dividend-only event
//   - a split-only event
//   - a row with both split and dividend on the same day
//   - a normal row with no events
//
// It verifies all three events are stored and the normal row produces no event.
func TestImportPrices_SplitAndDividendMixed(t *testing.T) {
	t.Parallel()
	secID, err := createTestStock(testPool, "TSTMIXEDTST", "TST Mixed Events Security")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { cleanupTestSecurity(testPool, "TSTMIXEDTST") })

	csvContent := "ticker,exchange,date,open,high,low,close,volume,dividend,split_coefficient\n" +
		"TSTMIXEDTST,NASDAQ,2024-11-01,100,105,99,102,1000000,0.50,1.0\n" +  // dividend only
		"TSTMIXEDTST,NASDAQ,2024-11-04,50,53,49,51,2000000,0,3.0\n" +        // split only
		"TSTMIXEDTST,NASDAQ,2024-11-05,102,106,101,105,1200000,0.25,2.0\n" + // both on same day
		"TSTMIXEDTST,NASDAQ,2024-11-06,104,108,103,107,900000,0,1.0\n"       // no event

	router := setupExportImportRouter(testPool)
	req := buildImportRequest(t, csvContent, false)
	wr := httptest.NewRecorder()
	router.ServeHTTP(wr, req)
	if wr.Code != http.StatusOK {
		t.Fatalf("import failed: %d %s", wr.Code, wr.Body.String())
	}

	var result models.ImportPricesResult
	if err := json.Unmarshal(wr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse result: %v", err)
	}
	if result.Inserted != 4 {
		t.Errorf("expected Inserted=4, got %d", result.Inserted)
	}
	if result.Failed != 0 {
		t.Errorf("expected Failed=0, got %d", result.Failed)
	}

	divOnlyDate := time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC)
	splitOnlyDate := time.Date(2024, 11, 4, 0, 0, 0, 0, time.UTC)
	bothDate := time.Date(2024, 11, 5, 0, 0, 0, 0, time.UTC)
	noneDate := time.Date(2024, 11, 6, 0, 0, 0, 0, time.UTC)

	div, sc, found := getEventRow(testPool, secID, divOnlyDate)
	if !found {
		t.Fatal("expected fact_event row for dividend-only date")
	}
	if div != 0.50 {
		t.Errorf("dividend-only: expected dividend=0.50, got %v", div)
	}
	if sc != 1.0 {
		t.Errorf("dividend-only: expected split=1.0, got %v", sc)
	}

	div, sc, found = getEventRow(testPool, secID, splitOnlyDate)
	if !found {
		t.Fatal("expected fact_event row for split-only date")
	}
	if div != 0 {
		t.Errorf("split-only: expected dividend=0, got %v", div)
	}
	if sc != 3.0 {
		t.Errorf("split-only: expected split=3.0, got %v", sc)
	}

	div, sc, found = getEventRow(testPool, secID, bothDate)
	if !found {
		t.Fatal("expected fact_event row for same-day split+dividend")
	}
	if div != 0.25 {
		t.Errorf("same-day: expected dividend=0.25, got %v", div)
	}
	if sc != 2.0 {
		t.Errorf("same-day: expected split=2.0, got %v", sc)
	}

	if _, _, found := getEventRow(testPool, secID, noneDate); found {
		t.Error("expected no fact_event row for the no-event row")
	}

	if got := countDBEventRows(testPool, secID); got != 3 {
		t.Errorf("expected exactly 3 fact_event rows, got %d", got)
	}
}

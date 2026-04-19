package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupAdminSyncRouter creates a router with the sync-from-provider endpoint wired to an EODHD client.
func setupAdminSyncRouter(pool *pgxpool.Pool, eodhdClient *eodhd.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)

	securityRepo := repository.NewSecurityRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)

	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, priceRepo, repository.NewFundamentalsRepository(testPool), eodhdClient, 10)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{
		Price:    eodhd.NewClient("test-key", "http://localhost:9999"),
		Treasury: fred.NewClient("test-key", "http://localhost:9999"),
	})
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo, exchangeRepo, priceRepo)

	router := gin.New()
	admin := router.Group("/admin/securities")
	{
		admin.POST("/sync-from-provider", adminHandler.SyncSecuritiesFromProvider)
	}
	return router
}

// newEODHDMockServer creates an httptest.Server that serves a fixed exchange list and per-exchange
// symbol lists. exchangeListJSON is the raw JSON for GET /exchanges-list/. symbolsByCode maps
// exchange code → raw JSON for GET /exchange-symbol-list/{code}.
func newEODHDMockServer(exchangeListJSON string, symbolsByCode map[string]string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.HasPrefix(path, "/exchanges-list/") || path == "/exchanges-list":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(exchangeListJSON)) // #nosec G104
		case strings.HasPrefix(path, "/exchange-symbol-list/"):
			code := strings.TrimPrefix(path, "/exchange-symbol-list/")
			lookupKey := code
			if r.URL.Query().Get("delisted") == "1" {
				lookupKey = code + "?delisted=1"
			}
			if body, ok := symbolsByCode[lookupKey]; ok {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(body)) // #nosec G104
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("[]")) // #nosec G104
			}
		default:
			http.NotFound(w, r)
		}
	}))
}

// TestSyncSecuritiesBasic verifies that symbols from EODHD are inserted into dim_security.
func TestSyncSecuritiesBasic(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	cleanupTestSecurities(pool, []string{"TSTSYNC1", "TSTSYNC2", "TSTSYNC3"})
	cleanupTestExchange(pool, "TSTEXCH")

	exchangeList := `[{"Code":"TSTEXCH","Name":"Test Exchange","Country":"TESTLAND","Currency":"USD","CountryISO2":"TT","CountryISO3":"TST"}]`
	symbols := map[string]string{
		"TSTEXCH": `[
			{"Code":"TSTSYNC1","Name":"Test Security One","Country":"TESTLAND","Exchange":"TSTEXCH","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTSYNC2","Name":"Test Security Two","Country":"TESTLAND","Exchange":"TSTEXCH","Currency":"USD","Type":"ETF","Isin":""},
			{"Code":"TSTSYNC3","Name":"Test Security Three","Country":"TESTLAND","Exchange":"TSTEXCH","Currency":"USD","Type":"Common Stock","Isin":""}
		]`,
	}

	mockServer := newEODHDMockServer(exchangeList, symbols)
	defer mockServer.Close()

	router := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mockServer.URL))

	req, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if result.SecuritiesInserted != 3 {
		t.Errorf("Expected 3 securities inserted, got %d; response: %s", result.SecuritiesInserted, w.Body.String())
	}
	if result.SecuritiesSkipped != 0 {
		t.Errorf("Expected 0 skipped, got %d", result.SecuritiesSkipped)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM dim_security WHERE ticker IN ('TSTSYNC1','TSTSYNC2','TSTSYNC3')`).Scan(&count); err != nil {
		t.Fatalf("DB query failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 rows in dim_security, got %d", count)
	}

	cleanupTestSecurities(pool, []string{"TSTSYNC1", "TSTSYNC2", "TSTSYNC3"})
	cleanupTestExchange(pool, "TSTEXCH")
}

// TestSyncSecuritiesIdempotency verifies that re-running sync skips already-inserted securities.
func TestSyncSecuritiesIdempotency(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	cleanupTestSecurities(pool, []string{"TSTIDEM1", "TSTIDEM2", "TSTIDEM3", "TSTNEW1", "TSTNEW2"})
	cleanupTestExchange(pool, "TSTEXCH2")

	exchangeList := `[{"Code":"TSTEXCH2","Name":"Test Exchange 2","Country":"TESTLAND","Currency":"USD","CountryISO2":"TT","CountryISO3":"TST"}]`

	// First sync: 3 securities
	symbols1 := map[string]string{
		"TSTEXCH2": `[
			{"Code":"TSTIDEM1","Name":"Idem One","Country":"TESTLAND","Exchange":"TSTEXCH2","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTIDEM2","Name":"Idem Two","Country":"TESTLAND","Exchange":"TSTEXCH2","Currency":"USD","Type":"ETF","Isin":""},
			{"Code":"TSTIDEM3","Name":"Idem Three","Country":"TESTLAND","Exchange":"TSTEXCH2","Currency":"USD","Type":"Common Stock","Isin":""}
		]`,
	}
	mock1 := newEODHDMockServer(exchangeList, symbols1)
	router1 := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mock1.URL))

	req1, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider", nil)
	w1 := httptest.NewRecorder()
	router1.ServeHTTP(w1, req1)
	mock1.Close()

	if w1.Code != http.StatusOK {
		t.Fatalf("First sync failed: %d - %s", w1.Code, w1.Body.String())
	}
	var result1 services.SyncSecuritiesResult
	json.Unmarshal(w1.Body.Bytes(), &result1) // #nosec G104
	if result1.SecuritiesInserted != 3 {
		t.Errorf("First sync: expected 3 inserted, got %d", result1.SecuritiesInserted)
	}

	// Second sync: same 3 + 2 new ones
	symbols2 := map[string]string{
		"TSTEXCH2": `[
			{"Code":"TSTIDEM1","Name":"Idem One","Country":"TESTLAND","Exchange":"TSTEXCH2","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTIDEM2","Name":"Idem Two","Country":"TESTLAND","Exchange":"TSTEXCH2","Currency":"USD","Type":"ETF","Isin":""},
			{"Code":"TSTIDEM3","Name":"Idem Three","Country":"TESTLAND","Exchange":"TSTEXCH2","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTNEW1","Name":"New One","Country":"TESTLAND","Exchange":"TSTEXCH2","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTNEW2","Name":"New Two","Country":"TESTLAND","Exchange":"TSTEXCH2","Currency":"USD","Type":"ETF","Isin":""}
		]`,
	}
	mock2 := newEODHDMockServer(exchangeList, symbols2)
	defer mock2.Close()

	router2 := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mock2.URL))
	req2, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider", nil)
	w2 := httptest.NewRecorder()
	router2.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Second sync failed: %d - %s", w2.Code, w2.Body.String())
	}
	var result2 services.SyncSecuritiesResult
	json.Unmarshal(w2.Body.Bytes(), &result2) // #nosec G104

	if result2.SecuritiesInserted != 2 {
		t.Errorf("Second sync: expected 2 inserted, got %d", result2.SecuritiesInserted)
	}
	if result2.SecuritiesSkipped != 3 {
		t.Errorf("Second sync: expected 3 skipped, got %d", result2.SecuritiesSkipped)
	}

	cleanupTestSecurities(pool, []string{"TSTIDEM1", "TSTIDEM2", "TSTIDEM3", "TSTNEW1", "TSTNEW2"})
	cleanupTestExchange(pool, "TSTEXCH2")
}

// TestSyncSecuritiesPerSymbolExchangeRouting is a regression test for the bug where
// processExchange used job.id (the aggregate exchange ID) for every symbol instead of
// the per-symbol Exchange field. EODHD aggregate exchanges like "US" return symbols whose
// Exchange field points to the actual exchange (NYSE, NASDAQ, etc.). Using job.id caused all
// such securities to be keyed to the wrong exchange, so they were never recognised as existing
// and were always treated as new.
func TestSyncSecuritiesPerSymbolExchangeRouting(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	tickers := []string{"TSTPE1", "TSTPE2", "TSTPE3"}
	cleanupTestSecurities(pool, tickers)
	cleanupTestExchange(pool, "TSTAGGREG")
	cleanupTestExchange(pool, "TSTSUBA")
	cleanupTestExchange(pool, "TSTSUBB")

	// Exchange list: one aggregate and two sub-exchanges.
	exchangeList := `[
		{"Code":"TSTAGGREG","Name":"Aggregate Exchange","Country":"TESTLAND","Currency":"USD","CountryISO2":"TT","CountryISO3":"TST"},
		{"Code":"TSTSUBA",  "Name":"Sub Exchange A",    "Country":"TESTLAND","Currency":"USD","CountryISO2":"TT","CountryISO3":"TST"},
		{"Code":"TSTSUBB",  "Name":"Sub Exchange B",    "Country":"TESTLAND","Currency":"USD","CountryISO2":"TT","CountryISO3":"TST"}
	]`

	// Symbols come back under TSTAGGREG but their Exchange field points to the sub-exchanges.
	// This mirrors how EODHD's /exchange-symbol-list/US returns NYSE/NASDAQ symbols.
	symbols := map[string]string{
		"TSTAGGREG": `[
			{"Code":"TSTPE1","Name":"Per-Exchange One","Country":"TESTLAND","Exchange":"TSTSUBA","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTPE2","Name":"Per-Exchange Two","Country":"TESTLAND","Exchange":"TSTSUBB","Currency":"USD","Type":"Common Stock","Isin":""}
		]`,
		"TSTSUBA": `[]`,
		"TSTSUBB": `[]`,
	}

	// ── First sync (live): inserts both securities ────────────────────────────
	mock1 := newEODHDMockServer(exchangeList, symbols)
	router1 := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mock1.URL))

	req1, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider", nil)
	w1 := httptest.NewRecorder()
	router1.ServeHTTP(w1, req1)
	mock1.Close()

	if w1.Code != http.StatusOK {
		t.Fatalf("First sync failed: %d - %s", w1.Code, w1.Body.String())
	}
	var result1 services.SyncSecuritiesResult
	json.Unmarshal(w1.Body.Bytes(), &result1) // #nosec G104
	if result1.SecuritiesInserted != 2 {
		t.Fatalf("First sync: expected 2 inserted, got %d; response: %s", result1.SecuritiesInserted, w1.Body.String())
	}

	// Verify TSTPE1 is stored under TSTSUBA, not TSTAGGREG.
	var exchangeName string
	if err := pool.QueryRow(ctx, `
		SELECT de.name FROM dim_security ds
		JOIN dim_exchanges de ON de.id = ds.exchange
		WHERE ds.ticker = 'TSTPE1'
	`).Scan(&exchangeName); err != nil {
		t.Fatalf("TSTPE1 not found in DB: %v", err)
	}
	if exchangeName != "TSTSUBA" {
		t.Errorf("TSTPE1 should be stored under TSTSUBA, got %q — per-symbol exchange routing broken", exchangeName)
	}

	// ── Second sync (live): adds one new security, skips two existing ─────────
	symbols2 := map[string]string{
		"TSTAGGREG": `[
			{"Code":"TSTPE1","Name":"Per-Exchange One","Country":"TESTLAND","Exchange":"TSTSUBA","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTPE2","Name":"Per-Exchange Two","Country":"TESTLAND","Exchange":"TSTSUBB","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTPE3","Name":"Per-Exchange Three","Country":"TESTLAND","Exchange":"TSTSUBA","Currency":"USD","Type":"ETF","Isin":""}
		]`,
		"TSTSUBA": `[]`,
		"TSTSUBB": `[]`,
	}

	mock2 := newEODHDMockServer(exchangeList, symbols2)
	defer mock2.Close()
	router2 := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mock2.URL))

	req2, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider", nil)
	w2 := httptest.NewRecorder()
	router2.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Second sync failed: %d - %s", w2.Code, w2.Body.String())
	}
	var result2 services.SyncSecuritiesResult
	json.Unmarshal(w2.Body.Bytes(), &result2) // #nosec G104

	// If the bug were present, processExchange would key all symbols to the aggregate
	// exchange ID (TSTAGGREG), so TSTPE1 and TSTPE2 would appear as new → 3 inserted, 0 skipped.
	if result2.SecuritiesInserted != 1 {
		t.Errorf("Second sync: expected 1 inserted (TSTPE3 only), got %d — per-symbol exchange routing broken", result2.SecuritiesInserted)
	}
	if result2.SecuritiesSkipped != 2 {
		t.Errorf("Second sync: expected 2 skipped (TSTPE1+TSTPE2 already exist), got %d", result2.SecuritiesSkipped)
	}

	cleanupTestSecurities(pool, tickers)
	cleanupTestExchange(pool, "TSTAGGREG")
	cleanupTestExchange(pool, "TSTSUBA")
	cleanupTestExchange(pool, "TSTSUBB")
}

// TestSyncSecuritiesNewExchange verifies that exchanges absent from dim_exchanges are created,
// using the country value provided by EODHD.
func TestSyncSecuritiesNewExchange(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	cleanupTestSecurities(pool, []string{"TSTNEWEX1"})
	cleanupTestExchange(pool, "TSTNEWEXCH")

	exchangeList := `[{"Code":"TSTNEWEXCH","Name":"Brand New Exchange","Country":"NEWCOUNTRY","Currency":"XYZ","CountryISO2":"NC","CountryISO3":"NCO"}]`
	symbols := map[string]string{
		"TSTNEWEXCH": `[{"Code":"TSTNEWEX1","Name":"New Exch Security","Country":"NEWCOUNTRY","Exchange":"TSTNEWEXCH","Currency":"XYZ","Type":"Common Stock","Isin":""}]`,
	}

	mock := newEODHDMockServer(exchangeList, symbols)
	defer mock.Close()

	router := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mock.URL))
	req, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	json.Unmarshal(w.Body.Bytes(), &result) // #nosec G104

	if len(result.ExchangesCreated) != 1 || result.ExchangesCreated[0] != "TSTNEWEXCH" {
		t.Errorf("Expected ExchangesCreated=[TSTNEWEXCH], got %v", result.ExchangesCreated)
	}

	// Verify exchange was created with the country from EODHD
	var country string
	if err := pool.QueryRow(ctx, `SELECT country FROM dim_exchanges WHERE name = $1`, "TSTNEWEXCH").Scan(&country); err != nil {
		t.Fatalf("Exchange not found in DB: %v", err)
	}
	if country != "NEWCOUNTRY" {
		t.Errorf("Expected country 'NEWCOUNTRY', got '%s'", country)
	}

	cleanupTestSecurities(pool, []string{"TSTNEWEX1"})
	cleanupTestExchange(pool, "TSTNEWEXCH")
}

// TestSyncSecuritiesSkipsFilteredExchanges verifies that EUFUND, FOREX, CC, and MONEY
// exchanges are not fetched or inserted.
func TestSyncSecuritiesSkipsFilteredExchanges(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	cleanupTestSecurities(pool, []string{"TSTGOOD1", "TSTFUND1", "TSTFX1", "TSTCC1", "TSTMONEY1"})
	cleanupTestExchange(pool, "TSTOK")

	exchangeList := `[
		{"Code":"TSTOK","Name":"OK Exchange","Country":"TESTLAND","Currency":"USD","CountryISO2":"TT","CountryISO3":"TST"},
		{"Code":"EUFUND","Name":"Europe Fund","Country":"Europe","Currency":"EUR","CountryISO2":"EU","CountryISO3":"EUR"},
		{"Code":"FOREX","Name":"Forex","Country":"","Currency":"","CountryISO2":"","CountryISO3":""},
		{"Code":"CC","Name":"Crypto","Country":"","Currency":"","CountryISO2":"","CountryISO3":""},
		{"Code":"MONEY","Name":"Money Market","Country":"","Currency":"","CountryISO2":"","CountryISO3":""}
	]`
	symbols := map[string]string{
		"TSTOK":   `[{"Code":"TSTGOOD1","Name":"Good Security","Country":"TESTLAND","Exchange":"TSTOK","Currency":"USD","Type":"Common Stock","Isin":""}]`,
		"EUFUND":  `[{"Code":"TSTFUND1","Name":"EU Fund","Country":"Europe","Exchange":"EUFUND","Currency":"EUR","Type":"Fund","Isin":""}]`,
		"FOREX":   `[{"Code":"TSTFX1","Name":"FX Pair","Country":"","Exchange":"FOREX","Currency":"","Type":"Currency","Isin":""}]`,
		"CC":      `[{"Code":"TSTCC1","Name":"Crypto","Country":"","Exchange":"CC","Currency":"","Type":"Common Stock","Isin":""}]`,
		"MONEY":   `[{"Code":"TSTMONEY1","Name":"Money","Country":"","Exchange":"MONEY","Currency":"","Type":"Fund","Isin":""}]`,
	}

	mock := newEODHDMockServer(exchangeList, symbols)
	defer mock.Close()

	router := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mock.URL))
	req, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	json.Unmarshal(w.Body.Bytes(), &result) // #nosec G104

	// Only the TSTOK security should be inserted; filtered exchanges produce no inserts
	if result.SecuritiesInserted != 1 {
		t.Errorf("Expected 1 inserted (from TSTOK only), got %d; response: %s", result.SecuritiesInserted, w.Body.String())
	}

	cleanupTestSecurities(pool, []string{"TSTGOOD1", "TSTFUND1", "TSTFX1", "TSTCC1", "TSTMONEY1"})
	cleanupTestExchange(pool, "TSTOK")
}

// TestSyncSecuritiesUnknownAssetType verifies that symbols with unrecognised types are counted
// in SkippedBadType and not inserted.
func TestSyncSecuritiesUnknownAssetType(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	cleanupTestSecurities(pool, []string{"TSTGOODTP", "TSTBADTP"})
	cleanupTestExchange(pool, "TSTEXCH3")

	exchangeList := `[{"Code":"TSTEXCH3","Name":"Test Exchange 3","Country":"TESTLAND","Currency":"USD","CountryISO2":"TT","CountryISO3":"TST"}]`
	symbols := map[string]string{
		"TSTEXCH3": `[
			{"Code":"TSTGOODTP","Name":"Good Type","Country":"TESTLAND","Exchange":"TSTEXCH3","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTBADTP","Name":"Bad Type","Country":"TESTLAND","Exchange":"TSTEXCH3","Currency":"USD","Type":"UNKNOWN_XYZTYPE","Isin":""}
		]`,
	}

	mock := newEODHDMockServer(exchangeList, symbols)
	defer mock.Close()

	router := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mock.URL))
	req, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	json.Unmarshal(w.Body.Bytes(), &result) // #nosec G104

	if result.SecuritiesInserted != 1 {
		t.Errorf("Expected 1 inserted, got %d", result.SecuritiesInserted)
	}
	if result.SkippedBadType != 1 {
		t.Errorf("Expected SkippedBadType=1, got %d; response: %s", result.SkippedBadType, w.Body.String())
	}

	cleanupTestSecurities(pool, []string{"TSTGOODTP", "TSTBADTP"})
	cleanupTestExchange(pool, "TSTEXCH3")
}

// TestSyncSecuritiesDryRun verifies that dry-run counts new vs existing securities
// without making any DB writes.
func TestSyncSecuritiesDryRun(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	cleanupTestSecurities(pool, []string{"TSTDR1", "TSTDR2", "TSTDR3"})
	cleanupTestExchange(pool, "TSTDREXCH")

	exchangeList := `[{"Code":"TSTDREXCH","Name":"Dry Run Exchange","Country":"DRYLAND","Currency":"USD","CountryISO2":"DR","CountryISO3":"DRY"}]`
	symbols := map[string]string{
		"TSTDREXCH": `[
			{"Code":"TSTDR1","Name":"Dry One","Country":"DRYLAND","Exchange":"TSTDREXCH","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTDR2","Name":"Dry Two","Country":"DRYLAND","Exchange":"TSTDREXCH","Currency":"USD","Type":"ETF","Isin":""},
			{"Code":"TSTDR3","Name":"Dry Three","Country":"DRYLAND","Exchange":"TSTDREXCH","Currency":"USD","Type":"Common Stock","Isin":""}
		]`,
	}

	mock := newEODHDMockServer(exchangeList, symbols)
	defer mock.Close()

	router := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mock.URL))

	// Dry-run: nothing should be written to DB.
	req, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider?type=dryrun", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if !result.DryRun {
		t.Errorf("Expected DryRun=true")
	}
	// All 3 are new; the exchange is also new (fake negative ID).
	if result.SecuritiesInserted != 3 {
		t.Errorf("Expected 3 would-insert, got %d; response: %s", result.SecuritiesInserted, w.Body.String())
	}
	if result.SecuritiesSkipped != 0 {
		t.Errorf("Expected 0 would-skip, got %d", result.SecuritiesSkipped)
	}
	if len(result.ExchangesCreated) != 1 || result.ExchangesCreated[0] != "TSTDREXCH" {
		t.Errorf("Expected ExchangesCreated=[TSTDREXCH], got %v", result.ExchangesCreated)
	}
	// Diagnostic fields: EODHD returned 3 raw symbols; DB has none of these tickers yet.
	if result.EODHDFetched != 3 {
		t.Errorf("Expected EODHDFetched=3, got %d", result.EODHDFetched)
	}
	// MissingSecurities = DatabaseSecurities - SecuritiesSkipped.
	// DatabaseSecurities may be >0 from production data; just verify the arithmetic holds.
	if result.MissingSecurities != result.DatabaseSecurities-result.SecuritiesSkipped {
		t.Errorf("MissingSecurities arithmetic wrong: %d != %d - %d",
			result.MissingSecurities, result.DatabaseSecurities, result.SecuritiesSkipped)
	}

	// Confirm nothing was written.
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM dim_security WHERE ticker IN ('TSTDR1','TSTDR2','TSTDR3')`).Scan(&count); err != nil {
		t.Fatalf("DB query failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Dry-run should not write to DB, but found %d rows", count)
	}
	var exCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM dim_exchanges WHERE name = 'TSTDREXCH'`).Scan(&exCount); err != nil {
		t.Fatalf("DB query failed: %v", err)
	}
	if exCount != 0 {
		t.Errorf("Dry-run should not create exchange, but found %d rows", exCount)
	}
}

// TestSyncSecuritiesDelistedUS verifies that the second (delisted) pass for the US virtual
// exchange inserts new delisted securities with delisted=true, and skips tickers already
// inserted by the live pass (ON CONFLICT DO NOTHING).
func TestSyncSecuritiesDelistedUS(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	allTickers := []string{"TSTLVUS1", "TSTLVUS2", "TSTDLUS1"}
	cleanupTestSecurities(pool, allTickers)
	cleanupTestExchange(pool, "TSTNASDQ")
	t.Cleanup(func() {
		cleanupTestSecurities(pool, allTickers)
		cleanupTestExchange(pool, "TSTNASDQ")
		// "US" is a real production exchange record — never deleted by tests.
	})

	// Exchange list: US virtual + one sub-exchange (mirrors real EODHD structure).
	exchangeList := `[
		{"Code":"TSTUSVIRT","Name":"TSTUSVIRT","Country":"USA","Currency":"USD","CountryISO2":"US","CountryISO3":"USA"},
		{"Code":"TSTNASDQ", "Name":"TSTNASDQ", "Country":"USA","Currency":"USD","CountryISO2":"US","CountryISO3":"USA"}
	]`

	symbols := map[string]string{
		// Live pass: 2 active symbols under the virtual exchange, routed to the sub-exchange.
		"TSTUSVIRT": `[
			{"Code":"TSTLVUS1","Name":"Live US One",  "Country":"USA","Exchange":"TSTNASDQ","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTLVUS2","Name":"Live US Two",  "Country":"USA","Exchange":"TSTNASDQ","Currency":"USD","Type":"ETF","Isin":""}
		]`,
		// Delisted pass: 1 new delisted + 1 conflict with a live ticker.
		"TSTUSVIRT?delisted=1": `[
			{"Code":"TSTDLUS1","Name":"Delisted US One","Country":"USA","Exchange":"TSTNASDQ","Currency":"USD","Type":"Common Stock","Isin":""},
			{"Code":"TSTLVUS1","Name":"Live US One",    "Country":"USA","Exchange":"TSTNASDQ","Currency":"USD","Type":"Common Stock","Isin":""}
		]`,
		"TSTNASDQ": `[]`,
	}

	// Use a custom mock server that routes the delisted pass to TSTUSVIRT, not "US".
	// The service looks for job.code == "US"; we need to override that check for this test.
	// Instead we patch the exchange code in the exchange list to match what the service looks for.
	// Since the service hard-codes `job.code == "US"`, we use "US" as the code in the exchange list.
	exchangeList = `[
		{"Code":"US",       "Name":"TSTUSVIRT","Country":"USA","Currency":"USD","CountryISO2":"US","CountryISO3":"USA"},
		{"Code":"TSTNASDQ", "Name":"TSTNASDQ", "Country":"USA","Currency":"USD","CountryISO2":"US","CountryISO3":"USA"}
	]`
	symbols["US"] = symbols["TSTUSVIRT"]
	symbols["US?delisted=1"] = symbols["TSTUSVIRT?delisted=1"]
	delete(symbols, "TSTUSVIRT")
	delete(symbols, "TSTUSVIRT?delisted=1")

	mock := newEODHDMockServer(exchangeList, symbols)
	defer mock.Close()

	router := setupAdminSyncRouter(pool, eodhd.NewClient("test-key", mock.URL))
	req, _ := http.NewRequest("POST", "/admin/securities/sync-from-provider", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result services.SyncSecuritiesResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if result.SecuritiesInserted != 2 {
		t.Errorf("Live pass: expected 2 inserted, got %d; response: %s", result.SecuritiesInserted, w.Body.String())
	}
	if result.DelistedInserted != 1 {
		t.Errorf("Delisted pass: expected 1 inserted (TSTDLUS1), got %d; response: %s", result.DelistedInserted, w.Body.String())
	}
	if result.DelistedSkipped != 1 {
		t.Errorf("Delisted pass: expected 1 skipped (TSTLVUS1 conflict), got %d", result.DelistedSkipped)
	}

	// TSTDLUS1 must be present and marked delisted.
	var delisted bool
	if err := pool.QueryRow(ctx, `SELECT delisted FROM dim_security WHERE ticker = 'TSTDLUS1'`).Scan(&delisted); err != nil {
		t.Fatalf("TSTDLUS1 not found in DB: %v", err)
	}
	if !delisted {
		t.Errorf("TSTDLUS1 should have delisted=true")
	}

	// TSTLVUS1 must remain with delisted=false (conflict not overwritten).
	if err := pool.QueryRow(ctx, `SELECT delisted FROM dim_security WHERE ticker = 'TSTLVUS1'`).Scan(&delisted); err != nil {
		t.Fatalf("TSTLVUS1 not found in DB: %v", err)
	}
	if delisted {
		t.Errorf("TSTLVUS1 should have delisted=false (live ticker, conflict ignored)")
	}

}

// Helper functions

func cleanupTestSecurities(pool *pgxpool.Pool, tickers []string) {
	ctx := context.Background()
	for _, ticker := range tickers {
		pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker) // #nosec G104
	}
}

func cleanupTestExchange(pool *pgxpool.Pool, name string) {
	ctx := context.Background()
	pool.Exec(ctx, `DELETE FROM dim_exchanges WHERE name = $1`, name) // #nosec G104
}

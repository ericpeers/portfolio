package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/providers/eodhd"
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

	avClient := alphavantage.NewClient("test-key", "http://localhost:9999")
	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, priceRepo, eodhdClient)
	pricingSvc := services.NewPricingService(priceRepo, securityRepo, services.PricingClients{Price: avClient, Treasury: avClient})
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
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
			if body, ok := symbolsByCode[code]; ok {
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

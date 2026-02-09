// interesting_sandbox_test.go
//
// This test creates sample portfolios for experimentation and exploration.
// It does NOT run by default with "go test" - it requires explicit invocation.
//
// HOW TO RUN:
//   RUN_SANDBOX=true go test -run TestSandbox ./tests/... -v
//
// WHAT IT CREATES:
//   - User: "Test Sandy"
//   - Portfolio 1: "Ideal Allocation" (Ideal type with percentage allocations)
//   - Portfolio 2: "Active Holdings" (Active type with share counts)
//   - Portfolio 3: "Tech Heavy" (Active type, tech-focused)
//
// NOTE: This test intentionally does NOT clean up after itself.
// The created data remains in the database for manual experimentation.
// To remove the data, delete user "Test Sandy" and their portfolios manually.

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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

// setupSandboxRouter creates a router with all endpoints needed for sandbox testing
func setupSandboxRouter(pool *pgxpool.Pool, avClient *alphavantage.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)

	// Initialize repositories
	portfolioRepo := repository.NewPortfolioRepository(pool)
	securityRepo := repository.NewSecurityRepository(pool)
	priceCacheRepo := repository.NewPriceCacheRepository(pool)
	exchangeRepo := repository.NewExchangeRepository(pool)
	securityTypeRepo := repository.NewSecurityTypeRepository(pool)

	// Initialize services
	pricingSvc := services.NewPricingService(priceCacheRepo, securityRepo, avClient)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo)
	comparisonSvc := services.NewComparisonService(portfolioSvc, membershipSvc, performanceSvc)
	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, securityTypeRepo, avClient)

	// Initialize handlers
	portfolioHandler := handlers.NewPortfolioHandler(portfolioSvc)
	compareHandler := handlers.NewCompareHandler(comparisonSvc)
	adminHandler := handlers.NewAdminHandler(adminSvc, pricingSvc, membershipSvc, securityRepo)

	router := gin.New()

	// Portfolio routes
	router.POST("/portfolios", portfolioHandler.Create)
	router.GET("/portfolios/:id", portfolioHandler.Get)
	router.POST("/portfolios/compare", compareHandler.Compare)

	// Admin routes
	admin := router.Group("/admin")
	{
		admin.POST("/sync-securities", adminHandler.SyncSecurities)
	}

	return router
}

func TestSandbox(t *testing.T) {
	// Double protection: skip unless RUN_SANDBOX=true
	if os.Getenv("RUN_SANDBOX") != "true" {
		t.Skip("Skipping sandbox test. Set RUN_SANDBOX=true to run.")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Get AV_KEY from environment for real API calls
	avKey := os.Getenv("AV_KEY")
	if avKey == "" {
		t.Fatal("AV_KEY environment variable is required for sandbox test")
	}
	avClient := alphavantage.NewClient(avKey)

	router := setupSandboxRouter(pool, avClient)

	// Step 1: Create or reuse user "Test Sandy"
	userID := getOrCreateSandboxUser(t, pool, ctx)
	t.Logf("Using user ID: %d", userID)

	// Step 2: Sync securities from AlphaVantage
	t.Log("Syncing securities from AlphaVantage...")
	syncReq, _ := http.NewRequest("POST", "/admin/sync-securities", nil)
	syncW := httptest.NewRecorder()
	router.ServeHTTP(syncW, syncReq)

	if syncW.Code != http.StatusOK {
		t.Fatalf("Failed to sync securities: %d - %s", syncW.Code, syncW.Body.String())
	}

	var syncResult services.SyncSecuritiesResult
	if err := json.Unmarshal(syncW.Body.Bytes(), &syncResult); err != nil {
		t.Fatalf("Failed to parse sync result: %v", err)
	}
	t.Logf("Sync result: inserted=%d, skipped=%d", syncResult.SecuritiesInserted, syncResult.SecuritiesSkipped)

	// Step 3: Look up security IDs
	securityRepo := repository.NewSecurityRepository(pool)

	// All tickers we need
	allTickers := []string{"SPY", "JPRE", "HYGH", "SPEM", "SPDW", "SPMD", "NVDA", "AAPL", "MSFT", "GOOGL", "MAGS", "META", "AMZN", "NFLX"}
	securityIDs := make(map[string]int64)

	//FIXME. Why not just fetch every symbol? Why not just pass tickers for create portfolio in this instead of by security id?
	for _, ticker := range allTickers {
		sec, err := securityRepo.GetBySymbol(ctx, ticker)
		if err != nil {
			t.Fatalf("Failed to look up security %s: %v", ticker, err)
		}
		securityIDs[ticker] = sec.ID
		t.Logf("Security %s -> ID %d", ticker, sec.ID)
	}

	// Step 4: Create portfolios (idempotent - check if they exist first)
	portfolio1ID := getOrCreatePortfolio(t, pool, router, userID, "Ideal Allocation", models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: securityIDs["SPY"], PercentageOrShares: 0.40},
		{SecurityID: securityIDs["JPRE"], PercentageOrShares: 0.10},
		{SecurityID: securityIDs["HYGH"], PercentageOrShares: 0.10},
		{SecurityID: securityIDs["SPEM"], PercentageOrShares: 0.10},
		{SecurityID: securityIDs["SPDW"], PercentageOrShares: 0.10},
		{SecurityID: securityIDs["SPMD"], PercentageOrShares: 0.20},
	})
	t.Logf("Portfolio 1 (Ideal Allocation) ID: %d", portfolio1ID)

	portfolio2ID := getOrCreatePortfolio(t, pool, router, userID, "Active Holdings", models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: securityIDs["SPY"], PercentageOrShares: 1000},
		{SecurityID: securityIDs["SPEM"], PercentageOrShares: 200},
		{SecurityID: securityIDs["NVDA"], PercentageOrShares: 20},
		{SecurityID: securityIDs["SPDW"], PercentageOrShares: 100},
	})
	t.Logf("Portfolio 2 (Active Holdings) ID: %d", portfolio2ID)

	portfolio3ID := getOrCreatePortfolio(t, pool, router, userID, "Tech Heavy", models.PortfolioTypeActive, []models.MembershipRequest{
		{SecurityID: securityIDs["NVDA"], PercentageOrShares: 50},
		{SecurityID: securityIDs["AAPL"], PercentageOrShares: 100},
		{SecurityID: securityIDs["MSFT"], PercentageOrShares: 75},
		{SecurityID: securityIDs["GOOGL"], PercentageOrShares: 30},
	})
	t.Logf("Portfolio 3 (Tech Heavy) ID: %d", portfolio3ID)

	//these are portfolios should be just 10 stocks broken out.
	portfolio4ID := getOrCreatePortfolio(t, pool, router, userID, "Mag 7", models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: securityIDs["MAGS"], PercentageOrShares: 1.0},
	})
	t.Logf("Portfolio 4 (FANG+ Index) ID: %d", portfolio4ID)
	portfolio5ID := getOrCreatePortfolio(t, pool, router, userID, "FAANG And Microsoft", models.PortfolioTypeIdeal, []models.MembershipRequest{
		{SecurityID: securityIDs["META"], PercentageOrShares: 0.166},
		{SecurityID: securityIDs["AAPL"], PercentageOrShares: 0.166},
		{SecurityID: securityIDs["AMZN"], PercentageOrShares: 0.166},
		{SecurityID: securityIDs["NFLX"], PercentageOrShares: 0.166},
		{SecurityID: securityIDs["GOOGL"], PercentageOrShares: 0.166},
		{SecurityID: securityIDs["MSFT"], PercentageOrShares: 0.17},
	})
	t.Logf("Portfolio 5 (FAANG And MS) ID: %d", portfolio5ID)

	// Step 5: Compare portfolios (Portfolio 1 vs Portfolio 2)
	endDate := time.Now()
	startDate := endDate.AddDate(-1, 0, 0) // 1 year ago

	compareReq := models.CompareRequest{
		PortfolioA:  portfolio1ID,
		PortfolioB:  portfolio2ID,
		StartPeriod: models.FlexibleDate{Time: startDate},
		EndPeriod:   models.FlexibleDate{Time: endDate},
	}

	compareBody, _ := json.Marshal(compareReq)
	compareHttpReq, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(compareBody))
	compareHttpReq.Header.Set("Content-Type", "application/json")
	compareHttpReq.Header.Set("X-User-ID", fmt.Sprintf("%d", userID))

	compareW := httptest.NewRecorder()
	router.ServeHTTP(compareW, compareHttpReq)

	if compareW.Code != http.StatusOK {
		t.Logf("Compare request failed: %d - %s", compareW.Code, compareW.Body.String())
	} else {
		t.Log("Compare request succeeded")
		var compareResult models.CompareResponse
		if err := json.Unmarshal(compareW.Body.Bytes(), &compareResult); err == nil {
			t.Logf("Portfolio A (%s) gain: %.2f%%", compareResult.PortfolioA.Name, compareResult.PerformanceMetrics.PortfolioAMetrics.GainPercent)
			t.Logf("Portfolio B (%s) gain: %.2f%%", compareResult.PortfolioB.Name, compareResult.PerformanceMetrics.PortfolioBMetrics.GainPercent)
		}
	}

	// Step 6: Log curl commands for manual experimentation
	t.Log("\n" + "=" + "=================================================")
	t.Log("CURL COMMANDS FOR MANUAL EXPERIMENTATION")
	t.Log("=================================================")

	t.Logf(`
# Compare Ideal Allocation vs Active Holdings
curl -X POST http://localhost:8080/portfolios/compare \
  -H "Content-Type: application/json" \
  -H "X-User-ID: %d" \
  -d '{
    "portfolio_a": %d,
    "portfolio_b": %d,
    "start_period": "%s",
    "end_period": "%s"
  }'
`, userID, portfolio1ID, portfolio2ID, startDate.Format(time.RFC3339), endDate.Format(time.RFC3339))

	t.Logf(`
# Compare Ideal Allocation vs Tech Heavy
curl -X POST http://localhost:8080/portfolios/compare \
  -H "Content-Type: application/json" \
  -H "X-User-ID: %d" \
  -d '{
    "portfolio_a": %d,
    "portfolio_b": %d,
    "start_period": "%s",
    "end_period": "%s"
  }'
`, userID, portfolio1ID, portfolio3ID, startDate.Format(time.RFC3339), endDate.Format(time.RFC3339))

	t.Logf(`
# Compare Active Holdings vs Tech Heavy
curl -X POST http://localhost:8080/portfolios/compare \
  -H "Content-Type: application/json" \
  -H "X-User-ID: %d" \
  -d '{
    "portfolio_a": %d,
    "portfolio_b": %d,
    "start_period": "%s",
    "end_period": "%s"
  }'
`, userID, portfolio2ID, portfolio3ID, startDate.Format(time.RFC3339), endDate.Format(time.RFC3339))

	t.Logf(`
# Get Portfolio 1 (Ideal Allocation)
curl http://localhost:8080/portfolios/%d

# Get Portfolio 2 (Active Holdings)
curl http://localhost:8080/portfolios/%d

# Get Portfolio 3 (Tech Heavy)
curl http://localhost:8080/portfolios/%d
`, portfolio1ID, portfolio2ID, portfolio3ID)

	t.Log("=================================================")
	t.Log("Sandbox setup complete! Data persists in database.")
	t.Log("=================================================")
}

// getOrCreateSandboxUser creates user "Test Sandy" if it doesn't exist, or returns existing ID
func getOrCreateSandboxUser(t *testing.T, pool *pgxpool.Pool, ctx context.Context) int64 {
	t.Helper()

	// Check if user already exists
	var existingID int64
	err := pool.QueryRow(ctx, `SELECT id FROM dim_user WHERE name = $1`, "Test Sandy").Scan(&existingID)
	if err == nil {
		t.Log("Found existing user 'Test Sandy'")
		return existingID
	}

	// Create user
	var newID int64
	err = pool.QueryRow(ctx, `INSERT INTO dim_user (name, email, join_date) VALUES ($1, $2, $3) RETURNING id`, "Test Sandy", "testsandy@mtnboy.net", "2026-01-31").Scan(&newID)
	if err != nil {
		t.Fatalf("Failed to create user 'Test Sandy': %v", err)
	}

	t.Log("Created new user 'Test Sandy'")
	return newID
}

// getOrCreatePortfolio creates a portfolio if it doesn't exist, or returns existing ID
func getOrCreatePortfolio(t *testing.T, pool *pgxpool.Pool, router *gin.Engine, userID int64, name string, portfolioType models.PortfolioType, memberships []models.MembershipRequest) int64 {
	t.Helper()
	ctx := context.Background()

	// Check if portfolio already exists
	var existingID int64
	err := pool.QueryRow(ctx, `SELECT id FROM portfolio WHERE name = $1 AND owner = $2`, name, userID).Scan(&existingID)
	if err == nil {
		t.Logf("Found existing portfolio '%s'", name)
		return existingID
	}

	// Create portfolio via HTTP
	reqBody := models.CreatePortfolioRequest{
		PortfolioType: portfolioType,
		Name:          name,
		OwnerID:       userID,
		Memberships:   memberships,
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", fmt.Sprintf("%d", userID))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to create portfolio '%s': %d - %s", name, w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse portfolio response: %v", err)
	}

	t.Logf("Created new portfolio '%s'", name)
	return response.Portfolio.ID
}

// interesting_sandbox_test.go
//
// This test creates sample portfolios for experimentation and exploration.
// It does NOT run by default with "go test" - it requires explicit invocation.
//
// HOW TO RUN:
//   RUN_SANDBOX=true go test -run TestSandbox ./tests/... -v -count=1
//
// The count=1 forces it to run even if no other go files have changed. This is useful since you want to
// hit database records.
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
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

	// Initialize services
	pricingSvc := services.NewPricingService(priceCacheRepo, securityRepo, avClient)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	membershipSvc := services.NewMembershipService(securityRepo, portfolioRepo, pricingSvc, avClient)
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo)
	comparisonSvc := services.NewComparisonService(portfolioSvc, membershipSvc, performanceSvc)
	adminSvc := services.NewAdminService(securityRepo, exchangeRepo, avClient)

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

	// except I want userID #1 for most of our work. So I'll use that.
	userID = 1 //use test user in db by default
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

	// Step 3: Eliminate Step 3 via refactor. Use Ticker based lookup.

	// Step 4: Create portfolios (idempotent - check if they exist first)
	portfolio1ID := getOrCreatePortfolio(t, pool, router, userID, "Ideal Allocation", models.PortfolioTypeIdeal, []models.MembershipRequest{
		{Ticker: "SPY", PercentageOrShares: 0.40},
		{Ticker: "JPRE", PercentageOrShares: 0.10},
		{Ticker: "HYGH", PercentageOrShares: 0.10},
		{Ticker: "SPEM", PercentageOrShares: 0.10},
		{Ticker: "SPDW", PercentageOrShares: 0.10},
		{Ticker: "SPMD", PercentageOrShares: 0.20},
	})
	t.Logf("Portfolio 1 (Ideal Allocation) ID: %d", portfolio1ID)

	portfolio2ID := getOrCreatePortfolio(t, pool, router, userID, "Active Holdings", models.PortfolioTypeActive, []models.MembershipRequest{
		{Ticker: "SPY", PercentageOrShares: 1000},
		{Ticker: "SPEM", PercentageOrShares: 200},
		{Ticker: "NVDA", PercentageOrShares: 20},
		{Ticker: "SPDW", PercentageOrShares: 100},
	})
	t.Logf("Portfolio 2 (Active Holdings) ID: %d", portfolio2ID)

	portfolio3ID := getOrCreatePortfolio(t, pool, router, userID, "Tech Heavy", models.PortfolioTypeActive, []models.MembershipRequest{
		{Ticker: "NVDA", PercentageOrShares: 50},
		{Ticker: "AAPL", PercentageOrShares: 100},
		{Ticker: "MSFT", PercentageOrShares: 75},
		{Ticker: "GOOGL", PercentageOrShares: 30},
	})
	t.Logf("Portfolio 3 (Tech Heavy) ID: %d", portfolio3ID)

	//these are portfolios of just a few holdings. It allows quick compares with minimal ETF breakout.
	portfolio4ID := getOrCreatePortfolio(t, pool, router, userID, "Mag 7 (via MAGS)", models.PortfolioTypeIdeal, []models.MembershipRequest{
		{Ticker: "MAGS", PercentageOrShares: 1.0},
	})
	t.Logf("Portfolio 4 (Mag7 via MAGS) ID: %d", portfolio4ID)

	portfolio5ID := getOrCreatePortfolio(t, pool, router, userID, "Mag 7 (via direct)", models.PortfolioTypeIdeal, []models.MembershipRequest{
		{Ticker: "AAPL", PercentageOrShares: 0.142857},
		{Ticker: "AMZN", PercentageOrShares: 0.142857},
		{Ticker: "GOOGL", PercentageOrShares: 0.142857},
		{Ticker: "META", PercentageOrShares: 0.142857},
		{Ticker: "MSFT", PercentageOrShares: 0.142857},
		{Ticker: "NVDA", PercentageOrShares: 0.142857},
		{Ticker: "TSLA", PercentageOrShares: 0.142857},
	})
	t.Logf("Portfolio 5 (Mag7 direct) ID: %d", portfolio5ID)

	portfolio6ID := getOrCreatePortfolio(t, pool, router, userID, "FAANG And Microsoft", models.PortfolioTypeIdeal, []models.MembershipRequest{
		{Ticker: "META", PercentageOrShares: 0.166},
		{Ticker: "AAPL", PercentageOrShares: 0.166},
		{Ticker: "AMZN", PercentageOrShares: 0.166},
		{Ticker: "NFLX", PercentageOrShares: 0.166},
		{Ticker: "GOOGL", PercentageOrShares: 0.166},
		{Ticker: "MSFT", PercentageOrShares: 0.17},
	})
	t.Logf("Portfolio 6 (FAANG And MS) ID: %d", portfolio6ID)

	portfolio7ID := getOrCreatePortfolio(t, pool, router, userID, "Allie Ideal", models.PortfolioTypeIdeal, []models.MembershipRequest{
		{Ticker: "SPY", PercentageOrShares: 0.55},
		{Ticker: "SPMD", PercentageOrShares: 0.10},
		{Ticker: "SPSM", PercentageOrShares: 0.05},
		{Ticker: "SPEM", PercentageOrShares: 0.05},
		{Ticker: "SPDW", PercentageOrShares: 0.10},
		{Ticker: "HYGH", PercentageOrShares: 0.025},
		{Ticker: "IGIB", PercentageOrShares: 0.025},
		{Ticker: "REZ", PercentageOrShares: 0.05},
		{Ticker: "JPRE", PercentageOrShares: 0.05},
	})
	t.Logf("Portfolio 7 (Allie Ideal) ID: %d", portfolio7ID)

	portfolio8ID := getOrCreatePortfolioFromCSV(t, pool, router, userID, "Allie Actual", models.PortfolioTypeActive, "merged_clean.csv")
	t.Logf("Portfolio 8 (Allie Actual) ID: %d", portfolio8ID)

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

# Create Allie Actual from CSV (multipart)
curl -X POST http://localhost:8080/portfolios \
  -H "X-User-ID: %d" \
  -F 'metadata={"portfolio_type":"Active","objective":"Growth","name":"Allie Actual","owner_id":%d}' \
  -F memberships=@tests/merged_clean.csv

# Compare Allie Ideal vs Allie Actual
curl -X POST http://localhost:8080/portfolios/compare \
  -H "Content-Type: application/json" \
  -H "X-User-ID: %d" \
  -d '{
    "portfolio_a": %d,
    "portfolio_b": %d,
    "start_period": "%s",
    "end_period": "%s"
  }'
`, portfolio1ID, portfolio2ID, portfolio3ID,
		userID, userID,
		userID, portfolio7ID, portfolio8ID, startDate.Format(time.RFC3339), endDate.Format(time.RFC3339))

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
func getOrCreatePortfolio(t *testing.T, pool *pgxpool.Pool, router *gin.Engine, userID int64, name string, portfolioType models.PortfolioType, memberships []models.MembershipRequest) int64 { //nolint:unparam
	return getOrCreatePortfolioWithObjective(t, pool, router, userID, name, portfolioType, models.ObjectiveGrowth, memberships)
}

// getOrCreatePortfolioWithObjective creates a portfolio with a specific objective if it doesn't exist
func getOrCreatePortfolioWithObjective(t *testing.T, pool *pgxpool.Pool, router *gin.Engine, userID int64, name string, portfolioType models.PortfolioType, objective models.Objective, memberships []models.MembershipRequest) int64 {
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
		Objective:     objective,
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

// getOrCreatePortfolioFromCSV creates a portfolio from a CSV file via multipart upload
func getOrCreatePortfolioFromCSV(t *testing.T, pool *pgxpool.Pool, router *gin.Engine, userID int64, name string, portfolioType models.PortfolioType, csvFilename string) int64 {
	t.Helper()
	ctx := context.Background()

	// Check if portfolio already exists
	var existingID int64
	err := pool.QueryRow(ctx, `SELECT id FROM portfolio WHERE name = $1 AND owner = $2`, name, userID).Scan(&existingID)
	if err == nil {
		t.Logf("Found existing portfolio '%s'", name)
		return existingID
	}

	// Read CSV file from the same directory as this test file
	csvPath := filepath.Join(".", csvFilename)
	csvContent, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("Failed to read CSV file '%s': %v", csvPath, err)
	}

	// Build multipart request
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	metadata := fmt.Sprintf(`{"portfolio_type":"%s","objective":"Growth","name":"%s","owner_id":%d}`, portfolioType, name, userID)
	if err := writer.WriteField("metadata", metadata); err != nil {
		t.Fatalf("Failed to write metadata field: %v", err)
	}

	part, err := writer.CreateFormFile("memberships", csvFilename)
	if err != nil {
		t.Fatalf("Failed to create memberships file part: %v", err)
	}
	if _, err := part.Write(csvContent); err != nil {
		t.Fatalf("Failed to write CSV content: %v", err)
	}
	writer.Close()

	req, _ := http.NewRequest("POST", "/portfolios", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", fmt.Sprintf("%d", userID))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to create portfolio '%s' from CSV: %d - %s", name, w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse portfolio response: %v", err)
	}

	t.Logf("Created new portfolio '%s' from CSV (%d memberships)", name, len(response.Memberships))
	return response.Portfolio.ID
}

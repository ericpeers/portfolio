package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupGlanceTestRouter creates a router with glance endpoints.
// priceClient may be nil if all price data will be served from cache.
func setupGlanceTestRouter(pool *pgxpool.Pool, avClient *alphavantage.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)

	portfolioRepo := repository.NewPortfolioRepository(pool)
	securityRepo := repository.NewSecurityRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	glanceRepo := repository.NewGlanceRepository(pool)

	pricingSvc := services.NewPricingService(priceRepo, securityRepo, avClient, nil, avClient)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	performanceSvc := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo)
	glanceSvc := services.NewGlanceService(glanceRepo, portfolioSvc, performanceSvc)

	glanceHandler := handlers.NewGlanceHandler(glanceSvc)

	router := gin.New()
	router.POST("/users/:user_id/glance", glanceHandler.Add)
	router.DELETE("/users/:user_id/glance/:portfolio_id", glanceHandler.Remove)
	router.GET("/users/:user_id/glance", glanceHandler.List)
	return router
}

// cleanupGlanceEntries removes all glance entries for a portfolio.
// Must be called before cleanupTestPortfolio to avoid FK violations.
func cleanupGlanceEntries(pool *pgxpool.Pool, portfolioID int64) {
	pool.Exec(context.Background(),
		`DELETE FROM portfolio_glance WHERE portfolio_id = $1`, portfolioID)
}

// createTestPortfolioWithDate creates a test portfolio with a specific creation date.
func createTestPortfolioWithDate(pool *pgxpool.Pool, name string, ownerID int64, portfolioType models.PortfolioType, memberships []models.MembershipRequest, createdAt time.Time) (int64, error) {
	ctx := context.Background()

	var portfolioID int64
	err := pool.QueryRow(ctx, `
		INSERT INTO portfolio (name, owner, portfolio_type, objective, created, updated)
		VALUES ($1, $2, $3, $4, $5, $5)
		RETURNING id
	`, name, ownerID, portfolioType, models.ObjectiveGrowth, createdAt).Scan(&portfolioID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert portfolio: %w", err)
	}

	for _, m := range memberships {
		_, err := pool.Exec(ctx, `
			INSERT INTO portfolio_membership (portfolio_id, security_id, percentage_or_shares)
			VALUES ($1, $2, $3)
		`, portfolioID, m.SecurityID, m.PercentageOrShares)
		if err != nil {
			return 0, fmt.Errorf("failed to insert membership: %w", err)
		}
	}

	return portfolioID, nil
}

// TestGlanceAdd verifies POST /users/:user_id/glance pins a portfolio.
func TestGlanceAdd(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupGlanceTestRouter(pool, nil)

	const portfolioName = "Glance Add Test Portfolio"
	const ownerID = int64(1)

	portfolioID, err := createTestPortfolio(pool, portfolioName, ownerID, models.PortfolioTypeActive, nil)
	if err != nil {
		t.Fatalf("Failed to create test portfolio: %v", err)
	}
	defer func() {
		cleanupGlanceEntries(pool, portfolioID)
		cleanupTestPortfolio(pool, portfolioName, ownerID)
	}()

	body, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: portfolioID})

	// First add: expect 201 Created
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if added, _ := resp["added"].(bool); !added {
		t.Errorf("Expected added=true on first pin, got %v", resp["added"])
	}

	// Second add (duplicate): expect 200 OK with added=false
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200 on duplicate pin, got %d: %s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if added, _ := resp["added"].(bool); added {
		t.Errorf("Expected added=false on duplicate pin, got %v", resp["added"])
	}
}

// TestGlanceAddNotFound verifies that pinning a nonexistent portfolio returns 404.
func TestGlanceAddNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupGlanceTestRouter(pool, nil)

	body, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: 999999999})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404 for nonexistent portfolio, got %d", w.Code)
	}
}

// TestGlanceRemove verifies DELETE /users/:user_id/glance/:portfolio_id unpins a portfolio.
func TestGlanceRemove(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupGlanceTestRouter(pool, nil)

	const portfolioName = "Glance Remove Test Portfolio"
	const ownerID = int64(1)

	portfolioID, err := createTestPortfolio(pool, portfolioName, ownerID, models.PortfolioTypeActive, nil)
	if err != nil {
		t.Fatalf("Failed to create test portfolio: %v", err)
	}
	defer func() {
		cleanupGlanceEntries(pool, portfolioID)
		cleanupTestPortfolio(pool, portfolioName, ownerID)
	}()

	// Pin first
	body, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: portfolioID})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("Setup: expected 201, got %d", w.Code)
	}

	// Remove: expect 204
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodDelete, fmt.Sprintf("/users/1/glance/%d", portfolioID), nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected 204 on remove, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGlanceRemoveNotFound verifies that removing an unpinned portfolio returns 404.
func TestGlanceRemoveNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupGlanceTestRouter(pool, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodDelete, "/users/1/glance/999999999", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404 for unpinned portfolio, got %d", w.Code)
	}
}

// TestGlanceListEmpty verifies GET /users/:user_id/glance returns an empty list
// when no portfolios are pinned.
func TestGlanceListEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	// Use a large user ID unlikely to have glance entries in production data.
	router := setupGlanceTestRouter(pool, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/users/999999/glance", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp models.GlanceListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if len(resp.Portfolios) != 0 {
		t.Errorf("Expected empty portfolios list, got %d items", len(resp.Portfolios))
	}
}

// TestGlanceList verifies GET /users/:user_id/glance returns correct portfolio metrics.
func TestGlanceList(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)

	// Use a week of Monday-Friday price data for stable, cache-only test execution.
	// Feb 23-27 2026 (Mon-Fri).
	priceStart := time.Date(2026, 2, 23, 0, 0, 0, 0, time.UTC)
	priceEnd := time.Date(2026, 2, 27, 0, 0, 0, 0, time.UTC)

	secID1, err := createTestStock(pool, "GLTSTKX", "Glance Test Stock X")
	if err != nil {
		t.Fatalf("Failed to create test security 1: %v", err)
	}
	defer cleanupTestSecurity(pool, "GLTSTKX")

	secID2, err := createTestStock(pool, "GLTSTRX", "Glance Test Stock R")
	if err != nil {
		t.Fatalf("Failed to create test security 2: %v", err)
	}
	defer cleanupTestSecurity(pool, "GLTSTRX")

	if err := insertPriceData(pool, secID1, priceStart, priceEnd, 100.0); err != nil {
		t.Fatalf("Failed to insert price data for security 1: %v", err)
	}
	if err := insertPriceData(pool, secID2, priceStart, priceEnd, 50.0); err != nil {
		t.Fatalf("Failed to insert price data for security 2: %v", err)
	}

	const portfolioName = "Glance List Test Portfolio"
	const ownerID = int64(1)

	memberships := []models.MembershipRequest{
		{SecurityID: secID1, PercentageOrShares: 10.0},
		{SecurityID: secID2, PercentageOrShares: 20.0},
	}
	// Portfolio created on first trading day of price data.
	portfolioID, err := createTestPortfolioWithDate(pool, portfolioName, ownerID, models.PortfolioTypeActive, memberships, priceStart)
	if err != nil {
		t.Fatalf("Failed to create test portfolio: %v", err)
	}
	defer func() {
		cleanupGlanceEntries(pool, portfolioID)
		cleanupTestPortfolio(pool, portfolioName, ownerID)
	}()

	// Use mock AV server that serves treasury data so ComputeSharpe doesn't try real fetches.
	// (Glance doesn't compute Sharpe, but pricingSvc initializes the same service chain.)
	avServer := createMockETFServer(nil, nil)
	defer avServer.Close()
	avClient := alphavantage.NewClientWithBaseURL("test-key", avServer.URL)

	router := setupGlanceTestRouter(pool, avClient)

	// Pin the portfolio
	body, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: portfolioID})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/users/1/glance", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("Setup pin: expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// GET glance list
	w = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodGet, "/users/1/glance", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp models.GlanceListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal GlanceListResponse: %v", err)
	}

	// Find our portfolio in the response (there may be other pinned portfolios from prod data for user 1)
	var found *models.GlancePortfolio
	for i := range resp.Portfolios {
		if resp.Portfolios[i].PortfolioID == portfolioID {
			found = &resp.Portfolios[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("Test portfolio %d not found in glance list (got %d portfolios)", portfolioID, len(resp.Portfolios))
	}

	if found.Name != portfolioName {
		t.Errorf("Expected name %q, got %q", portfolioName, found.Name)
	}

	// 10 shares × $102 close + 20 shares × $52 close = 1020 + 1040 = 2060
	expectedValue := 10.0*102.0 + 20.0*52.0
	if found.CurrentValue != expectedValue {
		t.Errorf("Expected current_value %.2f, got %.2f", expectedValue, found.CurrentValue)
	}

	// All prices are constant so gain should be 0 dollar / 0 percent.
	if found.LifeOfPortfolioReturn.Dollar != 0.0 {
		t.Errorf("Expected life return dollar=0, got %.4f", found.LifeOfPortfolioReturn.Dollar)
	}
	if found.LifeOfPortfolioReturn.Percentage != 0.0 {
		t.Errorf("Expected life return percentage=0, got %.4f", found.LifeOfPortfolioReturn.Percentage)
	}
}

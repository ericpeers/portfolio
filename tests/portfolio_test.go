package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/epeers/portfolio/internal/handlers"
	"github.com/epeers/portfolio/internal/middleware"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	testPool         *pgxpool.Pool
	testRouter       *gin.Engine
	portfolioHandler *handlers.PortfolioHandler
	userHandler      *handlers.UserHandler
)

func setupTestRouter(pool *pgxpool.Pool) *gin.Engine {
	gin.SetMode(gin.TestMode)

	portfolioRepo := repository.NewPortfolioRepository(pool)
	securityRepo := repository.NewSecurityRepository(pool)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)

	portfolioHandler = handlers.NewPortfolioHandler(portfolioSvc)
	userHandler = handlers.NewUserHandler(portfolioSvc)

	router := gin.New()
	router.Use(middleware.ValidateUser())

	router.POST("/portfolios", portfolioHandler.Create)
	router.GET("/portfolios/:id", portfolioHandler.Get)
	router.PUT("/portfolios/:id", portfolioHandler.Update)
	router.DELETE("/portfolios/:id", portfolioHandler.Delete)
	router.GET("/users/:user_id/portfolios", userHandler.ListPortfolios)

	return router
}

// TestCreatePortfolioWithBadUserID tests creating a portfolio with an invalid user ID
func TestCreatePortfolioWithBadUserID(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Use a user ID that doesn't exist (99999)
	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Name:          "Bad User Portfolio",
		OwnerID:       99999,
		Memberships:   []models.MembershipRequest{},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "99999")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Should fail due to foreign key constraint on owner
	if w.Code == http.StatusCreated {
		t.Errorf("Expected error for bad user ID, got status %d", w.Code)
	}
}

// TestCreatePortfolioWithGoodUserID tests creating a portfolio with a valid user ID
func TestCreatePortfolioWithGoodUserID(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Clean up any existing test portfolio
	cleanupTestPortfolio(pool, "Good User Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Good User Portfolio", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Name:          "Good User Portfolio",
		OwnerID:       1, // Valid test user
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, PercentageOrShares: 0.60},
			{SecurityID: 2, PercentageOrShares: 0.40},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if response.Portfolio.Name != "Good User Portfolio" {
		t.Errorf("Expected name 'Good User Portfolio', got '%s'", response.Portfolio.Name)
	}

	if len(response.Memberships) != 2 {
		t.Errorf("Expected 2 memberships, got %d", len(response.Memberships))
	}
}

// TestCreatePortfolioWithConflictingName tests creating a portfolio with a duplicate name
func TestCreatePortfolioWithConflictingName(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Clean up and create the first portfolio
	cleanupTestPortfolio(pool, "Conflict Test Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Conflict Test Portfolio", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Name:          "Conflict Test Portfolio",
		OwnerID:       1,
		Memberships:   []models.MembershipRequest{},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to create first portfolio: %d - %s", w.Code, w.Body.String())
	}

	// Try to create another portfolio with the same name and type
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-User-ID", "1")

	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusConflict {
		t.Errorf("Expected status 409 Conflict, got %d: %s", w2.Code, w2.Body.String())
	}
}

// TestListPortfoliosEmpty tests listing portfolios for a user with no portfolios
func TestListPortfoliosEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Use a user ID that exists but has no portfolios (assuming user 2 exists and has none)
	// For this test, we'll use user 1 but clean up all portfolios first
	cleanupAllUserPortfolios(pool, 999) // Non-existent user will have empty list

	req, _ := http.NewRequest("GET", "/users/999/portfolios", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var portfolios []models.PortfolioListItem
	if err := json.Unmarshal(w.Body.Bytes(), &portfolios); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(portfolios) != 0 {
		t.Errorf("Expected 0 portfolios, got %d", len(portfolios))
	}
}

// TestListPortfoliosSingleton tests listing portfolios for a user with one portfolio
func TestListPortfoliosSingleton(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Clean up and create exactly one portfolio
	cleanupTestPortfolio(pool, "Singleton Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Singleton Portfolio", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Name:          "Singleton Portfolio",
		OwnerID:       1,
		Memberships:   []models.MembershipRequest{},
	}

	body, _ := json.Marshal(reqBody)
	createReq, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, createReq)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to create portfolio: %d", w.Code)
	}

	// Now list and verify we get at least one
	listReq, _ := http.NewRequest("GET", "/users/1/portfolios", nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, listReq)

	if w2.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w2.Code)
	}

	var portfolios []models.PortfolioListItem
	if err := json.Unmarshal(w2.Body.Bytes(), &portfolios); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(portfolios) < 1 {
		t.Errorf("Expected at least 1 portfolio, got %d", len(portfolios))
	}

	found := false
	for _, p := range portfolios {
		if p.Name == "Singleton Portfolio" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find 'Singleton Portfolio' in list")
	}
}

// TestListPortfoliosMultiple tests listing portfolios for a user with multiple portfolios
func TestListPortfoliosMultiple(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Clean up and create multiple portfolios
	names := []string{"Multi Portfolio A", "Multi Portfolio B", "Multi Portfolio C"}
	for _, name := range names {
		cleanupTestPortfolio(pool, name, 1)
	}
	defer func() {
		for _, name := range names {
			cleanupTestPortfolio(pool, name, 1)
		}
	}()

	for _, name := range names {
		reqBody := models.CreatePortfolioRequest{
			PortfolioType: models.PortfolioTypeIdeal,
			Name:          name,
			OwnerID:       1,
			Memberships:   []models.MembershipRequest{},
		}

		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-User-ID", "1")

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusCreated {
			t.Fatalf("Failed to create portfolio %s: %d", name, w.Code)
		}
	}

	// List and verify we get all three
	listReq, _ := http.NewRequest("GET", "/users/1/portfolios", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, listReq)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var portfolios []models.PortfolioListItem
	if err := json.Unmarshal(w.Body.Bytes(), &portfolios); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	foundCount := 0
	for _, p := range portfolios {
		for _, name := range names {
			if p.Name == name {
				foundCount++
				break
			}
		}
	}

	if foundCount != 3 {
		t.Errorf("Expected to find all 3 multi portfolios, found %d", foundCount)
	}
}

// TestUpdatePortfolio tests updating a portfolio and verifying the update
func TestUpdatePortfolio(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Clean up and create a portfolio to update
	cleanupTestPortfolio(pool, "Update Test Portfolio", 1)
	cleanupTestPortfolio(pool, "Updated Portfolio Name", 1)
	defer cleanupTestPortfolio(pool, "Update Test Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Updated Portfolio Name", 1)

	createReqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Name:          "Update Test Portfolio",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, PercentageOrShares: 1.0},
		},
	}

	body, _ := json.Marshal(createReqBody)
	createReq, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, createReq)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to create portfolio: %d - %s", w.Code, w.Body.String())
	}

	var created models.PortfolioWithMemberships
	json.Unmarshal(w.Body.Bytes(), &created)

	// Update the portfolio
	updateReqBody := models.UpdatePortfolioRequest{
		Name: "Updated Portfolio Name",
		Memberships: []models.MembershipRequest{
			{SecurityID: 2, PercentageOrShares: 1.0},
		},
	}

	updateBody, _ := json.Marshal(updateReqBody)
	updateReq, _ := http.NewRequest("PUT", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), bytes.NewBuffer(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	updateReq.Header.Set("X-User-ID", "1")

	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, updateReq)

	if w2.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", w2.Code, w2.Body.String())
	}

	// Verify the update by reading the portfolio
	getReq, _ := http.NewRequest("GET", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), nil)
	w3 := httptest.NewRecorder()
	router.ServeHTTP(w3, getReq)

	var updated models.PortfolioWithMemberships
	json.Unmarshal(w3.Body.Bytes(), &updated)

	if updated.Portfolio.Name != "Updated Portfolio Name" {
		t.Errorf("Expected name 'Updated Portfolio Name', got '%s'", updated.Portfolio.Name)
	}

	if len(updated.Memberships) != 1 || updated.Memberships[0].SecurityID != 2 {
		t.Errorf("Expected single membership with security_id 2, got %+v", updated.Memberships)
	}
}

// TestReadKnownGoodPortfolio tests reading a portfolio that exists
func TestReadKnownGoodPortfolio(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Create a portfolio to read
	cleanupTestPortfolio(pool, "Read Test Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Read Test Portfolio", 1)

	createReqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Name:          "Read Test Portfolio",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, PercentageOrShares: 10},
		},
	}

	body, _ := json.Marshal(createReqBody)
	createReq, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, createReq)

	var created models.PortfolioWithMemberships
	json.Unmarshal(w.Body.Bytes(), &created)

	// Read the portfolio
	getReq, _ := http.NewRequest("GET", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), nil)
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, getReq)

	if w2.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w2.Code)
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w2.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if response.Portfolio.ID != created.Portfolio.ID {
		t.Errorf("Expected portfolio ID %d, got %d", created.Portfolio.ID, response.Portfolio.ID)
	}
}

// TestIdealPortfolioRejectsMemberOver1 tests that an ideal portfolio rejects a member with value > 1.0
func TestIdealPortfolioRejectsMemberOver1(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	cleanupTestPortfolio(pool, "Over1 Member Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Over1 Member Portfolio", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Name:          "Over1 Member Portfolio",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, PercentageOrShares: 60}, // > 1.0, should be rejected
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for member > 1.0, got %d: %s", w.Code, w.Body.String())
	}
}

// TestIdealPortfolioRejectsTotalOver1 tests that an ideal portfolio rejects when total > 1.0
func TestIdealPortfolioRejectsTotalOver1(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	cleanupTestPortfolio(pool, "Over1 Total Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Over1 Total Portfolio", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Name:          "Over1 Total Portfolio",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, PercentageOrShares: 0.60},
			{SecurityID: 2, PercentageOrShares: 0.50}, // total 1.10 > 1.0
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for total > 1.0, got %d: %s", w.Code, w.Body.String())
	}
}

// TestIdealPortfolioAcceptsValidDecimals tests that valid decimal memberships are accepted
func TestIdealPortfolioAcceptsValidDecimals(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	cleanupTestPortfolio(pool, "Valid Decimal Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Valid Decimal Portfolio", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Name:          "Valid Decimal Portfolio",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, PercentageOrShares: 0.60},
			{SecurityID: 2, PercentageOrShares: 0.40},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201 for valid decimals, got %d: %s", w.Code, w.Body.String())
	}
}

// TestActivePortfolioAcceptsShareCounts tests that active portfolios accept values > 1.0 (share counts)
func TestActivePortfolioAcceptsShareCounts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	cleanupTestPortfolio(pool, "Active Share Count Portfolio", 1)
	defer cleanupTestPortfolio(pool, "Active Share Count Portfolio", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Name:          "Active Share Count Portfolio",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, PercentageOrShares: 100},
			{SecurityID: 2, PercentageOrShares: 50},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201 for active portfolio with share counts > 1.0, got %d: %s", w.Code, w.Body.String())
	}
}

// TestReadBadPortfolio tests reading a portfolio that doesn't exist
func TestReadBadPortfolio(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Try to read a portfolio with an ID that doesn't exist
	req, _ := http.NewRequest("GET", "/portfolios/999999", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}

	var response models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal error response: %v", err)
	}

	if response.Error != "not_found" {
		t.Errorf("Expected error 'not_found', got '%s'", response.Error)
	}
}

// Helper functions

func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testPool == nil {
		t.Fatal("Test database pool not initialized. Run tests with proper DB connection.")
	}
	return testPool
}

func cleanupTestPortfolio(pool *pgxpool.Pool, name string, ownerID int64) {
	ctx := context.Background()
	// Delete memberships first, then portfolio
	pool.Exec(ctx, `
		DELETE FROM portfolio_membership
		WHERE portfolio_id IN (
			SELECT id FROM portfolio WHERE name = $1 AND owner = $2
		)
	`, name, ownerID)
	pool.Exec(ctx, `DELETE FROM portfolio WHERE name = $1 AND owner = $2`, name, ownerID)
}

func cleanupAllUserPortfolios(pool *pgxpool.Pool, ownerID int64) {
	ctx := context.Background()
	pool.Exec(ctx, `
		DELETE FROM portfolio_membership
		WHERE portfolio_id IN (
			SELECT id FROM portfolio WHERE owner = $1
		)
	`, ownerID)
	pool.Exec(ctx, `DELETE FROM portfolio WHERE owner = $1`, ownerID)
}

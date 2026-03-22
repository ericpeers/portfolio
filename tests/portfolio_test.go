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
	"github.com/epeers/portfolio/internal/middleware"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	testPool         *pgxpool.Pool
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
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Use a user ID that doesn't exist (99999)
	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          nextPortfolioName(),
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
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ticker1 := nextTicker()
	ticker2 := nextTicker()
	id1, err := createTestStock(pool, ticker1, "Test Good 1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	id2, err := createTestStock(pool, ticker2, "Test Good 2")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer func() {
		cleanupTestSecurity(pool, ticker1)
		cleanupTestSecurity(pool, ticker2)
	}()

	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1, // Valid test user
		Memberships: []models.MembershipRequest{
			{SecurityID: id1, PercentageOrShares: 0.60},
			{SecurityID: id2, PercentageOrShares: 0.40},
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

	if response.Portfolio.Name != name {
		t.Errorf("Expected name %q, got %q", name, response.Portfolio.Name)
	}

	if len(response.Memberships) != 2 {
		t.Errorf("Expected 2 memberships, got %d", len(response.Memberships))
	}
}

// TestCreatePortfolioWithConflictingName tests creating a portfolio with a duplicate name
func TestCreatePortfolioWithConflictingName(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	name := nextPortfolioName()
	// Clean up and create the first portfolio
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
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
	t.Parallel()
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
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	name := nextPortfolioName()
	// Clean up and create exactly one portfolio
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
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
		if p.Name == name {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find %q in portfolio list", name)
	}
}

// TestListPortfoliosMultiple tests listing portfolios for a user with multiple portfolios
func TestListPortfoliosMultiple(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Clean up and create multiple portfolios
	names := []string{nextPortfolioName(), nextPortfolioName(), nextPortfolioName()}
	for _, n := range names {
		cleanupTestPortfolio(pool, n, 1)
	}
	defer func() {
		for _, n := range names {
			cleanupTestPortfolio(pool, n, 1)
		}
	}()

	for _, n := range names {
		reqBody := models.CreatePortfolioRequest{
			PortfolioType: models.PortfolioTypeIdeal,
			Objective:     models.ObjectiveGrowth,
			Name:          n,
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
			t.Fatalf("Failed to create portfolio %s: %d", n, w.Code)
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
		for _, n := range names {
			if p.Name == n {
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
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ticker1 := nextTicker()
	ticker2 := nextTicker()
	origName := nextPortfolioName()
	updatedName := nextPortfolioName()

	// Clean up and create a portfolio to update
	cleanupTestPortfolio(pool, origName, 1)
	cleanupTestPortfolio(pool, updatedName, 1)
	defer cleanupTestPortfolio(pool, origName, 1)
	defer cleanupTestPortfolio(pool, updatedName, 1)

	id1, err := createTestStock(pool, ticker1, "Test Update 1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	id2, err := createTestStock(pool, ticker2, "Test Update 2")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer func() {
		cleanupTestSecurity(pool, ticker1)
		cleanupTestSecurity(pool, ticker2)
	}()

	createReqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          origName,
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: id1, PercentageOrShares: 1.0},
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
		Name: updatedName,
		Memberships: []models.MembershipRequest{
			{SecurityID: id2, PercentageOrShares: 1.0},
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

	if updated.Portfolio.Name != updatedName {
		t.Errorf("Expected name %q, got %q", updatedName, updated.Portfolio.Name)
	}

	if len(updated.Memberships) != 1 || updated.Memberships[0].SecurityID != id2 {
		t.Errorf("Expected single membership with security_id %d, got %+v", id2, updated.Memberships)
	}
}

// TestReadKnownGoodPortfolio tests reading a portfolio that exists
func TestReadKnownGoodPortfolio(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ticker1 := nextTicker()
	name := nextPortfolioName()

	// Create a portfolio to read
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	id1, err := createTestStock(pool, ticker1, "Test Read 1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker1)

	createReqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: id1, PercentageOrShares: 10},
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

	if len(response.Memberships) != 1 {
		t.Fatalf("Expected 1 membership, got %d", len(response.Memberships))
	}
	if response.Memberships[0].Ticker != ticker1 {
		t.Errorf("Expected ticker %q, got %q", ticker1, response.Memberships[0].Ticker)
	}
}

// TestIdealPortfolioRejectsMemberOver1 tests that an ideal portfolio rejects a member with value > 1.0
func TestIdealPortfolioRejectsMemberOver1(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ticker1 := nextTicker()
	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	id1, err := createTestStock(pool, ticker1, "Test Over1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: id1, PercentageOrShares: 60}, // > 1.0, should be rejected
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
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ticker1 := nextTicker()
	ticker2 := nextTicker()
	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	id1, err := createTestStock(pool, ticker1, "Test Total 1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	id2, err := createTestStock(pool, ticker2, "Test Total 2")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer func() {
		cleanupTestSecurity(pool, ticker1)
		cleanupTestSecurity(pool, ticker2)
	}()

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: id1, PercentageOrShares: 0.60},
			{SecurityID: id2, PercentageOrShares: 0.50}, // total 1.10 > 1.0
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
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ticker1 := nextTicker()
	ticker2 := nextTicker()
	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	id1, err := createTestStock(pool, ticker1, "Test Valid 1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	id2, err := createTestStock(pool, ticker2, "Test Valid 2")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer func() {
		cleanupTestSecurity(pool, ticker1)
		cleanupTestSecurity(pool, ticker2)
	}()

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: id1, PercentageOrShares: 0.60},
			{SecurityID: id2, PercentageOrShares: 0.40},
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

// TestIdealPortfolioAcceptsManySmallAllocations tests that an ideal portfolio with many
// small allocations summing to exactly 1.0 is accepted despite floating point accumulation.
func TestIdealPortfolioAcceptsManySmallAllocations(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Create 8 test securities
	tickers := make([]string, 8)
	for i := range tickers {
		tickers[i] = nextTicker()
	}
	secIDs := make([]int64, len(tickers))
	for i, ticker := range tickers {
		id, err := createTestStock(pool, ticker, "Test Small Alloc "+ticker)
		if err != nil {
			t.Fatalf("Failed to create test security %s: %v", ticker, err)
		}
		secIDs[i] = id
	}
	defer func() {
		for _, ticker := range tickers {
			cleanupTestSecurity(pool, ticker)
		}
	}()

	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	// 8 allocations that sum to exactly 1.0 mathematically, but floating point
	// accumulation may produce a total slightly above 1.0
	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: secIDs[0], PercentageOrShares: 0.55},
			{SecurityID: secIDs[1], PercentageOrShares: 0.10},
			{SecurityID: secIDs[2], PercentageOrShares: 0.05},
			{SecurityID: secIDs[3], PercentageOrShares: 0.05},
			{SecurityID: secIDs[4], PercentageOrShares: 0.10},
			{SecurityID: secIDs[5], PercentageOrShares: 0.05},
			{SecurityID: secIDs[6], PercentageOrShares: 0.05},
			{SecurityID: secIDs[7], PercentageOrShares: 0.05},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201 for allocations summing to 1.0, got %d: %s", w.Code, w.Body.String())
	}
}

// TestActivePortfolioAcceptsShareCounts tests that active portfolios accept values > 1.0 (share counts)
func TestActivePortfolioAcceptsShareCounts(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ticker1 := nextTicker()
	ticker2 := nextTicker()
	// Create 2 test securities
	id1, err := createTestStock(pool, ticker1, "Test Active 1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	id2, err := createTestStock(pool, ticker2, "Test Active 2")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer func() {
		cleanupTestSecurity(pool, ticker1)
		cleanupTestSecurity(pool, ticker2)
	}()

	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveAggressiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: id1, PercentageOrShares: 100},
			{SecurityID: id2, PercentageOrShares: 50},
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

// TestCreatePortfolioWithInvalidObjective tests creating a portfolio with a bad objective value
func TestCreatePortfolioWithInvalidObjective(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.Objective("Yolo Trading"),
		Name:          nextPortfolioName(),
		OwnerID:       1,
		Memberships:   []models.MembershipRequest{},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for invalid objective, got %d: %s", w.Code, w.Body.String())
	}
}

// TestCreatePortfolioWithoutObjective tests creating a portfolio without an objective
func TestCreatePortfolioWithoutObjective(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Omit objective by sending raw JSON without the field
	name := nextPortfolioName()
	jsonBody := fmt.Sprintf(`{"portfolio_type":"Ideal","name":%q,"owner_id":1,"memberships":[]}`, name)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBufferString(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for missing objective, got %d: %s", w.Code, w.Body.String())
	}
}

// TestUpdatePortfolioObjective tests updating just the objective of a portfolio
func TestUpdatePortfolioObjective(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	// Create a portfolio with Growth objective
	createReqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships:   []models.MembershipRequest{},
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

	if created.Portfolio.Objective != models.ObjectiveGrowth {
		t.Fatalf("Expected objective 'Growth', got '%s'", created.Portfolio.Objective)
	}

	// Update only the objective
	newObjective := models.ObjectiveCapitalPreservation
	updateReqBody := models.UpdatePortfolioRequest{
		Objective: &newObjective,
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

	// Verify the update
	getReq, _ := http.NewRequest("GET", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), nil)
	w3 := httptest.NewRecorder()
	router.ServeHTTP(w3, getReq)

	var updated models.PortfolioWithMemberships
	json.Unmarshal(w3.Body.Bytes(), &updated)

	if updated.Portfolio.Objective != models.ObjectiveCapitalPreservation {
		t.Errorf("Expected objective 'Capital Preservation', got '%s'", updated.Portfolio.Objective)
	}
}

// --- Delete tests ---

// TestDeletePortfolio tests the normal deletion path: create a portfolio,
// DELETE it via the API, verify the response and that the row is gone from the DB.
func TestDeletePortfolio(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()
	router := setupTestRouter(pool)

	const ownerID = int64(1)
	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, ownerID)

	// Create via API so we get the generated ID back
	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       ownerID,
		Memberships:   []models.MembershipRequest{},
	}
	body, _ := json.Marshal(reqBody)
	createReq, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("X-User-ID", fmt.Sprintf("%d", ownerID))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, createReq)
	if w.Code != http.StatusCreated {
		t.Fatalf("Setup: expected 201 creating portfolio, got %d: %s", w.Code, w.Body.String())
	}
	var created models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("Setup: failed to unmarshal create response: %v", err)
	}
	portfolioID := created.Portfolio.ID
	// Cleanup guard — no-op if the DELETE test already removed it
	defer cleanupTestPortfolio(pool, name, ownerID)

	// DELETE the portfolio
	delReq, _ := http.NewRequest("DELETE", fmt.Sprintf("/portfolios/%d", portfolioID), nil)
	delReq.Header.Set("X-User-ID", fmt.Sprintf("%d", ownerID))
	wd := httptest.NewRecorder()
	router.ServeHTTP(wd, delReq)

	if wd.Code != http.StatusOK {
		t.Fatalf("Expected 200 on delete, got %d: %s", wd.Code, wd.Body.String())
	}
	var resp map[string]string
	if err := json.Unmarshal(wd.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal delete response: %v", err)
	}
	if resp["message"] != "portfolio deleted" {
		t.Errorf("Expected message 'portfolio deleted', got %q", resp["message"])
	}

	// Verify the row is actually gone
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM portfolio WHERE id = $1`, portfolioID).Scan(&count); err != nil {
		t.Fatalf("Failed to query portfolio: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected portfolio row to be deleted, but it still exists (id=%d)", portfolioID)
	}
}

// TestDeletePortfolioNotFound tests that deleting a portfolio ID that never existed returns 404.
func TestDeletePortfolioNotFound(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("DELETE", "/portfolios/999999999", nil)
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404 for non-existent portfolio, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if resp.Error != "not_found" {
		t.Errorf("Expected error='not_found', got %q", resp.Error)
	}
}

// TestDeletePortfolioAlreadyDeleted tests that attempting to delete a previously
// deleted portfolio returns 404 on the second call.
func TestDeletePortfolioAlreadyDeleted(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	const ownerID = int64(1)
	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, ownerID)
	defer cleanupTestPortfolio(pool, name, ownerID)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       ownerID,
		Memberships:   []models.MembershipRequest{},
	}
	body, _ := json.Marshal(reqBody)
	createReq, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("X-User-ID", fmt.Sprintf("%d", ownerID))
	wc := httptest.NewRecorder()
	router.ServeHTTP(wc, createReq)
	if wc.Code != http.StatusCreated {
		t.Fatalf("Setup: expected 201, got %d: %s", wc.Code, wc.Body.String())
	}
	var created models.PortfolioWithMemberships
	json.Unmarshal(wc.Body.Bytes(), &created)
	portfolioID := created.Portfolio.ID

	// First delete — must succeed
	del1, _ := http.NewRequest("DELETE", fmt.Sprintf("/portfolios/%d", portfolioID), nil)
	del1.Header.Set("X-User-ID", fmt.Sprintf("%d", ownerID))
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, del1)
	if w1.Code != http.StatusOK {
		t.Fatalf("First delete: expected 200, got %d: %s", w1.Code, w1.Body.String())
	}

	// Second delete — must return 404
	del2, _ := http.NewRequest("DELETE", fmt.Sprintf("/portfolios/%d", portfolioID), nil)
	del2.Header.Set("X-User-ID", fmt.Sprintf("%d", ownerID))
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, del2)
	if w2.Code != http.StatusNotFound {
		t.Errorf("Second delete: expected 404, got %d: %s", w2.Code, w2.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal second-delete response: %v", err)
	}
	if resp.Error != "not_found" {
		t.Errorf("Second delete: expected error='not_found', got %q", resp.Error)
	}
}

// TestDeletePortfolioUnauthorized tests that a user cannot delete another user's portfolio.
func TestDeletePortfolioUnauthorized(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	const ownerID = int64(1)
	const wrongUserID = int64(2)
	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, ownerID)
	defer cleanupTestPortfolio(pool, name, ownerID)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       ownerID,
		Memberships:   []models.MembershipRequest{},
	}
	body, _ := json.Marshal(reqBody)
	createReq, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("X-User-ID", fmt.Sprintf("%d", ownerID))
	wc := httptest.NewRecorder()
	router.ServeHTTP(wc, createReq)
	if wc.Code != http.StatusCreated {
		t.Fatalf("Setup: expected 201, got %d: %s", wc.Code, wc.Body.String())
	}
	var created models.PortfolioWithMemberships
	json.Unmarshal(wc.Body.Bytes(), &created)
	portfolioID := created.Portfolio.ID

	// Attempt delete as a different user
	delReq, _ := http.NewRequest("DELETE", fmt.Sprintf("/portfolios/%d", portfolioID), nil)
	delReq.Header.Set("X-User-ID", fmt.Sprintf("%d", wrongUserID))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, delReq)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 when wrong user tries to delete, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if resp.Error != "unauthorized" {
		t.Errorf("Expected error='unauthorized', got %q", resp.Error)
	}
}

// TestDeletePortfolioNoAuth tests that DELETE without an X-User-ID header returns 401.
func TestDeletePortfolioNoAuth(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("DELETE", "/portfolios/1", nil)
	// No X-User-ID header
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 with no auth header, got %d: %s", w.Code, w.Body.String())
	}
}

// TestReadBadPortfolio tests reading a portfolio that doesn't exist
func TestReadBadPortfolio(t *testing.T) {
	t.Parallel()
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

// TestValidateObjective verifies that ValidateObjective accepts all valid enum values
// and rejects unrecognised strings.
func TestValidateObjective(t *testing.T) {
	t.Parallel()
	valid := []models.Objective{
		models.ObjectiveAggressiveGrowth,
		models.ObjectiveGrowth,
		models.ObjectiveIncomeGeneration,
		models.ObjectiveCapitalPreservation,
		models.ObjectiveMixedGrowthIncome,
	}
	for _, obj := range valid {
		if err := services.ValidateObjective(obj); err != nil {
			t.Errorf("Expected no error for valid objective %q, got: %v", obj, err)
		}
	}

	invalid := []models.Objective{
		"",
		"unknown",
		"GROWTH",       // wrong capitalisation
		"growth",       // lowercase
		"Speculation",  // not a defined constant
	}
	for _, obj := range invalid {
		if err := services.ValidateObjective(obj); err == nil {
			t.Errorf("Expected error for invalid objective %q, got nil", obj)
		}
	}
}

// TestCreatePortfolioWithCreatedAt verifies that an explicit created_at is honoured on create,
// and that omitting it falls back to the current timestamp.
func TestCreatePortfolioWithCreatedAt(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ticker1 := nextTicker()
	id1, err := createTestStock(pool, ticker1, "Test CreatedAt 1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker1)

	nameExplicit := nextPortfolioName()
	nameDefault := nextPortfolioName()
	cleanupTestPortfolio(pool, nameExplicit, 1)
	cleanupTestPortfolio(pool, nameDefault, 1)
	defer cleanupTestPortfolio(pool, nameExplicit, 1)
	defer cleanupTestPortfolio(pool, nameDefault, 1)

	wantDate := time.Date(2021, 3, 15, 0, 0, 0, 0, time.UTC)

	// --- Create with explicit created_at ---
	explicitReq := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveGrowth,
		Name:          nameExplicit,
		OwnerID:       1,
		Memberships:   []models.MembershipRequest{{SecurityID: id1, PercentageOrShares: 10}},
		CreatedAt:     &models.FlexibleDate{Time: wantDate},
	}
	body, _ := json.Marshal(explicitReq)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("Expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var resp1 models.PortfolioWithMemberships
	json.Unmarshal(w.Body.Bytes(), &resp1)
	if !resp1.Portfolio.CreatedAt.Equal(wantDate) {
		t.Errorf("explicit created_at: got %v, want %v", resp1.Portfolio.CreatedAt, wantDate)
	}

	// --- Create without created_at — should be close to now ---
	defaultReq := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveGrowth,
		Name:          nameDefault,
		OwnerID:       1,
		Memberships:   []models.MembershipRequest{{SecurityID: id1, PercentageOrShares: 10}},
	}
	body2, _ := json.Marshal(defaultReq)
	req2, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body2))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-User-ID", "1")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	if w2.Code != http.StatusCreated {
		t.Fatalf("Expected 201, got %d: %s", w2.Code, w2.Body.String())
	}
	var resp2 models.PortfolioWithMemberships
	json.Unmarshal(w2.Body.Bytes(), &resp2)
	// created is a date column — compare calendar date, not exact timestamp
	today := time.Now().UTC().Truncate(24 * time.Hour)
	got := resp2.Portfolio.CreatedAt.UTC().Truncate(24 * time.Hour)
	if !got.Equal(today) {
		t.Errorf("default created_at should be today (%v), got %v", today, got)
	}
}

// TestUpdatePortfolioCreatedAt verifies that PUT /portfolios/:id accepts a created_at override
// and that subsequent GETs reflect the new value.
func TestUpdatePortfolioCreatedAt(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ticker1 := nextTicker()
	id1, err := createTestStock(pool, ticker1, "Test UpdateCreatedAt 1")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker1)

	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	// Create portfolio (no explicit created_at)
	createReq := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships:   []models.MembershipRequest{{SecurityID: id1, PercentageOrShares: 5}},
	}
	body, _ := json.Marshal(createReq)
	reqC, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	reqC.Header.Set("Content-Type", "application/json")
	reqC.Header.Set("X-User-ID", "1")
	wC := httptest.NewRecorder()
	router.ServeHTTP(wC, reqC)
	if wC.Code != http.StatusCreated {
		t.Fatalf("create: expected 201, got %d: %s", wC.Code, wC.Body.String())
	}
	var created models.PortfolioWithMemberships
	json.Unmarshal(wC.Body.Bytes(), &created)

	// Update with explicit created_at
	newDate := time.Date(2019, 7, 4, 0, 0, 0, 0, time.UTC)
	updateReq := models.UpdatePortfolioRequest{
		CreatedAt: &models.FlexibleDate{Time: newDate},
	}
	ubody, _ := json.Marshal(updateReq)
	reqU, _ := http.NewRequest("PUT", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), bytes.NewBuffer(ubody))
	reqU.Header.Set("Content-Type", "application/json")
	reqU.Header.Set("X-User-ID", "1")
	wU := httptest.NewRecorder()
	router.ServeHTTP(wU, reqU)
	if wU.Code != http.StatusOK {
		t.Fatalf("update: expected 200, got %d: %s", wU.Code, wU.Body.String())
	}

	// Verify via GET
	reqG, _ := http.NewRequest("GET", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), nil)
	wG := httptest.NewRecorder()
	router.ServeHTTP(wG, reqG)
	var updated models.PortfolioWithMemberships
	json.Unmarshal(wG.Body.Bytes(), &updated)
	if !updated.Portfolio.CreatedAt.Equal(newDate) {
		t.Errorf("after update: created_at got %v, want %v", updated.Portfolio.CreatedAt, newDate)
	}
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

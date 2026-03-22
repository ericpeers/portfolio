package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/epeers/portfolio/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setupTickerTestSecurities creates test securities and returns their tickers and IDs.
// Cleanup is registered via t.Cleanup — no defer needed in the caller.
func setupTickerTestSecurities(t *testing.T, pool *pgxpool.Pool) (map[string]int64, string, string) {
	t.Helper()
	ticker1 := nextTicker()
	ticker2 := nextTicker()
	tickers := []struct {
		ticker string
		name   string
	}{
		{ticker1, "Ticker Test One"},
		{ticker2, "Ticker Test Two"},
	}

	result := make(map[string]int64)
	for _, s := range tickers {
		id, err := createTestStock(pool, s.ticker, s.name)
		if err != nil {
			t.Fatalf("Failed to insert test security %s: %v", s.ticker, err)
		}
		result[s.ticker] = id
	}
	t.Cleanup(func() {
		cleanupTestSecurity(pool, ticker1)
		cleanupTestSecurity(pool, ticker2)
	})
	return result, ticker1, ticker2
}

// TestTickerCreateWithTickers tests creating a portfolio using ticker symbols instead of security IDs
func TestTickerCreateWithTickers(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	securities, t1, t2 := setupTickerTestSecurities(t, pool)

	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{Ticker: t1, PercentageOrShares: 0.60},
			{Ticker: t2, PercentageOrShares: 0.40},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(response.Memberships) != 2 {
		t.Fatalf("Expected 2 memberships, got %d", len(response.Memberships))
	}

	// Verify that the resolved security IDs match
	foundIDs := make(map[int64]bool)
	for _, m := range response.Memberships {
		foundIDs[m.SecurityID] = true
	}
	for ticker, expectedID := range securities {
		if !foundIDs[expectedID] {
			t.Errorf("Expected resolved security ID %d for ticker %s in memberships", expectedID, ticker)
		}
	}
}

// TestTickerCreateWithMixedInput tests creating a portfolio with both security IDs and tickers
func TestTickerCreateWithMixedInput(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	securities, t1, _ := setupTickerTestSecurities(t, pool)

	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, 1)
	defer cleanupTestPortfolio(pool, name, 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, PercentageOrShares: 0.50},
			{Ticker: t1, PercentageOrShares: 0.50},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(response.Memberships) != 2 {
		t.Fatalf("Expected 2 memberships, got %d", len(response.Memberships))
	}

	// Verify both the direct security ID and the resolved ticker ID are present
	foundIDs := make(map[int64]bool)
	for _, m := range response.Memberships {
		foundIDs[m.SecurityID] = true
	}
	if !foundIDs[1] {
		t.Error("Expected security ID 1 (direct) in memberships")
	}
	if !foundIDs[securities[t1]] {
		t.Errorf("Expected resolved security ID %d for ticker %s in memberships", securities[t1], t1)
	}
}

// TestTickerCreateUnknownTicker tests that an unknown ticker returns 400
func TestTickerCreateUnknownTicker(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          "Unknown Ticker Test",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{Ticker: "DOESNOTEXIST999", PercentageOrShares: 100},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var errResp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("Failed to unmarshal error response: %v", err)
	}

	if !strings.Contains(errResp.Message, "DOESNOTEXIST999") {
		t.Errorf("Expected error message to mention bad ticker, got: %s", errResp.Message)
	}
}

// TestTickerCreateBothFieldsSet tests that setting both security_id and ticker returns 400
func TestTickerCreateBothFieldsSet(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          "Both Fields Test",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, Ticker: "AAPL", PercentageOrShares: 100},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var errResp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("Failed to unmarshal error response: %v", err)
	}

	if !strings.Contains(errResp.Message, "cannot specify both") {
		t.Errorf("Expected error about both fields, got: %s", errResp.Message)
	}
}

// TestTickerCreateNeitherFieldSet tests that setting neither security_id nor ticker returns 400
func TestTickerCreateNeitherFieldSet(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          "Neither Field Test",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{PercentageOrShares: 100},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d: %s", w.Code, w.Body.String())
	}

	var errResp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("Failed to unmarshal error response: %v", err)
	}

	if !strings.Contains(errResp.Message, "must specify either") {
		t.Errorf("Expected error about neither field, got: %s", errResp.Message)
	}
}

// setupDedupTestSecurity creates a single test security for dedup tests.
// Cleanup is registered via t.Cleanup — no defer needed in the caller.
func setupDedupTestSecurity(t *testing.T, pool *pgxpool.Pool) (int64, string) {
	t.Helper()
	ticker := nextTicker()
	id, err := createTestStock(pool, ticker, "Dedup Test One")
	if err != nil {
		t.Fatalf("Failed to insert dedup test security: %v", err)
	}
	t.Cleanup(func() {
		cleanupTestSecurity(pool, ticker)
	})
	return id, ticker
}

// TestDedupCreateWithDuplicateSecurityID tests that submitting the same security_id twice
// on create merges both entries into one by summing their values.
func TestDedupCreateWithDuplicateSecurityID(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	id, _ := setupDedupTestSecurity(t, pool)

	cleanupTestPortfolio(pool, "Dedup SecurityID Create Test", 1)
	defer cleanupTestPortfolio(pool, "Dedup SecurityID Create Test", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveGrowth,
		Name:          "Dedup SecurityID Create Test",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: id, PercentageOrShares: 100},
			{SecurityID: id, PercentageOrShares: 50},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(response.Memberships) != 1 {
		t.Fatalf("Expected 1 merged membership, got %d", len(response.Memberships))
	}
	if response.Memberships[0].SecurityID != id {
		t.Errorf("Expected security ID %d, got %d", id, response.Memberships[0].SecurityID)
	}
	if response.Memberships[0].PercentageOrShares != 150 {
		t.Errorf("Expected merged value 150, got %g", response.Memberships[0].PercentageOrShares)
	}
}

// TestDedupCreateWithDuplicateTicker tests that submitting the same ticker twice
// on create merges both entries into one by summing their values.
func TestDedupCreateWithDuplicateTicker(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	_, ticker := setupDedupTestSecurity(t, pool)

	cleanupTestPortfolio(pool, "Dedup Ticker Create Test", 1)
	defer cleanupTestPortfolio(pool, "Dedup Ticker Create Test", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveGrowth,
		Name:          "Dedup Ticker Create Test",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{Ticker: ticker, PercentageOrShares: 100},
			{Ticker: ticker, PercentageOrShares: 50},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(response.Memberships) != 1 {
		t.Fatalf("Expected 1 merged membership, got %d", len(response.Memberships))
	}
	if response.Memberships[0].PercentageOrShares != 150 {
		t.Errorf("Expected merged value 150, got %g", response.Memberships[0].PercentageOrShares)
	}
}

// TestDedupCreateTickerAndIDSameSecurity tests that a ticker entry and a security_id entry
// pointing to the same underlying security are merged into one.
func TestDedupCreateTickerAndIDSameSecurity(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	id, ticker := setupDedupTestSecurity(t, pool)

	cleanupTestPortfolio(pool, "Dedup Mixed Create Test", 1)
	defer cleanupTestPortfolio(pool, "Dedup Mixed Create Test", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveGrowth,
		Name:          "Dedup Mixed Create Test",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{Ticker: ticker, PercentageOrShares: 100},
			{SecurityID: id, PercentageOrShares: 50},
		},
	}

	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(response.Memberships) != 1 {
		t.Fatalf("Expected 1 merged membership (ticker+id same security), got %d", len(response.Memberships))
	}
	if response.Memberships[0].SecurityID != id {
		t.Errorf("Expected security ID %d, got %d", id, response.Memberships[0].SecurityID)
	}
	if response.Memberships[0].PercentageOrShares != 150 {
		t.Errorf("Expected merged value 150, got %g", response.Memberships[0].PercentageOrShares)
	}
}

// TestDedupUpdateWithDuplicateSecurityID tests that submitting the same security_id twice
// on update merges both entries into one by summing their values.
func TestDedupUpdateWithDuplicateSecurityID(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	id, _ := setupDedupTestSecurity(t, pool)

	cleanupTestPortfolio(pool, "Dedup Update Test", 1)
	defer cleanupTestPortfolio(pool, "Dedup Update Test", 1)

	// Create the portfolio first
	createReqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeActive,
		Objective:     models.ObjectiveGrowth,
		Name:          "Dedup Update Test",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: id, PercentageOrShares: 10},
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

	// Update with the same security ID listed twice
	updateReqBody := models.UpdatePortfolioRequest{
		Memberships: []models.MembershipRequest{
			{SecurityID: id, PercentageOrShares: 100},
			{SecurityID: id, PercentageOrShares: 50},
		},
	}

	updateBody, _ := json.Marshal(updateReqBody)
	updateReq, _ := http.NewRequest("PUT", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), bytes.NewBuffer(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	updateReq.Header.Set("X-User-ID", "1")

	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, updateReq)

	if w2.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var updated models.PortfolioWithMemberships
	if err := json.Unmarshal(w2.Body.Bytes(), &updated); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(updated.Memberships) != 1 {
		t.Fatalf("Expected 1 merged membership after update, got %d", len(updated.Memberships))
	}
	if updated.Memberships[0].PercentageOrShares != 150 {
		t.Errorf("Expected merged value 150 after update, got %g", updated.Memberships[0].PercentageOrShares)
	}
}

// TestTickerUpdateWithTickers tests updating a portfolio's memberships using ticker symbols
func TestTickerUpdateWithTickers(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	securities, t1, t2 := setupTickerTestSecurities(t, pool)

	cleanupTestPortfolio(pool, "Ticker Update Test", 1)
	defer cleanupTestPortfolio(pool, "Ticker Update Test", 1)

	// Create a portfolio first with security IDs
	createReqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          "Ticker Update Test",
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

	// Update the portfolio using ticker-based memberships
	updateReqBody := models.UpdatePortfolioRequest{
		Memberships: []models.MembershipRequest{
			{Ticker: t1, PercentageOrShares: 0.70},
			{Ticker: t2, PercentageOrShares: 0.30},
		},
	}

	updateBody, _ := json.Marshal(updateReqBody)
	updateReq, _ := http.NewRequest("PUT", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), bytes.NewBuffer(updateBody))
	updateReq.Header.Set("Content-Type", "application/json")
	updateReq.Header.Set("X-User-ID", "1")

	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, updateReq)

	if w2.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var updated models.PortfolioWithMemberships
	if err := json.Unmarshal(w2.Body.Bytes(), &updated); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(updated.Memberships) != 2 {
		t.Fatalf("Expected 2 memberships after update, got %d", len(updated.Memberships))
	}

	// Verify resolved IDs
	foundIDs := make(map[int64]bool)
	for _, m := range updated.Memberships {
		foundIDs[m.SecurityID] = true
	}
	for ticker, expectedID := range securities {
		if !foundIDs[expectedID] {
			t.Errorf("Expected resolved security ID %d for ticker %s after update", expectedID, ticker)
		}
	}
}

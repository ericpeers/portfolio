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
// Caller must defer cleanupTickerTestSecurities.
func setupTickerTestSecurities(t *testing.T, pool *pgxpool.Pool) map[string]int64 {
	t.Helper()

	tickers := []struct {
		ticker string
		name   string
	}{
		{"TKTST1", "Ticker Test One"},
		{"TKTST2", "Ticker Test Two"},
	}

	result := make(map[string]int64)
	for _, s := range tickers {
		id, err := createTestStock(pool, s.ticker, s.name)
		if err != nil {
			t.Fatalf("Failed to insert test security %s: %v", s.ticker, err)
		}
		result[s.ticker] = id
	}
	return result
}

func cleanupTickerTestSecurities(pool *pgxpool.Pool) {
	cleanupTestSecurity(pool, "TKTST1")
	cleanupTestSecurity(pool, "TKTST2")
}

// TestTickerCreateWithTickers tests creating a portfolio using ticker symbols instead of security IDs
func TestTickerCreateWithTickers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	securities := setupTickerTestSecurities(t, pool)
	defer cleanupTickerTestSecurities(pool)

	cleanupTestPortfolio(pool, "Ticker Create Test", 1)
	defer cleanupTestPortfolio(pool, "Ticker Create Test", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          "Ticker Create Test",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{Ticker: "TKTST1", PercentageOrShares: 0.60},
			{Ticker: "TKTST2", PercentageOrShares: 0.40},
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
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	securities := setupTickerTestSecurities(t, pool)
	defer cleanupTickerTestSecurities(pool)

	cleanupTestPortfolio(pool, "Ticker Mixed Test", 1)
	defer cleanupTestPortfolio(pool, "Ticker Mixed Test", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          "Ticker Mixed Test",
		OwnerID:       1,
		Memberships: []models.MembershipRequest{
			{SecurityID: 1, PercentageOrShares: 0.50},
			{Ticker: "TKTST1", PercentageOrShares: 0.50},
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
	if !foundIDs[securities["TKTST1"]] {
		t.Errorf("Expected resolved security ID %d for ticker TKTST1 in memberships", securities["TKTST1"])
	}
}

// TestTickerCreateUnknownTicker tests that an unknown ticker returns 400
func TestTickerCreateUnknownTicker(t *testing.T) {
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

// TestTickerUpdateWithTickers tests updating a portfolio's memberships using ticker symbols
func TestTickerUpdateWithTickers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	securities := setupTickerTestSecurities(t, pool)
	defer cleanupTickerTestSecurities(pool)

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
			{Ticker: "TKTST1", PercentageOrShares: 0.70},
			{Ticker: "TKTST2", PercentageOrShares: 0.30},
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

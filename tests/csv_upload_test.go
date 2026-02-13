package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/epeers/portfolio/internal/models"
)

// buildMultipartRequest builds an HTTP request with multipart/form-data containing
// an optional metadata form field and an optional CSV file part named "memberships".
func buildMultipartRequest(t *testing.T, method, url, metadata, csvContent string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	if metadata != "" {
		if err := writer.WriteField("metadata", metadata); err != nil {
			t.Fatalf("failed to write metadata field: %v", err)
		}
	}

	if csvContent != "" {
		part, err := writer.CreateFormFile("memberships", "memberships.csv")
		if err != nil {
			t.Fatalf("failed to create memberships file part: %v", err)
		}
		if _, err := part.Write([]byte(csvContent)); err != nil {
			t.Fatalf("failed to write CSV content: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", "1")
	return req
}

func TestCSVCreateWithMultipart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	securities := setupTickerTestSecurities(t, pool)
	defer cleanupTickerTestSecurities(pool)

	cleanupTestPortfolio(pool, "CSV Create Test", 1)
	defer cleanupTestPortfolio(pool, "CSV Create Test", 1)

	metadata := `{"portfolio_type":"Ideal","objective":"Growth","name":"CSV Create Test","owner_id":1}`
	csv := "ticker,percentage_or_shares\nTKTST1,0.60\nTKTST2,0.40\n"

	req := buildMultipartRequest(t, "POST", "/portfolios", metadata, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(response.Memberships) != 2 {
		t.Fatalf("expected 2 memberships, got %d", len(response.Memberships))
	}

	foundIDs := make(map[int64]bool)
	for _, m := range response.Memberships {
		foundIDs[m.SecurityID] = true
	}
	for ticker, expectedID := range securities {
		if !foundIDs[expectedID] {
			t.Errorf("expected resolved security ID %d for ticker %s", expectedID, ticker)
		}
	}
}

func TestCSVCreateMissingMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	csv := "ticker,percentage_or_shares\nAAPL,60\n"
	req := buildMultipartRequest(t, "POST", "/portfolios", "", csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCSVCreateInvalidMetadataJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	csv := "ticker,percentage_or_shares\nAAPL,60\n"
	req := buildMultipartRequest(t, "POST", "/portfolios", "{not valid json", csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCSVCreateMissingCSVColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	metadata := `{"portfolio_type":"Ideal","objective":"Growth","name":"CSV Missing Col Test","owner_id":1}`
	csv := "ticker,something_else\nAAPL,60\n"
	req := buildMultipartRequest(t, "POST", "/portfolios", metadata, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCSVCreateInvalidCSVValue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	metadata := `{"portfolio_type":"Ideal","objective":"Growth","name":"CSV Bad Value Test","owner_id":1}`
	csv := "ticker,percentage_or_shares\nAAPL,not_a_number\n"
	req := buildMultipartRequest(t, "POST", "/portfolios", metadata, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCSVCreateNoMembershipsFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	cleanupTestPortfolio(pool, "CSV No Members Test", 1)
	defer cleanupTestPortfolio(pool, "CSV No Members Test", 1)

	metadata := `{"portfolio_type":"Ideal","objective":"Growth","name":"CSV No Members Test","owner_id":1}`
	req := buildMultipartRequest(t, "POST", "/portfolios", metadata, "")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if response.Portfolio.Name != "CSV No Members Test" {
		t.Errorf("expected name 'CSV No Members Test', got %q", response.Portfolio.Name)
	}
}

func TestCSVUpdateWithMultipart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	securities := setupTickerTestSecurities(t, pool)
	defer cleanupTickerTestSecurities(pool)

	cleanupTestPortfolio(pool, "CSV Update Test", 1)
	defer cleanupTestPortfolio(pool, "CSV Update Test", 1)

	// Create via JSON first
	createReqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          "CSV Update Test",
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
		t.Fatalf("failed to create portfolio: %d - %s", w.Code, w.Body.String())
	}

	var created models.PortfolioWithMemberships
	json.Unmarshal(w.Body.Bytes(), &created)

	// Update via multipart CSV
	metadata := `{"name":"CSV Update Test"}`
	csv := "ticker,percentage_or_shares\nTKTST1,0.70\nTKTST2,0.30\n"
	updateReq := buildMultipartRequest(t, "PUT", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), metadata, csv)

	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, updateReq)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var updated models.PortfolioWithMemberships
	if err := json.Unmarshal(w2.Body.Bytes(), &updated); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(updated.Memberships) != 2 {
		t.Fatalf("expected 2 memberships after update, got %d", len(updated.Memberships))
	}

	foundIDs := make(map[int64]bool)
	for _, m := range updated.Memberships {
		foundIDs[m.SecurityID] = true
	}
	for ticker, expectedID := range securities {
		if !foundIDs[expectedID] {
			t.Errorf("expected resolved security ID %d for ticker %s after update", expectedID, ticker)
		}
	}
}

func TestCSVCreateJSONStillWorks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	cleanupTestPortfolio(pool, "CSV JSON Regression", 1)
	defer cleanupTestPortfolio(pool, "CSV JSON Regression", 1)

	reqBody := models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          "CSV JSON Regression",
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
		t.Fatalf("expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if response.Portfolio.Name != "CSV JSON Regression" {
		t.Errorf("expected name 'CSV JSON Regression', got %q", response.Portfolio.Name)
	}
	if len(response.Memberships) != 2 {
		t.Errorf("expected 2 memberships, got %d", len(response.Memberships))
	}
}

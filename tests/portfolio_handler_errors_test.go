package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/epeers/portfolio/internal/models"
)

// TestUpdatePortfolio_InvalidID verifies that PUT /portfolios/abc returns 400
// (invalid portfolio ID).
func TestUpdatePortfolio_InvalidID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	body, _ := json.Marshal(map[string]any{"name": "new-name"})
	req, _ := http.NewRequest("PUT", "/portfolios/abc", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid portfolio ID, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != "bad_request" {
		t.Errorf("expected error='bad_request', got %q", resp.Error)
	}
}

// TestUpdatePortfolio_NotFound verifies that PUT /portfolios/:id for a non-existent
// portfolio returns 404.
func TestUpdatePortfolio_NotFound(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	body, _ := json.Marshal(map[string]any{"name": "whatever"})
	req, _ := http.NewRequest("PUT", "/portfolios/999999999", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for non-existent portfolio, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != "not_found" {
		t.Errorf("expected error='not_found', got %q", resp.Error)
	}
}

// TestUpdatePortfolio_Unauthorized verifies that PUT /portfolios/:id by a user who
// does not own the portfolio returns 401.
func TestUpdatePortfolio_Unauthorized(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	const ownerID = int64(1)
	const otherID = int64(2)
	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, ownerID)
	defer cleanupTestPortfolio(pool, name, ownerID)

	// Create portfolio as owner 1.
	createBody, _ := json.Marshal(models.CreatePortfolioRequest{
		PortfolioType: models.PortfolioTypeIdeal,
		Objective:     models.ObjectiveGrowth,
		Name:          name,
		OwnerID:       ownerID,
		Memberships:   []models.MembershipRequest{},
	})
	cr, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(createBody))
	cr.Header.Set("Content-Type", "application/json")
	cr.Header.Set("X-User-ID", fmt.Sprintf("%d", ownerID))
	cw := httptest.NewRecorder()
	router.ServeHTTP(cw, cr)
	if cw.Code != http.StatusCreated {
		t.Fatalf("setup: expected 201, got %d: %s", cw.Code, cw.Body.String())
	}
	var created models.PortfolioWithMemberships
	json.Unmarshal(cw.Body.Bytes(), &created)

	// Attempt update as owner 2.
	updateBody, _ := json.Marshal(map[string]any{"name": "hijack"})
	req, _ := http.NewRequest("PUT", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), bytes.NewBuffer(updateBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", fmt.Sprintf("%d", otherID))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauthorized update, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != "unauthorized" {
		t.Errorf("expected error='unauthorized', got %q", resp.Error)
	}
}

// TestGetPortfolio_NotFound verifies that GET /portfolios/:id for a non-existent
// portfolio returns 404.
func TestGetPortfolio_NotFound(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("GET", "/portfolios/999999999", nil)
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for non-existent portfolio, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGetPortfolio_InvalidID verifies that GET /portfolios/abc returns 400.
func TestGetPortfolio_InvalidID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("GET", "/portfolios/abc", nil)
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid portfolio ID, got %d: %s", w.Code, w.Body.String())
	}
}

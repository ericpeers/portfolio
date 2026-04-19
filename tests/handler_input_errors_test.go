package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/epeers/portfolio/internal/models"
)

// --- Glance handler input-validation errors ---

// TestGlanceAdd_InvalidUserID verifies POST /users/abc/glance returns 400.
func TestGlanceAdd_InvalidUserID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupGlanceTestRouter(pool)

	body, _ := json.Marshal(models.AddGlanceRequest{PortfolioID: 1})
	req, _ := http.NewRequest("POST", "/users/abc/glance", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGlanceAdd_InvalidJSON verifies POST /users/1/glance with malformed JSON returns 400.
func TestGlanceAdd_InvalidJSON(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupGlanceTestRouter(pool)

	req, _ := http.NewRequest("POST", "/users/1/glance", bytes.NewBufferString("not-json"))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGlanceRemove_InvalidUserID verifies DELETE /users/abc/glance/1 returns 400.
func TestGlanceRemove_InvalidUserID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupGlanceTestRouter(pool)

	req, _ := http.NewRequest("DELETE", "/users/abc/glance/1", nil)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGlanceRemove_InvalidPortfolioID verifies DELETE /users/1/glance/abc returns 400.
func TestGlanceRemove_InvalidPortfolioID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupGlanceTestRouter(pool)

	req, _ := http.NewRequest("DELETE", "/users/1/glance/abc", nil)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGlanceList_InvalidUserID verifies GET /users/abc/glance returns 400.
func TestGlanceList_InvalidUserID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupGlanceTestRouter(pool)

	req, _ := http.NewRequest("GET", "/users/abc/glance", nil)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// --- Compare handler input-validation errors ---

// TestCompare_InvalidJSON verifies POST /portfolios/compare with malformed JSON returns 400.
func TestCompare_InvalidJSON(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupDailyValuesTestRouter(pool)

	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBufferString("not-json"))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// TestCompare_EndBeforeStart verifies POST /portfolios/compare with end before start returns 400.
func TestCompare_EndBeforeStart(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupDailyValuesTestRouter(pool)

	body, _ := json.Marshal(map[string]any{
		"portfolio_a":  1,
		"portfolio_b":  2,
		"start_period": "2025-06-01",
		"end_period":   "2025-01-01",
	})
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for end before start, got %d: %s", w.Code, w.Body.String())
	}
}

// TestCompare_NotFound verifies POST /portfolios/compare with non-existent portfolio IDs
// returns 404.
func TestCompare_NotFound(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupDailyValuesTestRouter(pool)

	body, _ := json.Marshal(map[string]any{
		"portfolio_a":  999999991,
		"portfolio_b":  999999992,
		"start_period": "2025-01-01",
		"end_period":   "2025-06-01",
	})
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for non-existent portfolios, got %d: %s", w.Code, w.Body.String())
	}
}

// --- User handler input-validation errors ---

// TestListPortfolios_InvalidUserID verifies GET /users/abc/portfolios returns 400.
func TestListPortfolios_InvalidUserID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("GET", "/users/abc/portfolios", nil)
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid user ID, got %d: %s", w.Code, w.Body.String())
	}
}

// TestCompare_InvalidDate verifies that POST /portfolios/compare with an unparseable
// date string returns 400, covering the FlexibleDate.UnmarshalJSON error path.
func TestCompare_InvalidDate(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupDailyValuesTestRouter(pool)

	body, _ := json.Marshal(map[string]any{
		"portfolio_a":  1,
		"portfolio_b":  2,
		"start_period": "not-a-date",
		"end_period":   "2025-06-01",
	})
	req, _ := http.NewRequest("POST", "/portfolios/compare", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid date, got %d: %s", w.Code, w.Body.String())
	}
}


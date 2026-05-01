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

// TestUpdatePortfolio_InvalidID verifies that PUT /portfolios/abc returns 400
// (invalid portfolio ID).
func TestUpdatePortfolio_InvalidID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	body, _ := json.Marshal(map[string]any{"name": "new-name"})
	req, _ := http.NewRequest("PUT", "/portfolios/abc", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader(1, "USER"))

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
	req.Header.Set("Authorization", authHeader(1, "USER"))

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
	req.Header.Set("Authorization", authHeader(otherID, "USER"))

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
	req.Header.Set("Authorization", authHeader(1, "USER"))

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
	req.Header.Set("Authorization", authHeader(1, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid portfolio ID, got %d: %s", w.Code, w.Body.String())
	}
}

// TestDeletePortfolio_InvalidID verifies that DELETE /portfolios/abc returns 400.
func TestDeletePortfolio_InvalidID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("DELETE", "/portfolios/abc", nil)
	req.Header.Set("Authorization", authHeader(1, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid portfolio ID, got %d: %s", w.Code, w.Body.String())
	}
}

// TestDeletePortfolio_NotFound verifies that DELETE /portfolios/:id for a non-existent
// portfolio returns 404.
func TestDeletePortfolio_NotFound(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("DELETE", "/portfolios/999999999", nil)
	req.Header.Set("Authorization", authHeader(1, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for non-existent portfolio, got %d: %s", w.Code, w.Body.String())
	}
}

// TestDeletePortfolio_Unauthorized verifies that DELETE /portfolios/:id by a non-owner
// returns 401.
func TestDeletePortfolio_Unauthorized(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	const ownerID = int64(1)
	const otherID = int64(2)
	name := nextPortfolioName()
	cleanupTestPortfolio(pool, name, ownerID)
	defer cleanupTestPortfolio(pool, name, ownerID)

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

	req, _ := http.NewRequest("DELETE", fmt.Sprintf("/portfolios/%d", created.Portfolio.ID), nil)
	req.Header.Set("Authorization", authHeader(otherID, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for unauthorized delete, got %d: %s", w.Code, w.Body.String())
	}
}

// TestUpdatePortfolio_NoAuth verifies that PUT /portfolios/:id without X-User-ID returns 401
// with error="unauthorized" and message="authentication required".
func TestUpdatePortfolio_NoAuth(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	body, _ := json.Marshal(map[string]any{"name": "whatever"})
	req, _ := http.NewRequest("PUT", "/portfolios/1", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	// Deliberately omit X-User-ID

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for missing auth header, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != "unauthorized" {
		t.Errorf("expected error='unauthorized', got %q", resp.Error)
	}
}

// TestCreatePortfolio_InvalidType verifies that POST /portfolios with an unrecognised
// portfolio_type returns 400.
func TestCreatePortfolio_InvalidType(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	body, _ := json.Marshal(map[string]any{
		"portfolio_type": "INVALID",
		"objective":      string(models.ObjectiveGrowth),
		"name":           nextPortfolioName(),
		"owner_id":       1,
		"memberships":    []any{},
	})
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid portfolio_type, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != "bad_request" {
		t.Errorf("expected error='bad_request', got %q", resp.Error)
	}
}

// TestCreatePortfolio_InvalidObjective verifies that POST /portfolios with an unrecognised
// objective returns 400.
func TestCreatePortfolio_InvalidObjective(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	body, _ := json.Marshal(map[string]any{
		"portfolio_type": string(models.PortfolioTypeIdeal),
		"objective":      "INVALID_OBJECTIVE",
		"name":           nextPortfolioName(),
		"owner_id":       1,
		"memberships":    []any{},
	})
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid objective, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != "bad_request" {
		t.Errorf("expected error='bad_request', got %q", resp.Error)
	}
}

// TestUpdatePortfolio_InvalidObjective verifies that PUT /portfolios/:id with an invalid
// objective value returns 400 before any portfolio lookup is performed.
func TestUpdatePortfolio_InvalidObjective(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Send a valid integer ID with auth and an invalid objective.
	// Objective validation fires before UpdatePortfolio is called, so the
	// portfolio doesn't need to exist.
	body, _ := json.Marshal(map[string]any{"objective": "INVALID_OBJECTIVE"})
	req, _ := http.NewRequest("PUT", "/portfolios/1", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader(1, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid objective in update, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != "bad_request" {
		t.Errorf("expected error='bad_request', got %q", resp.Error)
	}
}

// TestUpdatePortfolio_InvalidUserIDFormat verifies that an invalid Authorization header
// causes the middleware to skip setting the user, resulting in 401 from the handler.
func TestUpdatePortfolio_InvalidUserIDFormat(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	body, _ := json.Marshal(map[string]any{"name": "whatever"})
	req, _ := http.NewRequest("PUT", "/portfolios/1", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer not-a-valid-jwt")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for non-numeric X-User-ID, got %d: %s", w.Code, w.Body.String())
	}
}

// TestUpdatePortfolio_CSVBody verifies that sending a CSV-looking body (non-JSON) to PUT
// /portfolios/:id returns 400 with a helpful error message.
func TestUpdatePortfolio_CSVBody(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("PUT", "/portfolios/1", bytes.NewBufferString("TICKER,SHARES\nAAPL,5"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader(1, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for CSV body, got %d: %s", w.Code, w.Body.String())
	}
}

// TestUpdatePortfolio_WhitespaceCSVBody verifies that a body with leading whitespace
// followed by non-JSON content covers the whitespace-skip branch in detectCSVBody.
func TestUpdatePortfolio_WhitespaceCSVBody(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("PUT", "/portfolios/1", bytes.NewBufferString("\n\tTICKER,SHARES\nAAPL,5"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader(1, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for whitespace+CSV body, got %d: %s", w.Code, w.Body.String())
	}
}

// TestUpdatePortfolio_EmptyBody verifies that an empty request body triggers the
// ShouldBindJSON error path after detectCSVBody returns nil for empty input.
func TestUpdatePortfolio_EmptyBody(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("PUT", "/portfolios/1", bytes.NewBuffer([]byte{}))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader(1, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty body, got %d: %s", w.Code, w.Body.String())
	}
}

// TestUpdatePortfolio_SyntaxError verifies that a JSON syntax error in the request body
// returns 400, covering the *json.SyntaxError branch in jsonBindError.
func TestUpdatePortfolio_SyntaxError(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("PUT", "/portfolios/1", bytes.NewBufferString(`{"name": }`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader(1, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for JSON syntax error, got %d: %s", w.Code, w.Body.String())
	}
}

// TestUpdatePortfolio_TypeMismatch verifies that a JSON type mismatch returns 400,
// covering the *json.UnmarshalTypeError branch in jsonBindError.
func TestUpdatePortfolio_TypeMismatch(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	req, _ := http.NewRequest("PUT", "/portfolios/1", bytes.NewBufferString(`{"memberships": "not-an-array"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader(1, "USER"))

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for type mismatch, got %d: %s", w.Code, w.Body.String())
	}
}

// TestCreatePortfolio_MultipartMissingType verifies that a multipart POST without
// portfolio_type in the metadata JSON returns 400.
func TestCreatePortfolio_MultipartMissingType(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("metadata", `{"objective":"growth","name":"test-mp","owner_id":1}`)
	mw.Close()

	req, _ := http.NewRequest("POST", "/portfolios", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing portfolio_type, got %d: %s", w.Code, w.Body.String())
	}
}

// TestCreatePortfolio_MultipartMissingObjective verifies that a multipart POST without
// objective in the metadata JSON returns 400.
func TestCreatePortfolio_MultipartMissingObjective(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("metadata", `{"portfolio_type":"Ideal","name":"test-mp","owner_id":1}`)
	mw.Close()

	req, _ := http.NewRequest("POST", "/portfolios", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing objective, got %d: %s", w.Code, w.Body.String())
	}
}

// TestCreatePortfolio_MultipartMissingName verifies that a multipart POST without
// name in the metadata JSON returns 400.
func TestCreatePortfolio_MultipartMissingName(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("metadata", `{"portfolio_type":"Ideal","objective":"growth","owner_id":1}`)
	mw.Close()

	req, _ := http.NewRequest("POST", "/portfolios", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing name, got %d: %s", w.Code, w.Body.String())
	}
}

// TestCreatePortfolio_MultipartMissingOwnerID verifies that a multipart POST without
// owner_id in the metadata JSON returns 400.
func TestCreatePortfolio_MultipartMissingOwnerID(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupTestRouter(pool)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("metadata", `{"portfolio_type":"Ideal","objective":"growth","name":"test-mp"}`)
	mw.Close()

	req, _ := http.NewRequest("POST", "/portfolios", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing owner_id, got %d: %s", w.Code, w.Body.String())
	}
}

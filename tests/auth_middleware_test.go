package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/epeers/portfolio/internal/middleware"
	"github.com/gin-gonic/gin"
)

// setupAuthRouter returns a router with ValidateUser + RequireAuth protecting GET /protected.
func setupAuthRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.ValidateUser())
	r.GET("/protected", middleware.RequireAuth(), func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	return r
}

// TestRequireAuthMissingHeader verifies that a request without X-User-ID is rejected with 401.
func TestRequireAuthMissingHeader(t *testing.T) {
	router := setupAuthRouter()

	req, _ := http.NewRequest("GET", "/protected", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized, got %d: %s", w.Code, w.Body.String())
	}
}

// TestRequireAuthInvalidHeader verifies that a non-numeric X-User-ID is rejected with 401.
// ValidateUser silently ignores malformed IDs so the context has no user — RequireAuth
// should then block the request.
func TestRequireAuthInvalidHeader(t *testing.T) {
	router := setupAuthRouter()

	req, _ := http.NewRequest("GET", "/protected", nil)
	req.Header.Set("X-User-ID", "not-a-number")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized for invalid X-User-ID, got %d: %s", w.Code, w.Body.String())
	}
}

// TestRequireAuthValidHeader verifies that a valid numeric X-User-ID passes through RequireAuth.
func TestRequireAuthValidHeader(t *testing.T) {
	router := setupAuthRouter()

	req, _ := http.NewRequest("GET", "/protected", nil)
	req.Header.Set("X-User-ID", "42")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 OK for valid X-User-ID, got %d: %s", w.Code, w.Body.String())
	}
}

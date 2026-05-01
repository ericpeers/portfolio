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

// uniqueEmail returns a unique email address for test isolation.
func uniqueEmail(prefix string) string {
	return fmt.Sprintf("%s_%s@test.example", prefix, nextPortfolioName())
}

// setupAuthTestRouter builds a full auth+portfolio router for auth integration tests.
func setupAuthTestRouter(pool *pgxpool.Pool) *gin.Engine {
	gin.SetMode(gin.TestMode)

	userRepo := repository.NewUserRepository(pool)
	authSvc := services.NewAuthService(userRepo, testJWTSecret)
	authHandler := handlers.NewAuthHandler(authSvc)

	portfolioRepo := repository.NewPortfolioRepository(pool)
	securityRepo := repository.NewSecurityRepository(pool)
	portfolioSvc := services.NewPortfolioService(portfolioRepo, securityRepo)
	portfolioHandler := handlers.NewPortfolioHandler(portfolioSvc)

	router := gin.New()
	router.Use(middleware.ValidateUser([]byte(testJWTSecret)))

	auth := router.Group("/auth")
	auth.POST("/register", authHandler.Register)
	auth.POST("/login", authHandler.Login)
	auth.GET("/me", middleware.RequireAuth(), authHandler.Me)

	router.GET("/portfolios/:id", middleware.RequireAuth(), portfolioHandler.Get)

	admin := router.Group("/admin")
	admin.Use(middleware.RequireAuth(), middleware.RequireAdmin())
	admin.GET("/users/pending", authHandler.ListPending)
	admin.PATCH("/users/:id/approve", authHandler.Approve)

	return router
}

// cleanupAuthTestUser removes a test user by email. Safe to call if user doesn't exist.
func cleanupAuthTestUser(pool *pgxpool.Pool, email string) {
	pool.Exec(context.Background(), `DELETE FROM dim_user WHERE email = $1`, email)
}

// registerUser is a helper that POSTs to /auth/register and returns the response.
func registerUser(router *gin.Engine, name, email, password string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]any{"name": name, "email": email, "password": password})
	req, _ := http.NewRequest("POST", "/auth/register", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// loginUser is a helper that POSTs to /auth/login and returns the response.
func loginUser(router *gin.Engine, email, password string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]any{"email": email, "password": password})
	req, _ := http.NewRequest("POST", "/auth/login", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// approveUser approves a user by ID using the admin token.
func approveUser(router *gin.Engine, userID int64, adminToken string) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("/admin/users/%d/approve", userID), nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// TestRegister_HappyPath verifies that a valid registration creates a user in the DB
// with is_approved=false and a hashed (not plaintext) password.
func TestRegister_HappyPath(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	email := uniqueEmail("reg_happy")
	defer cleanupAuthTestUser(pool, email)

	w := registerUser(router, "Test Reg", email, "password123")
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp models.UserDTO
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Email != email {
		t.Errorf("expected email %q, got %q", email, resp.Email)
	}
	if resp.IsApproved {
		t.Error("new user should not be approved")
	}
	if resp.Role != "USER" {
		t.Errorf("expected role USER, got %q", resp.Role)
	}

	// Verify password is hashed in DB — must not equal plaintext
	var storedHash string
	err := pool.QueryRow(context.Background(),
		`SELECT passwd FROM dim_user WHERE email = $1`, email).Scan(&storedHash)
	if err != nil {
		t.Fatalf("DB query: %v", err)
	}
	if storedHash == "password123" {
		t.Error("password should be hashed, not stored in plaintext")
	}
	if len(storedHash) < 20 {
		t.Errorf("stored hash looks too short: %q", storedHash)
	}
}

// TestRegister_DuplicateEmail verifies that registering the same email twice returns 409.
func TestRegister_DuplicateEmail(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	email := uniqueEmail("reg_dup")
	defer cleanupAuthTestUser(pool, email)

	if w := registerUser(router, "First", email, "password123"); w.Code != http.StatusCreated {
		t.Fatalf("first register: expected 201, got %d: %s", w.Code, w.Body.String())
	}

	w := registerUser(router, "Second", email, "password456")
	if w.Code != http.StatusConflict {
		t.Errorf("expected 409 for duplicate email, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Error != "conflict" {
		t.Errorf("expected error='conflict', got %q", resp.Error)
	}
}

// TestRegister_InvalidBody verifies that a missing required field returns 400.
func TestRegister_InvalidBody(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	// Missing password field
	body, _ := json.Marshal(map[string]any{"name": "Test", "email": "x@test.example"})
	req, _ := http.NewRequest("POST", "/auth/register", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing password, got %d: %s", w.Code, w.Body.String())
	}
}

// TestLogin_BeforeApproval verifies that a registered-but-not-approved user gets 403.
func TestLogin_BeforeApproval(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	email := uniqueEmail("login_pending")
	defer cleanupAuthTestUser(pool, email)

	registerUser(router, "Pending", email, "password123")

	w := loginUser(router, email, "password123")
	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403 for unapproved user, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.ErrorResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Error != "forbidden" {
		t.Errorf("expected error='forbidden', got %q", resp.Error)
	}
}

// TestLogin_InvalidEmail verifies that an unknown email returns 401.
func TestLogin_InvalidEmail(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	w := loginUser(router, "nobody@test.example", "password123")
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for unknown email, got %d: %s", w.Code, w.Body.String())
	}
}

// TestLogin_WrongPassword verifies that a correct email with wrong password returns 401.
func TestLogin_WrongPassword(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	email := uniqueEmail("login_wrongpw")
	defer cleanupAuthTestUser(pool, email)

	registerUser(router, "Wrong PW", email, "password123")
	// Approve directly so we can reach the password check
	pool.Exec(context.Background(),
		`UPDATE dim_user SET is_approved = TRUE WHERE email = $1`, email)

	w := loginUser(router, email, "wrongpassword")
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong password, got %d: %s", w.Code, w.Body.String())
	}
}

// TestLogin_HappyPath verifies that a registered+approved user gets 200 with a non-empty token.
func TestLogin_HappyPath(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	email := uniqueEmail("login_ok")
	defer cleanupAuthTestUser(pool, email)

	registerUser(router, "Login OK", email, "password123")
	pool.Exec(context.Background(),
		`UPDATE dim_user SET is_approved = TRUE WHERE email = $1`, email)

	w := loginUser(router, email, "password123")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp models.AuthResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Token == "" {
		t.Error("expected non-empty JWT token in response")
	}
}

// TestMe_WithToken verifies that GET /auth/me with a valid token returns 200 UserDTO.
func TestMe_WithToken(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	email := uniqueEmail("me_ok")
	defer cleanupAuthTestUser(pool, email)

	registerUser(router, "Me Test", email, "password123")
	pool.Exec(context.Background(),
		`UPDATE dim_user SET is_approved = TRUE WHERE email = $1`, email)

	lw := loginUser(router, email, "password123")
	var loginResp models.AuthResponse
	json.Unmarshal(lw.Body.Bytes(), &loginResp)

	req, _ := http.NewRequest("GET", "/auth/me", nil)
	req.Header.Set("Authorization", "Bearer "+loginResp.Token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var user models.UserDTO
	json.Unmarshal(w.Body.Bytes(), &user)
	if user.Email != email {
		t.Errorf("expected email %q, got %q", email, user.Email)
	}
}

// TestMe_NoToken verifies that GET /auth/me without a token returns 401.
func TestMe_NoToken(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	req, _ := http.NewRequest("GET", "/auth/me", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
}

// TestProtectedRoute_NoToken verifies that a protected portfolio route returns 401 without a token.
func TestProtectedRoute_NoToken(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	req, _ := http.NewRequest("GET", "/portfolios/1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for protected route without token, got %d: %s", w.Code, w.Body.String())
	}
}

// TestAdminListPending_AsUser verifies that a non-admin token gets 403 on admin endpoints.
func TestAdminListPending_AsUser(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	req, _ := http.NewRequest("GET", "/admin/users/pending", nil)
	req.Header.Set("Authorization", authHeader(999, "USER"))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("expected 403 for non-admin, got %d: %s", w.Code, w.Body.String())
	}
}

// TestAdminListPending_AsAdmin verifies that an ADMIN token gets 200 with a JSON array.
func TestAdminListPending_AsAdmin(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	req, _ := http.NewRequest("GET", "/admin/users/pending", nil)
	req.Header.Set("Authorization", authHeader(1, "ADMIN"))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var users []models.PendingUser
	if err := json.Unmarshal(w.Body.Bytes(), &users); err != nil {
		t.Fatalf("expected JSON array in response: %v", err)
	}
}

// TestApproveUser_ThenLogin exercises the full flow:
// register → list pending (admin) → approve → second user can now log in.
func TestApproveUser_ThenLogin(t *testing.T) {
	t.Parallel()
	pool := getTestPool(t)
	router := setupAuthTestRouter(pool)

	email := uniqueEmail("approve_flow")
	defer cleanupAuthTestUser(pool, email)

	// Step 1: Register
	rw := registerUser(router, "New User", email, "password123")
	if rw.Code != http.StatusCreated {
		t.Fatalf("register: expected 201, got %d: %s", rw.Code, rw.Body.String())
	}
	var regResp models.UserDTO
	json.Unmarshal(rw.Body.Bytes(), &regResp)

	// Step 2: Login should fail (not approved)
	if lw := loginUser(router, email, "password123"); lw.Code != http.StatusForbidden {
		t.Fatalf("before approval: expected 403, got %d", lw.Code)
	}

	// Step 3: Admin approves the user
	// Use a direct JWT for user 1 (seeded ADMIN) — no need to login as admin here
	aw := approveUser(router, regResp.ID, makeTestToken(1, "ADMIN"))
	if aw.Code != http.StatusNoContent {
		t.Fatalf("approve: expected 204, got %d: %s", aw.Code, aw.Body.String())
	}

	// Step 4: Login should now succeed
	lw := loginUser(router, email, "password123")
	if lw.Code != http.StatusOK {
		t.Fatalf("after approval: expected 200, got %d: %s", lw.Code, lw.Body.String())
	}
	var loginResp models.AuthResponse
	json.Unmarshal(lw.Body.Bytes(), &loginResp)
	if loginResp.Token == "" {
		t.Error("expected non-empty token after approval")
	}
}


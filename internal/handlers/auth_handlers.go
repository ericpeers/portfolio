package handlers

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/epeers/portfolio/internal/middleware"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
)

type AuthHandler struct {
	authSvc *services.AuthService
}

func NewAuthHandler(authSvc *services.AuthService) *AuthHandler {
	return &AuthHandler{authSvc: authSvc}
}

// Register creates a new user account. The account is inactive until an admin approves it.
// @Summary Register a new user
// @Description Creates an unapproved user account. The user cannot log in until an admin approves them.
// @Tags auth
// @Accept json
// @Produce json
// @Param request body models.RegisterRequest true "Registration details"
// @Success 201 {object} models.UserDTO
// @Failure 400 {object} models.ErrorResponse
// @Failure 409 {object} models.ErrorResponse "Email already registered"
// @Failure 500 {object} models.ErrorResponse
// @Router /auth/register [post]
func (h *AuthHandler) Register(c *gin.Context) {
	var req models.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "bad_request", Message: err.Error()})
		return
	}

	user, err := h.authSvc.Register(c.Request.Context(), req)
	if err != nil {
		if errors.Is(err, repository.ErrEmailTaken) {
			c.JSON(http.StatusConflict, models.ErrorResponse{Error: "conflict", Message: "email already registered"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "internal_error", Message: err.Error()})
		return
	}

	c.JSON(http.StatusCreated, user)
}

// Login authenticates a user and returns a signed JWT on success.
// @Summary Log in
// @Description Verifies credentials and returns a Bearer token. The account must be approved before login is permitted.
// @Tags auth
// @Accept json
// @Produce json
// @Param request body models.LoginRequest true "Email and password"
// @Success 200 {object} models.AuthResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 401 {object} models.ErrorResponse "Invalid credentials"
// @Failure 403 {object} models.ErrorResponse "Account pending approval"
// @Failure 500 {object} models.ErrorResponse
// @Router /auth/login [post]
func (h *AuthHandler) Login(c *gin.Context) {
	var req models.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "bad_request", Message: err.Error()})
		return
	}

	resp, err := h.authSvc.Login(c.Request.Context(), req)
	if err != nil {
		if errors.Is(err, services.ErrInvalidCredentials) {
			c.JSON(http.StatusUnauthorized, models.ErrorResponse{Error: "unauthorized", Message: "invalid email or password"})
			return
		}
		if errors.Is(err, services.ErrNotApproved) {
			c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "forbidden", Message: "account pending approval"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "internal_error", Message: err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// Me returns the profile of the currently authenticated user.
// @Summary Get current user
// @Description Returns the UserDTO for the user identified by the Bearer token.
// @Tags auth
// @Produce json
// @Security BearerAuth
// @Success 200 {object} models.UserDTO
// @Failure 401 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /auth/me [get]
func (h *AuthHandler) Me(c *gin.Context) {
	userID, _ := middleware.GetUserID(c)
	user, err := h.authSvc.GetUserByID(c.Request.Context(), userID)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "not_found", Message: "user not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "internal_error", Message: err.Error()})
		return
	}
	c.JSON(http.StatusOK, user)
}

// ListPending returns all user accounts that are awaiting admin approval.
// @Summary List pending users (admin)
// @Description Returns users with is_approved=false, ordered by registration date. Requires ADMIN role.
// @Tags auth
// @Produce json
// @Security BearerAuth
// @Success 200 {array} models.PendingUser
// @Failure 401 {object} models.ErrorResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/users/pending [get]
func (h *AuthHandler) ListPending(c *gin.Context) {
	users, err := h.authSvc.ListPending(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "internal_error", Message: err.Error()})
		return
	}
	if users == nil {
		users = []models.PendingUser{}
	}
	c.JSON(http.StatusOK, users)
}

// Approve grants a pending user access to the application.
// @Summary Approve a user (admin)
// @Description Sets is_approved=true for the given user ID, allowing them to log in. Requires ADMIN role.
// @Tags auth
// @Produce json
// @Security BearerAuth
// @Param id path int true "User ID"
// @Success 204
// @Failure 400 {object} models.ErrorResponse
// @Failure 401 {object} models.ErrorResponse
// @Failure 403 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse "User not found"
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/users/{id}/approve [patch]
func (h *AuthHandler) Approve(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "bad_request", Message: "invalid user ID"})
		return
	}

	if err := h.authSvc.Approve(c.Request.Context(), id); err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "not_found", Message: "user not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "internal_error", Message: err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

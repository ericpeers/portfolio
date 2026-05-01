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

// GlanceHandler handles the portfolio glance endpoints.
type GlanceHandler struct {
	glanceSvc *services.GlanceService
}

// NewGlanceHandler creates a new GlanceHandler.
func NewGlanceHandler(glanceSvc *services.GlanceService) *GlanceHandler {
	return &GlanceHandler{glanceSvc: glanceSvc}
}

// Add handles POST /users/:user_id/glance
// @Summary Pin a portfolio to the user's glance list
// @Tags glance
// @Accept json
// @Produce json
// @Param user_id path int true "User ID"
// @Param body body models.AddGlanceRequest true "Portfolio to pin"
// @Success 201 {object} map[string]interface{}
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /users/{user_id}/glance [post]
func (h *GlanceHandler) Add(c *gin.Context) {
	userID, err := strconv.ParseInt(c.Param("user_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "bad_request", Message: "invalid user_id"})
		return
	}

	if authUserID, ok := middleware.GetUserID(c); ok && authUserID != userID {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "forbidden", Message: "access denied"})
		return
	}

	var req models.AddGlanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "bad_request", Message: err.Error()})
		return
	}

	inserted, err := h.glanceSvc.Add(c.Request.Context(), userID, req.PortfolioID)
	if err != nil {
		if errors.Is(err, services.ErrPortfolioNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "not_found", Message: "portfolio not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "internal_error", Message: err.Error()})
		return
	}

	if inserted {
		c.JSON(http.StatusCreated, gin.H{"portfolio_id": req.PortfolioID, "added": true})
	} else {
		c.JSON(http.StatusOK, gin.H{"portfolio_id": req.PortfolioID, "added": false})
	}
}

// Remove handles DELETE /users/:user_id/glance/:portfolio_id
// @Summary Unpin a portfolio from the user's glance list
// @Tags glance
// @Param user_id path int true "User ID"
// @Param portfolio_id path int true "Portfolio ID"
// @Success 204
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /users/{user_id}/glance/{portfolio_id} [delete]
func (h *GlanceHandler) Remove(c *gin.Context) {
	userID, err := strconv.ParseInt(c.Param("user_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "bad_request", Message: "invalid user_id"})
		return
	}

	if authUserID, ok := middleware.GetUserID(c); ok && authUserID != userID {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "forbidden", Message: "access denied"})
		return
	}

	portfolioID, err := strconv.ParseInt(c.Param("portfolio_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "bad_request", Message: "invalid portfolio_id"})
		return
	}

	if err := h.glanceSvc.Remove(c.Request.Context(), userID, portfolioID); err != nil {
		if errors.Is(err, repository.ErrGlanceEntryNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{Error: "not_found", Message: "glance entry not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "internal_error", Message: err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

// List handles GET /users/:user_id/glance
// @Summary List pinned portfolios with key metrics
// @Tags glance
// @Produce json
// @Param user_id path int true "User ID"
// @Param missing_data_strategy query string false "Pre-IPO gap strategy: empty (constrain start date), cash_flat, cash_appreciating, or reallocate"
// @Success 200 {object} models.GlanceListResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /users/{user_id}/glance [get]
func (h *GlanceHandler) List(c *gin.Context) {
	userID, err := strconv.ParseInt(c.Param("user_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Error: "bad_request", Message: "invalid user_id"})
		return
	}

	if authUserID, ok := middleware.GetUserID(c); ok && authUserID != userID {
		c.JSON(http.StatusForbidden, models.ErrorResponse{Error: "forbidden", Message: "access denied"})
		return
	}

	strategy := models.MissingDataStrategy(c.Query("missing_data_strategy"))
	portfolios, err := h.glanceSvc.List(c.Request.Context(), userID, strategy)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Error: "internal_error", Message: err.Error()})
		return
	}

	if portfolios == nil {
		portfolios = []models.GlancePortfolio{}
	}
	c.JSON(http.StatusOK, models.GlanceListResponse{Portfolios: portfolios})
}

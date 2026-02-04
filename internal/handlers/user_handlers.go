package handlers

import (
	"net/http"
	"strconv"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
)

// UserHandler handles user-related endpoints
type UserHandler struct {
	portfolioSvc *services.PortfolioService
}

// NewUserHandler creates a new UserHandler
func NewUserHandler(portfolioSvc *services.PortfolioService) *UserHandler {
	return &UserHandler{
		portfolioSvc: portfolioSvc,
	}
}

// ListPortfolios handles GET /users/:user_id/portfolios
// @Summary List user's portfolios
// @Description Get all portfolios belonging to a user
// @Tags users
// @Produce json
// @Param user_id path int true "User ID"
// @Success 200 {array} models.PortfolioListItem
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /users/{user_id}/portfolios [get]
func (h *UserHandler) ListPortfolios(c *gin.Context) {
	userIDStr := c.Param("user_id")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: "invalid user ID",
		})
		return
	}

	portfolios, err := h.portfolioSvc.GetUserPortfolios(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	// Return empty array if no portfolios
	if portfolios == nil {
		portfolios = []models.PortfolioListItem{}
	}

	c.JSON(http.StatusOK, portfolios)
}

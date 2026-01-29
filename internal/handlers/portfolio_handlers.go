package handlers

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/epeers/portfolio/internal/middleware"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
)

// PortfolioHandler handles portfolio CRUD endpoints
type PortfolioHandler struct {
	portfolioSvc *services.PortfolioService
}

// NewPortfolioHandler creates a new PortfolioHandler
func NewPortfolioHandler(portfolioSvc *services.PortfolioService) *PortfolioHandler {
	return &PortfolioHandler{
		portfolioSvc: portfolioSvc,
	}
}

// Create handles POST /portfolios
func (h *PortfolioHandler) Create(c *gin.Context) {
	var req models.CreatePortfolioRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: err.Error(),
		})
		return
	}

	// Validate portfolio type
	if req.PortfolioType != models.PortfolioTypeIdeal && req.PortfolioType != models.PortfolioTypeActive {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: "portfolio_type must be 'Ideal' or 'Active'",
		})
		return
	}

	portfolio, err := h.portfolioSvc.CreatePortfolio(c.Request.Context(), &req)
	if err != nil {
		if errors.Is(err, services.ErrConflict) {
			c.JSON(http.StatusConflict, models.ErrorResponse{
				Error:   "conflict",
				Message: "portfolio with same name and type already exists",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, portfolio)
}

// Get handles GET /portfolios/:id
func (h *PortfolioHandler) Get(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: "invalid portfolio ID",
		})
		return
	}

	portfolio, err := h.portfolioSvc.GetPortfolio(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, services.ErrPortfolioNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error:   "not_found",
				Message: "portfolio not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, portfolio)
}

// Update handles PUT /portfolios/:id
func (h *PortfolioHandler) Update(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: "invalid portfolio ID",
		})
		return
	}

	userID, ok := middleware.GetUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, models.ErrorResponse{
			Error:   "unauthorized",
			Message: "authentication required",
		})
		return
	}

	var req models.UpdatePortfolioRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: err.Error(),
		})
		return
	}

	portfolio, err := h.portfolioSvc.UpdatePortfolio(c.Request.Context(), id, userID, &req)
	if err != nil {
		if errors.Is(err, services.ErrPortfolioNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error:   "not_found",
				Message: "portfolio not found",
			})
			return
		}
		if errors.Is(err, services.ErrUnauthorized) {
			c.JSON(http.StatusUnauthorized, models.ErrorResponse{
				Error:   "unauthorized",
				Message: "not authorized to modify this portfolio",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, portfolio)
}

// Delete handles DELETE /portfolios/:id
func (h *PortfolioHandler) Delete(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: "invalid portfolio ID",
		})
		return
	}

	userID, ok := middleware.GetUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, models.ErrorResponse{
			Error:   "unauthorized",
			Message: "authentication required",
		})
		return
	}

	err = h.portfolioSvc.DeletePortfolio(c.Request.Context(), id, userID)
	if err != nil {
		if errors.Is(err, services.ErrPortfolioNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error:   "not_found",
				Message: "portfolio not found",
			})
			return
		}
		if errors.Is(err, services.ErrUnauthorized) {
			c.JSON(http.StatusUnauthorized, models.ErrorResponse{
				Error:   "unauthorized",
				Message: "not authorized to delete this portfolio",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "portfolio deleted"})
}

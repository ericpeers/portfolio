package handlers

import (
	"errors"
	"net/http"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
)

// CompareHandler handles portfolio comparison endpoints
type CompareHandler struct {
	comparisonSvc *services.ComparisonService
}

// NewCompareHandler creates a new CompareHandler
func NewCompareHandler(comparisonSvc *services.ComparisonService) *CompareHandler {
	return &CompareHandler{
		comparisonSvc: comparisonSvc,
	}
}

// Compare handles POST /portfolios/compare
// @Summary Compare two portfolios
// @Description Compare two portfolios over a time period, showing membership differences and performance metrics
// @Tags portfolios
// @Accept json
// @Produce json
// @Param request body models.CompareRequest true "Comparison parameters"
// @Success 200 {object} models.CompareResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /portfolios/compare [post]
func (h *CompareHandler) Compare(c *gin.Context) {
	var req models.CompareRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: err.Error(),
		})
		return
	}

	// If EndPeriod is just a date with no time, set it to the end of that day
	if req.EndPeriod.Hour() == 0 && req.EndPeriod.Minute() == 0 && req.EndPeriod.Second() == 0 {
		req.EndPeriod.Time = req.EndPeriod.Time.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	}

	// Validate time range
	if req.EndPeriod.Before(req.StartPeriod.Time) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: "end_period must be after start_period",
		})
		return
	}

	result, err := h.comparisonSvc.ComparePortfolios(c.Request.Context(), &req)
	if err != nil {
		if errors.Is(err, services.ErrPortfolioNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error:   "not_found",
				Message: "one or both portfolios not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

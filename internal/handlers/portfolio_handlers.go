package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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
// @Summary Create a new portfolio
// @Description Create a new portfolio with optional memberships. Accepts JSON or multipart/form-data with a CSV file.
// @Tags portfolios
// @Accept json,mpfd
// @Produce json
// @Param portfolio body models.CreatePortfolioRequest false "Portfolio to create (JSON)"
// @Param metadata formData string false "Portfolio metadata as JSON (multipart)"
// @Param memberships formData file false "CSV file with ticker,percentage_or_shares columns (multipart)"
// @Success 201 {object} models.PortfolioWithMemberships
// @Failure 400 {object} models.ErrorResponse
// @Failure 409 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /portfolios [post]
func (h *PortfolioHandler) Create(c *gin.Context) {
	req, err := h.bindCreateRequest(c)
	if err != nil {
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

	// Validate objective
	if err := services.ValidateObjective(req.Objective); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: err.Error(),
		})
		return
	}

	portfolio, err := h.portfolioSvc.CreatePortfolio(c.Request.Context(), req)
	if err != nil {
		if errors.Is(err, services.ErrInvalidMembership) || errors.Is(err, services.ErrInvalidIdealPercentage) || errors.Is(err, services.ErrIdealTotalExceedsOne) || errors.Is(err, services.ErrInvalidObjective) {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error:   "bad_request",
				Message: err.Error(),
			})
			return
		}
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
// @Summary Get a portfolio by ID
// @Description Retrieve a portfolio and its memberships by ID
// @Tags portfolios
// @Produce json
// @Param id path int true "Portfolio ID"
// @Success 200 {object} models.PortfolioWithMemberships
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /portfolios/{id} [get]
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
// @Summary Update a portfolio
// @Description Update a portfolio's name and/or memberships. Accepts JSON or multipart/form-data with a CSV file.
// @Tags portfolios
// @Accept json,mpfd
// @Produce json
// @Param id path int true "Portfolio ID"
// @Param portfolio body models.UpdatePortfolioRequest false "Portfolio updates (JSON)"
// @Param metadata formData string false "Portfolio metadata as JSON (multipart)"
// @Param memberships formData file false "CSV file with ticker,percentage_or_shares columns (multipart)"
// @Success 200 {object} models.PortfolioWithMemberships
// @Failure 400 {object} models.ErrorResponse
// @Failure 401 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security UserID
// @Router /portfolios/{id} [put]
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

	req, err := h.bindUpdateRequest(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "bad_request",
			Message: err.Error(),
		})
		return
	}

	// Validate objective if provided
	if req.Objective != nil {
		if err := services.ValidateObjective(*req.Objective); err != nil {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error:   "bad_request",
				Message: err.Error(),
			})
			return
		}
	}

	portfolio, err := h.portfolioSvc.UpdatePortfolio(c.Request.Context(), id, userID, req)
	if err != nil {
		if errors.Is(err, services.ErrInvalidMembership) || errors.Is(err, services.ErrInvalidIdealPercentage) || errors.Is(err, services.ErrIdealTotalExceedsOne) || errors.Is(err, services.ErrInvalidObjective) {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error:   "bad_request",
				Message: err.Error(),
			})
			return
		}
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
// @Summary Delete a portfolio
// @Description Delete a portfolio by ID
// @Tags portfolios
// @Produce json
// @Param id path int true "Portfolio ID"
// @Success 200 {object} map[string]string
// @Failure 400 {object} models.ErrorResponse
// @Failure 401 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Security UserID
// @Router /portfolios/{id} [delete]
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

func (h *PortfolioHandler) bindCreateRequest(c *gin.Context) (*models.CreatePortfolioRequest, error) {
	ct := c.ContentType()
	if strings.HasPrefix(ct, "multipart/") {
		return h.bindCreateFromMultipart(c)
	}
	var req models.CreatePortfolioRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		return nil, err
	}
	return &req, nil
}

func (h *PortfolioHandler) bindCreateFromMultipart(c *gin.Context) (*models.CreatePortfolioRequest, error) {
	metadata := c.PostForm("metadata")
	if metadata == "" {
		return nil, fmt.Errorf("metadata field is required")
	}

	var req models.CreatePortfolioRequest
	if err := json.Unmarshal([]byte(metadata), &req); err != nil {
		return nil, fmt.Errorf("invalid metadata JSON: %w", err)
	}

	// Manually validate required fields since binding tags don't apply with manual unmarshal
	if req.PortfolioType == "" {
		return nil, fmt.Errorf("portfolio_type is required")
	}
	if req.Objective == "" {
		return nil, fmt.Errorf("objective is required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if req.OwnerID == 0 {
		return nil, fmt.Errorf("owner_id is required")
	}

	fileHeader, err := c.FormFile("memberships")
	if err == nil {
		f, err := fileHeader.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open memberships file: %w", err)
		}
		defer f.Close()

		memberships, err := ParseMembershipCSV(f)
		if err != nil {
			return nil, err
		}
		req.Memberships = memberships
	}

	return &req, nil
}

func (h *PortfolioHandler) bindUpdateRequest(c *gin.Context) (*models.UpdatePortfolioRequest, error) {
	ct := c.ContentType()
	if strings.HasPrefix(ct, "multipart/") {
		return h.bindUpdateFromMultipart(c)
	}
	var req models.UpdatePortfolioRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		return nil, err
	}
	return &req, nil
}

func (h *PortfolioHandler) bindUpdateFromMultipart(c *gin.Context) (*models.UpdatePortfolioRequest, error) {
	var req models.UpdatePortfolioRequest

	metadata := c.PostForm("metadata")
	if metadata != "" {
		if err := json.Unmarshal([]byte(metadata), &req); err != nil {
			return nil, fmt.Errorf("invalid metadata JSON: %w", err)
		}
	}

	fileHeader, err := c.FormFile("memberships")
	if err == nil {
		f, err := fileHeader.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open memberships file: %w", err)
		}
		defer f.Close()

		memberships, err := ParseMembershipCSV(f)
		if err != nil {
			return nil, err
		}
		req.Memberships = memberships
	}

	return &req, nil
}

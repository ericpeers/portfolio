package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// AdminHandler handles admin endpoints
type AdminHandler struct {
	adminSvc      *services.AdminService
	pricingSvc    *services.PricingService
	membershipSvc *services.MembershipService
	secRepo       *repository.SecurityRepository
}

// NewAdminHandler creates a new AdminHandler
func NewAdminHandler(adminSvc *services.AdminService, pricingSvc *services.PricingService, membershipSvc *services.MembershipService, secRepo *repository.SecurityRepository) *AdminHandler {
	return &AdminHandler{
		adminSvc:      adminSvc,
		pricingSvc:    pricingSvc,
		membershipSvc: membershipSvc,
		secRepo:       secRepo,
	}
}

// SyncSecurities handles POST /admin/sync-securities
// @Summary Sync securities from AlphaVantage
// @Description Synchronize the securities database with AlphaVantage listing status
// @Tags admin
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/sync-securities [post]
func (h *AdminHandler) SyncSecurities(c *gin.Context) {
	result, err := h.adminSvc.SyncSecurities(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// GetDailyPrices handles GET /admin/get_daily_prices
// @Summary Get daily prices for a security
// @Description Fetch daily price data for a security by ticker or ID
// @Tags admin
// @Produce json
// @Param ticker query string false "Security ticker symbol"
// @Param security_id query int false "Security ID"
// @Param start_date query string true "Start date (YYYY-MM-DD)"
// @Param end_date query string true "End date (YYYY-MM-DD)"
// @Success 200 {object} models.GetDailyPricesResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/get_daily_prices [get]
func (h *AdminHandler) GetDailyPrices(c *gin.Context) {
	var req models.GetDailyPricesRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: err.Error(),
		})
		return
	}

	// Must have either ticker or security_id
	if req.Ticker == "" && req.SecurityID == 0 {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "must provide either ticker or security_id",
		})
		return
	}

	// Parse dates
	startDate, err := time.Parse("2006-01-02", req.StartDate)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "start_date must be in YYYY-MM-DD format",
		})
		return
	}

	endDate, err := time.Parse("2006-01-02", req.EndDate)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "end_date must be in YYYY-MM-DD format",
		})
		return
	}

	ctx := c.Request.Context()

	// Resolve ticker to security_id if needed
	securityID := req.SecurityID
	symbol := req.Ticker
	if req.Ticker != "" && req.SecurityID == 0 {
		security, err := h.secRepo.GetByTicker(ctx, req.Ticker)
		if err != nil {
			if err == repository.ErrSecurityNotFound {
				c.JSON(http.StatusNotFound, models.ErrorResponse{
					Error:   "not_found",
					Message: "security not found for ticker: " + req.Ticker,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{
				Error:   "internal_error",
				Message: err.Error(),
			})
			return
		}
		securityID = security.ID
		symbol = security.Symbol
	} else if req.SecurityID != 0 && req.Ticker == "" {
		// We have security_id but need symbol for response
		security, err := h.secRepo.GetByID(ctx, req.SecurityID)
		if err != nil {
			if err == repository.ErrSecurityNotFound {
				c.JSON(http.StatusNotFound, models.ErrorResponse{
					Error:   "not_found",
					Message: "security not found",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{
				Error:   "internal_error",
				Message: err.Error(),
			})
			return
		}
		symbol = security.Symbol
	}

	// Fetch prices
	//FIXME: return split data to the end user in the response — refactor later
	prices, _, err := h.pricingSvc.GetDailyPrices(ctx, securityID, startDate, endDate)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.GetDailyPricesResponse{
		SecurityID: securityID,
		Symbol:     symbol,
		StartDate:  req.StartDate,
		EndDate:    req.EndDate,
		DataPoints: len(prices),
		Prices:     prices,
	})
}

// GetETFHoldings handles GET /admin/get_etf_holdings
// @Summary Get ETF holdings
// @Description Fetch holdings for an ETF or mutual fund by ticker or ID
// @Tags admin
// @Produce json
// @Param ticker query string false "ETF ticker symbol"
// @Param security_id query int false "Security ID"
// @Success 200 {object} models.GetETFHoldingsResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/get_etf_holdings [get]
func (h *AdminHandler) GetETFHoldings(c *gin.Context) {
	var req models.GetETFHoldingsRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: err.Error(),
		})
		return
	}

	// Must have either ticker or security_id
	if req.Ticker == "" && req.SecurityID == 0 {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "must provide either ticker or security_id",
		})
		return
	}

	ctx := c.Request.Context()

	// Resolve ticker to security_id if needed
	var security *models.Security
	var err error
	if req.Ticker != "" {
		security, err = h.secRepo.GetByTicker(ctx, req.Ticker)
		if err != nil {
			if err == repository.ErrSecurityNotFound {
				c.JSON(http.StatusNotFound, models.ErrorResponse{
					Error:   "not_found",
					Message: "security not found for ticker: " + req.Ticker,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{
				Error:   "internal_error",
				Message: err.Error(),
			})
			return
		}
	} else {
		security, err = h.secRepo.GetByID(ctx, req.SecurityID)
		if err != nil {
			if err == repository.ErrSecurityNotFound {
				c.JSON(http.StatusNotFound, models.ErrorResponse{
					Error:   "not_found",
					Message: "security not found",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{
				Error:   "internal_error",
				Message: err.Error(),
			})
			return
		}
	}

	// Check if it's an ETF or mutual fund
	isETFOrMF, err := h.secRepo.IsETFOrMutualFund(ctx, security.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}
	if !isETFOrMF {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "security is not an ETF or mutual fund",
		})
		return
	}

	// Fetch holdings
	warnCtx, wc := services.NewWarningContext(ctx)
	holdings, pullDate, err := h.membershipSvc.GetETFHoldings(warnCtx, security.ID, security.Symbol, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	// Resolver chain: merge swaps into real equities, then handle special symbols
	resolved, unresolved := services.ResolveSwapHoldings(holdings)
	resolved2, unresolved2 := services.ResolveSpecialSymbols(unresolved)
	resolved = append(resolved, resolved2...)

	for _, uh := range unresolved2 {
		services.AddWarning(warnCtx, models.Warning{
			Code:    models.WarnUnresolvedETFHolding,
			Message: fmt.Sprintf("ETF %s: unresolved holding %q (weight %.4f)", security.Symbol, uh.Name, uh.Percentage),
		})
	}

	resolved = services.NormalizeHoldings(warnCtx, resolved, security.Symbol)

	// Persist resolved holdings if freshly fetched from AlphaVantage
	if pullDate == nil {
		if err := h.membershipSvc.PersistETFHoldings(ctx, security.ID, resolved, nil); err != nil {
			log.Errorf("Issue in saving ETF holdings: %s", err)
		}
	}

	// Build response
	var pullDateStr *string
	if pullDate != nil {
		s := pullDate.Format("2006-01-02")
		pullDateStr = &s
	}

	holdingsDTO := make([]models.ETFHoldingDTO, len(resolved))
	for i, h := range resolved {
		holdingsDTO[i] = models.ETFHoldingDTO{
			Symbol:     h.Symbol,
			Name:       h.Name,
			Percentage: h.Percentage,
		}
	}

	c.JSON(http.StatusOK, models.GetETFHoldingsResponse{
		SecurityID: security.ID,
		Symbol:     security.Symbol,
		Name:       security.Name,
		PullDate:   pullDateStr,
		Holdings:   holdingsDTO,
		Warnings:   wc.GetWarnings(),
	})
}

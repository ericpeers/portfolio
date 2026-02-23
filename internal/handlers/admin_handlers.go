package handlers

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// pickETFSecurity returns the first ETF or MutualFund from matches, falling
// back to matches[0] if none qualify.
func pickETFSecurity(matches []*models.SecurityWithCountry) *models.SecurityWithCountry {
	for _, m := range matches {
		if m.Type == string(models.SecurityTypeETF) || m.Type == string(models.SecurityTypeMutualFund) {
			return m
		}
	}
	return matches[0]
}

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
// @Description Synchronize the securities database with AlphaVantage listing status. Pass type=dryrun to simulate without writes.
// @Tags admin
// @Produce json
// @Param type query string false "Run mode: omit for live sync, 'dryrun' or 'dry_run' for simulation"
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/sync-securities [post]
func (h *AdminHandler) SyncSecurities(c *gin.Context) {
	ctx := c.Request.Context()

	switch strings.ToLower(strings.ReplaceAll(c.Query("type"), "_", "")) {
	case "dryrun":
		result, err := h.adminSvc.DryRunSyncSecurities(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{
				Error:   "internal_error",
				Message: err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, result)
	default:
		result, err := h.adminSvc.SyncSecurities(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{
				Error:   "internal_error",
				Message: err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, result)
	}
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

	prefetchedByID, prefetchedBySymbol, err := h.membershipSvc.GetAllSecurities(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	var security *models.Security
	if req.Ticker != "" {
		matches := prefetchedBySymbol[req.Ticker]
		if len(matches) == 0 {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error:   "not_found",
				Message: "security not found for ticker: " + req.Ticker,
			})
			return
		}
		sec := pickETFSecurity(matches).Security
		security = &sec
	} else {
		security = prefetchedByID[req.SecurityID]
		if security == nil {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error:   "not_found",
				Message: "security not found",
			})
			return
		}
	}

	if security.Type != string(models.SecurityTypeETF) && security.Type != string(models.SecurityTypeMutualFund) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "security is not an ETF or mutual fund",
		})
		return
	}

	warnCtx, wc := services.NewWarningContext(ctx)
	holdings, pullDate, err := h.membershipSvc.FetchOrRefreshETFHoldings(
		warnCtx, security.ID, security.Symbol, prefetchedByID, prefetchedBySymbol)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	var pullDateStr *string
	if pullDate != nil {
		s := pullDate.Format("2006-01-02")
		pullDateStr = &s
	}

	holdingsDTO := make([]models.ETFHoldingDTO, len(holdings))
	for i, h := range holdings {
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

// LoadETFHoldings handles POST /admin/load_etf_holdings
// @Summary Load ETF holdings from a CSV upload
// @Description Parse a CSV (Symbol,Company,Weight) and persist holdings for an ETF or mutual fund. Bypasses the postgres check: persists it always
// @Tags admin
// @Accept multipart/form-data
// @Produce json
// @Param ticker formData string false "ETF ticker symbol"
// @Param security_id formData int false "Security ID"
// @Param file formData file true "CSV file (Symbol,Company,Weight)"
// @Success 200 {object} models.GetETFHoldingsResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/load_etf_holdings [post]
func (h *AdminHandler) LoadETFHoldings(c *gin.Context) {
	ctx := c.Request.Context()

	// Resolve security from ticker or security_id form fields.
	ticker := strings.TrimSpace(c.PostForm("ticker"))
	securityIDStr := strings.TrimSpace(c.PostForm("security_id"))

	if ticker == "" && securityIDStr == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "must provide either ticker or security_id",
		})
		return
	}

	var securityID int64
	if ticker == "" {
		id, parseErr := strconv.ParseInt(securityIDStr, 10, 64)
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error:   "invalid_request",
				Message: "security_id must be an integer",
			})
			return
		}
		securityID = id
	}

	prefetchedByID, prefetchedBySymbol, err := h.membershipSvc.GetAllSecurities(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	var security *models.Security
	if ticker != "" {
		matches := prefetchedBySymbol[ticker]
		if len(matches) == 0 {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error:   "not_found",
				Message: "security not found for ticker: " + ticker,
			})
			return
		}
		sec := pickETFSecurity(matches).Security
		security = &sec
	} else {
		security = prefetchedByID[securityID]
		if security == nil {
			c.JSON(http.StatusNotFound, models.ErrorResponse{
				Error:   "not_found",
				Message: "security not found",
			})
			return
		}
	}

	if security.Type != string(models.SecurityTypeETF) && security.Type != string(models.SecurityTypeMutualFund) {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "security is not an ETF or mutual fund",
		})
		return
	}

	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "must provide a CSV file in the 'file' field",
		})
		return
	}

	f, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to open uploaded file: " + err.Error(),
		})
		return
	}
	defer f.Close()

	rawHoldings, err := ParseETFHoldingsCSV(f)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "failed to parse Fidelity CSV: " + err.Error(),
		})
		return
	}

	warnCtx, wc := services.NewWarningContext(ctx)
	resolved, err := h.membershipSvc.ResolveAndPersistETFHoldings(
		warnCtx, security.ID, security.Symbol, rawHoldings, prefetchedBySymbol)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	holdingsDTO := make([]models.ETFHoldingDTO, len(resolved))
	for i, holding := range resolved {
		holdingsDTO[i] = models.ETFHoldingDTO{
			Symbol:     holding.Symbol,
			Name:       holding.Name,
			Percentage: holding.Percentage,
		}
	}

	log.Infof("LoadETFHoldings: persisted %d holdings for %s", len(resolved), security.Symbol)

	c.JSON(http.StatusOK, models.GetETFHoldingsResponse{
		SecurityID: security.ID,
		Symbol:     security.Symbol,
		Name:       security.Name,
		Holdings:   holdingsDTO,
		Warnings:   wc.GetWarnings(),
	})
}

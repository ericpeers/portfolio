package handlers

import (
	"fmt"
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

// validSecurityTypes is the set of accepted ds_type values.
var validSecurityTypes = map[string]bool{
	string(models.SecurityTypeStock):          true,
	string(models.SecurityTypePreferredStock): true,
	string(models.SecurityTypeBond):           true,
	string(models.SecurityTypeETC):            true,
	string(models.SecurityTypeETF):            true,
	string(models.SecurityTypeFund):           true,
	string(models.SecurityTypeIndex):          true,
	string(models.SecurityTypeMutualFund):     true,
	string(models.SecurityTypeNotes):          true,
	string(models.SecurityTypeUnit):           true,
	string(models.SecurityTypeWarrant):        true,
	string(models.SecurityTypeCurrency):       true,
	string(models.SecurityTypeCommodity):      true,
	string(models.SecurityTypeOption):         true,
}

// securitiesExchangeMap mirrors the Python EXCHANGE_MAP for CSV imports.
var securitiesExchangeMap = map[string]string{
	"GBOND": "BONDS/CASH/TREASURIES",
}

// AdminHandler handles admin endpoints
type AdminHandler struct {
	adminSvc      *services.AdminService
	pricingSvc    *services.PricingService
	membershipSvc *services.MembershipService
	secRepo       *repository.SecurityRepository
	exchangeRepo  *repository.ExchangeRepository
}

// NewAdminHandler creates a new AdminHandler
func NewAdminHandler(adminSvc *services.AdminService, pricingSvc *services.PricingService, membershipSvc *services.MembershipService, secRepo *repository.SecurityRepository, exchangeRepo *repository.ExchangeRepository) *AdminHandler {
	return &AdminHandler{
		adminSvc:      adminSvc,
		pricingSvc:    pricingSvc,
		membershipSvc: membershipSvc,
		secRepo:       secRepo,
		exchangeRepo:  exchangeRepo,
	}
}

// SyncSecurities handles POST /admin/sync-securities-from-av
// @Summary Sync securities from AlphaVantage
// @Description Synchronize the securities database with AlphaVantage listing status. Pass type=dryrun to simulate without writes.
// @Tags admin
// @Produce json
// @Param type query string false "Run mode: omit for live sync, 'dryrun' or 'dry_run' for simulation"
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/sync-securities-from-av [post]
func (h *AdminHandler) SyncSecuritiesFromAV(c *gin.Context) {
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
	ticker := req.Ticker
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
		ticker = security.Ticker
	} else if req.SecurityID != 0 && req.Ticker == "" {
		// We have security_id but need ticker for response
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
		ticker = security.Ticker
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
		Ticker:     ticker,
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

	prefetchedByID, prefetchedByTicker, err := h.membershipSvc.GetAllSecurities(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	var security *models.Security
	if req.Ticker != "" {
		matches := prefetchedByTicker[req.Ticker]
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
		warnCtx, security.ID, security.Ticker, prefetchedByID, prefetchedByTicker)
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
			Ticker:     h.Ticker,
			Name:       h.Name,
			Percentage: h.Percentage,
		}
	}

	c.JSON(http.StatusOK, models.GetETFHoldingsResponse{
		SecurityID: security.ID,
		Ticker:     security.Ticker,
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

	prefetchedByID, prefetchedByTicker, err := h.membershipSvc.GetAllSecurities(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	var security *models.Security
	if ticker != "" {
		matches := prefetchedByTicker[ticker]
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
		warnCtx, security.ID, security.Ticker, rawHoldings, prefetchedByTicker)
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
			Ticker:     holding.Ticker,
			Name:       holding.Name,
			Percentage: holding.Percentage,
		}
	}

	log.Infof("LoadETFHoldings: persisted %d holdings for %s", len(resolved), security.Ticker)

	c.JSON(http.StatusOK, models.GetETFHoldingsResponse{
		SecurityID: security.ID,
		Ticker:     security.Ticker,
		Name:       security.Name,
		Holdings:   holdingsDTO,
		Warnings:   wc.GetWarnings(),
	})
}

// BulkFetchEODHDPrices handles GET /admin/bulk-fetch-eodhd-prices
// @Summary Bulk fetch EODHD end-of-day prices for the US exchange
// @Description Fetches end-of-day prices for all US securities from EODHD and stores them in the price cache. Always targets the US exchange — EODHD bulk fetch is only cost-effective for US equities.
// @Tags admin
// @Produce json
// @Param date query string false "Date to fetch (YYYY-MM-DD, defaults to today)"
// @Success 200 {object} models.BulkFetchResult
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/bulk-fetch-eodhd-prices [get]
func (h *AdminHandler) BulkFetchEODHDPrices(c *gin.Context) {
	var date time.Time
	if dateStr := strings.TrimSpace(c.Query("date")); dateStr != "" {
		var err error
		date, err = time.Parse("2006-01-02", dateStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, models.ErrorResponse{
				Error:   "invalid_request",
				Message: "date must be in YYYY-MM-DD format",
			})
			return
		}
	} else {
		lmc := services.LastMarketClose(time.Now())
		date = time.Date(lmc.Year(), lmc.Month(), lmc.Day(), 0, 0, 0, 0, time.UTC)
	}

	if wd := date.Weekday(); wd == time.Saturday || wd == time.Sunday {
		c.JSON(http.StatusUnprocessableEntity, models.ErrorResponse{
			Error:   "markets_closed",
			Message: "Markets not open: " + date.Format("2006-01-02") + " is a " + wd.String(),
		})
		return
	}
	if services.IsUSMarketHoliday(date) {
		c.JSON(http.StatusUnprocessableEntity, models.ErrorResponse{
			Error:   "markets_closed",
			Message: "Markets not open: " + date.Format("2006-01-02") + " is a market holiday",
		})
		return
	}

	ctx := c.Request.Context()

	allSecurities, err := h.secRepo.GetAllUS(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to load securities: " + err.Error(),
		})
		return
	}
	secsByTicker := make(map[string]*models.Security, len(allSecurities))
	for _, s := range allSecurities {
		secsByTicker[s.Ticker] = s
	}

	result, err := h.pricingSvc.BulkFetchPrices(ctx, "US", date, secsByTicker)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// LoadSecurities handles POST /admin/load_securities
// @Summary Load securities from a CSV upload
// @Description Parse a CSV (ticker,name,exchange,type[,currency,isin,country]) and bulk-insert securities into dim_security. Pass dry_run=true to validate and preview without writing. Also updates ISIN on existing securities when the CSV provides one.
// @Tags admin
// @Accept multipart/form-data
// @Produce json
// @Param file formData file true "CSV file (ticker,name,exchange,type[,currency,isin,country])"
// @Param dry_run formData string false "Set to 'true' to validate without writing (returns new_exchanges and row counts)"
// @Success 200 {object} models.LoadSecuritiesResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {object} models.ErrorResponse
// @Router /admin/load_securities [post]
func (h *AdminHandler) LoadSecurities(c *gin.Context) {
	ctx := c.Request.Context()

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

	dryRun := strings.TrimSpace(c.PostForm("dry_run")) == "true" ||
		strings.TrimSpace(c.PostForm("dry_run")) == "1"

	rows, err := ParseSecuritiesCSV(f)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{
			Error:   "invalid_request",
			Message: "failed to parse CSV: " + err.Error(),
		})
		return
	}

	// Pre-load exchanges (name → id)
	exchanges, err := h.exchangeRepo.GetAllExchanges(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to load exchanges: " + err.Error(),
		})
		return
	}

	// Pre-load US exchange IDs and the set of tickers already on any US exchange.
	// Both are used by the cross-exchange dupe guard below.
	usExchangeIDs, err := h.exchangeRepo.GetUSExchangeIDs(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to load US exchange IDs: " + err.Error(),
		})
		return
	}
	usTickerSet, err := h.secRepo.GetUSTickerSet(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: "failed to load US ticker set: " + err.Error(),
		})
		return
	}

	resp := models.LoadSecuritiesResponse{
		NewExchanges: []string{},
		Warnings:     []string{},
		DryRun:       dryRun,
	}

	seen := make(map[string]struct{}) // dedup key: "TICKER|EXCHANGE"
	var inputs []repository.DimSecurityInput

	const maxNameLen = 200

	for _, row := range rows {
		// Skip long tickers
		if len(row.Ticker) > 30 {
			resp.SkippedLongTicker++
			resp.Warnings = append(resp.Warnings, strings.ToUpper(row.Ticker)+" ticker exceeds 30 chars, skipped")
			continue
		}

		// Truncate names that exceed the DB column limit
		if len(row.Name) > maxNameLen {
			resp.TruncatedName++
			resp.Warnings = append(resp.Warnings, row.Ticker+": name exceeds 200 chars, truncated")
			row.Name = row.Name[:maxNameLen]
		}

		// Resolve exchange name: uppercase then apply map
		exchangeName := strings.ToUpper(row.Exchange)
		if mapped, ok := securitiesExchangeMap[exchangeName]; ok {
			exchangeName = mapped
		}

		// Resolve type: uppercase then map MUTUAL FUND → FUND
		secType := strings.ToUpper(row.Type)
		if secType == "MUTUAL FUND" {
			secType = string(models.SecurityTypeFund)
		}
		if !validSecurityTypes[secType] {
			resp.SkippedBadType++
			resp.Warnings = append(resp.Warnings, row.Ticker+": unknown type '"+row.Type+"', skipped")
			continue
		}

		// Dedup within the file: first (ticker, exchange) wins
		dedupKey := row.Ticker + "|" + exchangeName
		if _, dup := seen[dedupKey]; dup {
			resp.SkippedDupInFile++
			continue
		}
		seen[dedupKey] = struct{}{}

		// Resolve or auto-create exchange
		exchangeID, ok := exchanges[exchangeName]
		if !ok {
			country := row.Country
			if country == "" {
				country = "Unknown"
			}
			if !dryRun {
				newID, createErr := h.exchangeRepo.CreateExchange(ctx, exchangeName, country)
				if createErr != nil {
					resp.Warnings = append(resp.Warnings, "failed to create exchange '"+exchangeName+"': "+createErr.Error())
					continue
				}
				exchanges[exchangeName] = newID
				exchangeID = newID
				// Track newly created US exchanges so the cross-exchange dupe check
				// below catches tickers added later in the same CSV run.
				if strings.EqualFold(country, "USA") {
					usExchangeIDs[newID] = true
				}
			} else {
				// Mark as seen so subsequent rows with the same exchange don't
				// append it again. ExchangeID 0 is a sentinel for "would be new".
				exchanges[exchangeName] = 0
			}
			resp.NewExchanges = append(resp.NewExchanges, exchangeName)
		}

		// US ticker symbols are unique across all US exchanges — the same ticker
		// cannot trade on NYSE and NASDAQ simultaneously. However, data sources
		// that lack per-exchange detail (e.g. FinancialData.net) may assign
		// securities to a generic fallback exchange such as "US". Because
		// dim_security's unique constraint is (ticker, exchange_id), a security
		// already recorded on NYSE Arca would not conflict when re-inserted under
		// the "US" exchange ID. We therefore check the full US ticker set and skip
		// any row whose ticker already appears on any US exchange, regardless of
		// which specific exchange this row targets.
		if usExchangeIDs[exchangeID] && usTickerSet[row.Ticker] {
			resp.SkippedExisting++
			continue
		}

		var currency *string
		if row.Currency != "" {
			s := row.Currency
			currency = &s
		}
		var isin *string
		if row.ISIN != "" {
			s := row.ISIN
			isin = &s
		}

		inputs = append(inputs, repository.DimSecurityInput{
			Ticker:     row.Ticker,
			Name:       row.Name,
			ExchangeID: exchangeID,
			Type:       secType,
			Currency:   currency,
			ISIN:       isin,
		})
	}

	if dryRun {
		existingKeys, existingISINs, dryRunErr := h.secRepo.FindExistingForDryRun(ctx, inputs)
		if dryRunErr != nil {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{
				Error:   "internal_error",
				Message: "dry_run: failed to check existing records: " + dryRunErr.Error(),
			})
			return
		}
		for _, inp := range inputs {
			key := fmt.Sprintf("%s|%d", inp.Ticker, inp.ExchangeID)
			if existingKeys[key] {
				resp.SkippedExisting++
				if inp.ISIN != nil && *inp.ISIN != "" {
					currentISIN := existingISINs[key]
					if currentISIN == nil || *currentISIN != *inp.ISIN {
						resp.UpdatedIsin++
					}
				}
			} else {
				resp.Inserted++
			}
		}
		log.Infof("LoadSecurities dry_run: would_insert=%d would_skip=%d would_update_isin=%d skipped_dup=%d skipped_bad_type=%d skipped_long=%d truncated_name=%d new_exchanges=%d",
			resp.Inserted, resp.SkippedExisting, resp.UpdatedIsin, resp.SkippedDupInFile, resp.SkippedBadType, resp.SkippedLongTicker, resp.TruncatedName, len(resp.NewExchanges))
		c.JSON(http.StatusOK, resp)
		return
	}

	inserted, skipped, bulkErrs := h.secRepo.BulkCreateDimSecurities(ctx, inputs)
	resp.Inserted = inserted
	resp.SkippedExisting += skipped
	for _, e := range bulkErrs {
		resp.Warnings = append(resp.Warnings, e.Error())
	}

	updatedIsin, isinErrs := h.secRepo.UpdateISINsForExisting(ctx, inputs)
	resp.UpdatedIsin = updatedIsin
	for _, e := range isinErrs {
		resp.Warnings = append(resp.Warnings, e.Error())
	}

	log.Infof("LoadSecurities: inserted=%d skipped_existing=%d skipped_dup=%d skipped_bad_type=%d skipped_long=%d truncated_name=%d updated_isin=%d new_exchanges=%d",
		resp.Inserted, resp.SkippedExisting, resp.SkippedDupInFile, resp.SkippedBadType, resp.SkippedLongTicker, resp.TruncatedName, resp.UpdatedIsin, len(resp.NewExchanges))

	c.JSON(http.StatusOK, resp)
}

package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// CreatePortfolioRequest represents the request body for creating a portfolio
type CreatePortfolioRequest struct {
	PortfolioType PortfolioType       `json:"portfolio_type" binding:"required"`
	Objective     Objective           `json:"objective" binding:"required"`
	Name          string              `json:"name" binding:"required"`
	OwnerID       int64               `json:"owner_id" binding:"required"`
	Memberships   []MembershipRequest `json:"memberships"`
}

// MembershipRequest represents a membership in create/update requests
type MembershipRequest struct {
	SecurityID         int64   `json:"security_id"`
	Ticker             string  `json:"ticker"`
	PercentageOrShares float64 `json:"percentage_or_shares" binding:"required"`
}

// UpdatePortfolioRequest represents the request body for updating a portfolio
type UpdatePortfolioRequest struct {
	PortfolioType *PortfolioType      `json:"portfolio_type"`
	Name          string              `json:"name"`
	Objective     *Objective          `json:"objective"`
	Memberships   []MembershipRequest `json:"memberships"`
}

// CompareRequest represents the request body for comparing portfolios
type CompareRequest struct {
	PortfolioA  int64        `json:"portfolio_a" binding:"required"`
	PortfolioB  int64        `json:"portfolio_b" binding:"required"`
	StartPeriod FlexibleDate `json:"start_period" binding:"required" swaggertype:"string" example:"2025-12-01"`
	EndPeriod   FlexibleDate `json:"end_period" binding:"required" swaggertype:"string" example:"2025-12-31"`
}

// CompareResponse represents the comparison result between two portfolios
type CompareResponse struct {
	PortfolioA              PortfolioSummary   `json:"portfolio_a"`
	PortfolioB              PortfolioSummary   `json:"portfolio_b"`
	AbsoluteSimilarityScore float64            `json:"absolute_similarity_score"`
	PerformanceMetrics      PerformanceMetrics `json:"performance_metrics"`
	Warnings                []Warning          `json:"warnings,omitempty"`
	Baskets                 *BasketResult      `json:"baskets,omitempty"`
}

// PortfolioSummary provides a summary of a portfolio for comparison
type PortfolioSummary struct {
	ID                  int64                `json:"id"`
	Name                string               `json:"name"`
	Type                PortfolioType        `json:"type"`
	DirectMembership    []ExpandedMembership `json:"direct_membership"`
	ExpandedMemberships []ExpandedMembership `json:"expanded_memberships"`
}

// MembershipDiff represents the difference in allocation for a security
type MembershipDiff struct {
	SecurityID  int64   `json:"security_id"`
	Symbol      string  `json:"symbol"`
	AllocationA float64 `json:"allocation_a"`
	AllocationB float64 `json:"allocation_b"`
	Difference  float64 `json:"difference"`
}

// PerformanceMetrics contains performance comparison data
type PerformanceMetrics struct {
	PortfolioAMetrics PortfolioPerformance `json:"portfolio_a_metrics"`
	PortfolioBMetrics PortfolioPerformance `json:"portfolio_b_metrics"`
}

// PortfolioPerformance contains performance metrics for a single portfolio
type PortfolioPerformance struct {
	StartValue   float64      `json:"start_value"`
	EndValue     float64      `json:"end_value"`
	GainDollar   float64      `json:"gain_dollar"`
	GainPercent  float64      `json:"gain_percent"`
	Dividends    float64      `json:"dividends"`
	SharpeRatios SharpeRatios `json:"sharpe_ratios"`
	DailyValues  []DailyValue `json:"daily_values"`
}

// DailyValue represents portfolio value on a specific date
type DailyValue struct {
	Date  string  `json:"date"`
	Value float64 `json:"value"`
}

// SharpeRatios contains Sharpe ratios for different time periods
type SharpeRatios struct {
	Daily      float64 `json:"daily"`
	Monthly    float64 `json:"monthly"`
	ThreeMonth float64 `json:"three_month"`
	Yearly     float64 `json:"yearly"`
}

// PortfolioListItem represents a portfolio in a list (metadata only)
type PortfolioListItem struct {
	ID            int64         `json:"id"`
	PortfolioType PortfolioType `json:"portfolio_type"`
	Objective     Objective     `json:"objective"`
	Name          string        `json:"name"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

// ErrorResponse represents an API error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// GetDailyPricesRequest represents the request parameters for fetching daily prices
type GetDailyPricesRequest struct {
	Ticker     string `form:"ticker"`
	SecurityID int64  `form:"security_id"`
	StartDate  string `form:"start_date" binding:"required"`
	EndDate    string `form:"end_date" binding:"required"`
}

// GetDailyPricesResponse represents the response for daily prices
type GetDailyPricesResponse struct {
	SecurityID int64       `json:"security_id"`
	Symbol     string      `json:"symbol"`
	StartDate  string      `json:"start_date"`
	EndDate    string      `json:"end_date"`
	DataPoints int         `json:"data_points"`
	Prices     []PriceData `json:"prices"`
}

// GetETFHoldingsRequest represents the request parameters for fetching ETF holdings
type GetETFHoldingsRequest struct {
	Ticker     string `form:"ticker"`
	SecurityID int64  `form:"security_id"`
}

// GetETFHoldingsResponse represents the response for ETF holdings
type GetETFHoldingsResponse struct {
	SecurityID int64           `json:"security_id"`
	Symbol     string          `json:"symbol"`
	Name       string          `json:"name"`
	PullDate   *string         `json:"pull_date,omitempty"`
	Holdings   []ETFHoldingDTO `json:"holdings"`
	Warnings   []Warning       `json:"warnings,omitempty"`
}

// ETFHoldingDTO represents a single holding in the ETF holdings response
type ETFHoldingDTO struct {
	SecurityID int64   `json:"security_id,omitempty"`
	Symbol     string  `json:"symbol"`
	Name       string  `json:"name,omitempty"`
	Percentage float64 `json:"percentage"`
}

// BuySell represents the trade needed to bring a portfolio B holding to its target allocation.
// Positive values mean buy; negative values mean sell.
type BuySell struct {
	Dollars float64 `json:"dollars"`
	Shares  float64 `json:"shares"`
}

// BasketHolding represents one security's fill data within a basket level
type BasketHolding struct {
	SecurityID     int64   `json:"security_id"`
	Symbol         string  `json:"symbol"`
	IdealAlloc     float64 `json:"ideal_allocation"`
	DirectFill     float64 `json:"direct_fill"`
	RedeemedFill   float64 `json:"redeemed_fill"`
	CoverageWeight float64 `json:"coverage_weight,omitempty"`
	BuySell        BuySell `json:"buy_sell"`
}

// BasketLevel represents one threshold level in the basket analysis
type BasketLevel struct {
	Threshold float64         `json:"threshold"`
	Holdings  []BasketHolding `json:"holdings"`
	TotalFill float64         `json:"total_fill"`
}

func (b BasketHolding) MarshalJSON() ([]byte, error) {
	type buySellJSON struct {
		Dollars json.RawMessage `json:"dollars"`
		Shares  json.RawMessage `json:"shares"`
	}
	type plain struct {
		SecurityID     int64           `json:"security_id"`
		Symbol         string          `json:"symbol"`
		IdealAlloc     json.RawMessage `json:"ideal_allocation"`
		DirectFill     json.RawMessage `json:"direct_fill"`
		RedeemedFill   json.RawMessage `json:"redeemed_fill"`
		CoverageWeight json.RawMessage `json:"coverage_weight,omitempty"`
		BuySell        buySellJSON     `json:"buy_sell"`
	}
	p := plain{
		SecurityID:   b.SecurityID,
		Symbol:       b.Symbol,
		IdealAlloc:   json.RawMessage(fmt.Sprintf("%.6f", b.IdealAlloc)),
		DirectFill:   json.RawMessage(fmt.Sprintf("%.6f", b.DirectFill)),
		RedeemedFill: json.RawMessage(fmt.Sprintf("%.6f", b.RedeemedFill)),
		BuySell: buySellJSON{
			Dollars: json.RawMessage(fmt.Sprintf("%.2f", b.BuySell.Dollars)),
			Shares:  json.RawMessage(fmt.Sprintf("%.4f", b.BuySell.Shares)),
		},
	}
	if b.CoverageWeight != 0 {
		p.CoverageWeight = json.RawMessage(fmt.Sprintf("%.6f", b.CoverageWeight))
	}
	return json.Marshal(p)
}

func (b BasketLevel) MarshalJSON() ([]byte, error) {
	type plain struct {
		Threshold float64          `json:"threshold"`
		Holdings  []BasketHolding  `json:"holdings"`
		TotalFill json.RawMessage  `json:"total_fill"`
	}
	return json.Marshal(plain{
		Threshold: b.Threshold,
		Holdings:  b.Holdings,
		TotalFill: json.RawMessage(fmt.Sprintf("%.4f", b.TotalFill)),
	})
}

// BasketResult holds basket analysis across all five threshold levels
type BasketResult struct {
	Basket20  BasketLevel `json:"basket_20"`
	Basket40  BasketLevel `json:"basket_40"`
	Basket60  BasketLevel `json:"basket_60"`
	Basket80  BasketLevel `json:"basket_80"`
	Basket100 BasketLevel `json:"basket_100"`
}

// MarshalJSON emits Percentage rounded to 4 decimal places so the JSON
// response doesn't carry spurious floating-point noise (e.g. 0.00010075566...).
func (e ETFHoldingDTO) MarshalJSON() ([]byte, error) {
	type plain struct {
		SecurityID int64           `json:"security_id,omitempty"`
		Symbol     string          `json:"symbol"`
		Name       string          `json:"name,omitempty"`
		Percentage json.RawMessage `json:"percentage"`
	}
	return json.Marshal(plain{
		SecurityID: e.SecurityID,
		Symbol:     e.Symbol,
		Name:       e.Name,
		Percentage: json.RawMessage(fmt.Sprintf("%.4f", e.Percentage)),
	})
}

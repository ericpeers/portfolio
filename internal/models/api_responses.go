package models

import (
	"time"
)

// CreatePortfolioRequest represents the request body for creating a portfolio
type CreatePortfolioRequest struct {
	PortfolioType PortfolioType       `json:"portfolio_type" binding:"required"`
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
	Name        string              `json:"name"`
	Memberships []MembershipRequest `json:"memberships"`
}

// CompareRequest represents the request body for comparing portfolios
type CompareRequest struct {
	PortfolioA  int64        `json:"portfolio_a" binding:"required"`
	PortfolioB  int64        `json:"portfolio_b" binding:"required"`
	StartPeriod FlexibleDate `json:"start_period" binding:"required"`
	EndPeriod   FlexibleDate `json:"end_period" binding:"required"`
}

// CompareResponse represents the comparison result between two portfolios
type CompareResponse struct {
	PortfolioA              PortfolioSummary   `json:"portfolio_a"`
	PortfolioB              PortfolioSummary   `json:"portfolio_b"`
	AbsoluteSimilarityScore float64            `json:"absolute_similarity_score"`
	PerformanceMetrics      PerformanceMetrics `json:"performance_metrics"`
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
}

// ETFHoldingDTO represents a single holding in the ETF holdings response
type ETFHoldingDTO struct {
	SecurityID int64   `json:"security_id,omitempty"`
	Symbol     string  `json:"symbol"`
	Name       string  `json:"name,omitempty"`
	Percentage float64 `json:"percentage"`
}

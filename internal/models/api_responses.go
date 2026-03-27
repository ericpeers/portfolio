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
	// CreatedAt is the date the portfolio began trading or was built.
	// Reflects when the strategy was initiated, not when it was imported into this system.
	// Accepts "YYYY-MM-DD" or RFC3339. Defaults to the current timestamp if omitted.
	CreatedAt *FlexibleDate `json:"created_at,omitempty" swaggertype:"string" example:"2023-06-01"`
	// SnapshottedAt records when the membership share counts were entered into this system.
	// Used to reverse stock splits that occurred between created_at and this date,
	// so historical performance is computed with correct pre-split share counts.
	SnapshottedAt *FlexibleDate `json:"snapshotted_at,omitempty" swaggertype:"string" example:"2025-03-15"`
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
	// CreatedAt is the date the portfolio began trading or was built.
	// Reflects when the strategy was initiated, not when it was imported into this system.
	// Accepts "YYYY-MM-DD" or RFC3339. If omitted, the existing value is preserved.
	CreatedAt *FlexibleDate `json:"created_at,omitempty" swaggertype:"string" example:"2023-06-01"`
	// SnapshottedAt records when the membership share counts were entered into this system.
	// If omitted, the existing value is preserved.
	SnapshottedAt *FlexibleDate `json:"snapshotted_at,omitempty" swaggertype:"string" example:"2025-03-15"`
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
	Ticker      string  `json:"ticker"`
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
	StartValue       float64          `json:"start_value"`
	EndValue         float64          `json:"end_value"`
	GainDollar       float64          `json:"gain_dollar"`
	GainPercent      float64          `json:"gain_percent"`
	Dividends        float64          `json:"dividends"`
	SharpeRatios     SharpeRatios     `json:"sharpe_ratios"`
	SortinoRatios    SortinoRatios    `json:"sortino_ratios"`
	BenchmarkMetrics BenchmarkMetrics `json:"benchmark_metrics"`
	DailyValues      []DailyValue     `json:"daily_values"`
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

// SortinoRatios contains Sortino ratios for different time periods.
// Unlike Sharpe, Sortino only penalizes downside volatility.
type SortinoRatios struct {
	Daily      float64 `json:"daily"`
	Monthly    float64 `json:"monthly"`
	ThreeMonth float64 `json:"three_month"`
	Yearly     float64 `json:"yearly"`
}

// AlphaBeta holds Jensen's Alpha (annualized, ×252) and Beta for a portfolio vs. one benchmark.
type AlphaBeta struct {
	Alpha float64 `json:"alpha"`
	Beta  float64 `json:"beta"`
}

// BenchmarkMetrics holds Alpha/Beta vs. each supported market benchmark.
type BenchmarkMetrics struct {
	SP500    AlphaBeta `json:"sp500"`     // vs. ^GSPC
	DowJones AlphaBeta `json:"dow_jones"` // vs. ^DJI
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
	Ticker     string      `json:"ticker"`
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
	Ticker     string          `json:"ticker"`
	Name       string          `json:"name"`
	PullDate   *string         `json:"pull_date,omitempty"`
	Holdings   []ETFHoldingDTO `json:"holdings"`
	Warnings   []Warning       `json:"warnings,omitempty"`
}

// ETFHoldingDTO represents a single holding in the ETF holdings response
type ETFHoldingDTO struct {
	SecurityID int64   `json:"security_id,omitempty"`
	Ticker     string  `json:"ticker"`
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
	Ticker         string  `json:"ticker"`
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
		Ticker         string          `json:"ticker"`
		IdealAlloc     json.RawMessage `json:"ideal_allocation"`
		DirectFill     json.RawMessage `json:"direct_fill"`
		RedeemedFill   json.RawMessage `json:"redeemed_fill"`
		CoverageWeight json.RawMessage `json:"coverage_weight,omitempty"`
		BuySell        buySellJSON     `json:"buy_sell"`
	}
	p := plain{
		SecurityID:   b.SecurityID,
		Ticker:       b.Ticker,
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
		Threshold float64         `json:"threshold"`
		Holdings  []BasketHolding `json:"holdings"`
		TotalFill json.RawMessage `json:"total_fill"`
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

// ReturnMetric holds a return in both dollar and percentage form.
type ReturnMetric struct {
	Dollar     float64 `json:"dollar"`
	Percentage float64 `json:"percentage"`
	StartDate  string  `json:"start_date,omitempty"`
}

// GlancePortfolio is one entry in the glance list with key performance metrics.
type GlancePortfolio struct {
	PortfolioID           int64        `json:"portfolio_id"`
	Name                  string       `json:"name"`
	CurrentValue          float64      `json:"current_value"`
	ValuationDate         string       `json:"valuation_date,omitempty"`
	DailyReturn           ReturnMetric `json:"daily_return"`
	OneMonthReturn        ReturnMetric `json:"one_month_return"`
	OneYearReturn         ReturnMetric `json:"one_year_return"`
	LifeOfPortfolioReturn ReturnMetric `json:"life_of_portfolio_return"`
	Warnings              []Warning    `json:"warnings,omitempty"`
}

// GlanceListResponse is the response body for GET /users/:user_id/glance.
type GlanceListResponse struct {
	Portfolios []GlancePortfolio `json:"portfolios"`
}

// AddGlanceRequest is the request body for POST /users/:user_id/glance.
type AddGlanceRequest struct {
	PortfolioID int64 `json:"portfolio_id" binding:"required"`
}

// ImportPricesResult is returned by POST /admin/import-prices.
type ImportPricesResult struct {
	Inserted       int      `json:"inserted"`
	Failed         int      `json:"failed"`
	UnknownTickers []string `json:"unknown_tickers,omitempty"`
	DryRun         bool     `json:"dry_run,omitempty"`
}

func (e ETFHoldingDTO) MarshalJSON() ([]byte, error) {
	type plain struct {
		SecurityID int64           `json:"security_id,omitempty"`
		Ticker     string          `json:"ticker"`
		Name       string          `json:"name,omitempty"`
		Percentage json.RawMessage `json:"percentage"`
	}
	return json.Marshal(plain{
		SecurityID: e.SecurityID,
		Ticker:     e.Ticker,
		Name:       e.Name,
		Percentage: json.RawMessage(fmt.Sprintf("%.4f", e.Percentage)),
	})
}

package alphavantage

import "time"

// ETFProfileResponse represents the AlphaVantage ETF_PROFILE response
type ETFProfileResponse struct {
	Holdings []ETFHolding `json:"holdings"`
}

// ETFHolding represents a single ETF holding
type ETFHolding struct {
	Symbol string `json:"symbol"`
	Name   string `json:"description"`
	Weight string `json:"weight"`
}

// TreasuryYieldResponse represents the AlphaVantage TREASURY_YIELD response
type TreasuryYieldResponse struct {
	Name     string              `json:"name"`
	Interval string              `json:"interval"`
	Unit     string              `json:"unit"`
	Data     []TreasuryDataPoint `json:"data"`
}

// TreasuryDataPoint represents a single treasury yield data point
type TreasuryDataPoint struct {
	Date  string `json:"date"`
	Value string `json:"value"`
}

// ParsedPriceData represents parsed price data ready for use
type ParsedPriceData struct {
	Date             time.Time
	Open             float64
	High             float64
	Low              float64
	Close            float64
	Volume           int64
	Dividend         float64
	SplitCoefficient float64
}

// ParsedETFHolding represents a parsed ETF holding
type ParsedETFHolding struct {
	Symbol     string
	Name       string
	Percentage float64
}

// ParsedTreasuryRate represents a parsed treasury rate
type ParsedTreasuryRate struct {
	Date time.Time
	Rate float64
}

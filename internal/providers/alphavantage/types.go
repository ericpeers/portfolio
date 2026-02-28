package alphavantage

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

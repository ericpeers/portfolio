package financialdata

// fdPriceRecord is the JSON record returned by FinancialData.net price endpoints.
type fdPriceRecord struct {
	TradingSymbol string  `json:"trading_symbol"`
	Date          string  `json:"date"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	Volume        float64 `json:"volume"`
}

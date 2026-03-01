package financialdata

// fdSplitRecord is the JSON record returned by the FinancialData.net stock-splits endpoint.
type fdSplitRecord struct {
	TradingSymbol string  `json:"trading_symbol"`
	ExecutionDate string  `json:"execution_date"`
	Multiplier    float64 `json:"multiplier"`
}

// fdDividendRecord is the JSON record returned by the FinancialData.net dividends endpoint.
type fdDividendRecord struct {
	TradingSymbol string  `json:"trading_symbol"`
	Type          string  `json:"type"`
	Amount        float64 `json:"amount"`
	ExDate        string  `json:"ex_date"`
}

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

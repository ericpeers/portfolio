package eodhd

// eohdEODRecord is the raw JSON record from the EODHD EOD endpoint.
type eohdEODRecord struct {
	Date          string  `json:"date"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	AdjustedClose float64 `json:"adjusted_close"`
	Volume        float64 `json:"volume"` // float64 because EODHD occasionally returns fractional volumes
}

// eohdDividendRecord is the raw JSON record from the EODHD dividends endpoint.
type eohdDividendRecord struct {
	Date  string  `json:"date"`
	Value float64 `json:"value"`
}

// eohdSplitRecord is the raw JSON record from the EODHD splits endpoint.
// Split is formatted as "numerator/denominator" with decimals (e.g., "4.0000/1.0000").
type eohdSplitRecord struct {
	Date  string `json:"date"`
	Split string `json:"split"`
}

// eohdBulkSplitRecord is the raw JSON record from the EODHD bulk splits endpoint.
type eohdBulkSplitRecord struct {
	Code  string `json:"code"`
	Date  string `json:"date"`
	Split string `json:"split"` // "numerator/denominator", e.g. "1.000000/80.000000"
}

// eohdBulkDividendRecord is the raw JSON record from the EODHD bulk dividends endpoint.
type eohdBulkDividendRecord struct {
	Code     string `json:"code"`
	Date     string `json:"date"`
	Dividend string `json:"dividend"` // quoted decimal string in API response, e.g. "0.15954"
}

// eohdBulkEODRecord is the raw JSON record from the EODHD bulk EOD endpoint.
type eohdBulkEODRecord struct {
	Code          string  `json:"code"`
	Date          string  `json:"date"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	AdjustedClose float64 `json:"adjusted_close"`
	Volume        float64 `json:"volume"` // float64 because EODHD occasionally returns fractional volumes
}

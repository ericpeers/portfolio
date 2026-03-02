package eodhd

// eohdEODRecord is the raw JSON record from the EODHD EOD endpoint.
type eohdEODRecord struct {
	Date          string  `json:"date"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	AdjustedClose float64 `json:"adjusted_close"`
	Volume        int64   `json:"volume"`
}

// eohdDividendRecord is the raw JSON record from the EODHD dividends endpoint.
type eohdDividendRecord struct {
	Date  string  `json:"date"`
	Value float64 `json:"value"`
}

// eohdSplitRecord is the raw JSON record from the EODHD splits endpoint.
// Split is formatted as "numerator:denominator" (e.g., "4:1").
type eohdSplitRecord struct {
	Date  string `json:"date"`
	Split string `json:"split"`
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
	Volume        int64   `json:"volume"`
}

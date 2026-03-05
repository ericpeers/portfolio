package providers

import "time"

// BulkEODRecord represents a single end-of-day record from a bulk price fetch.
type BulkEODRecord struct {
	Code     string
	Date     time.Time
	Open     float64
	High     float64
	Low      float64
	Close    float64
	AdjClose float64
	Volume   int64
}

// MergeEventsByDate combines split and dividend slices into one slice, merging
// entries that share the same date. Split-only dates get Dividend=0.
// Dividend-only dates get SplitCoefficient=1.0.
func MergeEventsByDate(splits, dividends []ParsedEventData) []ParsedEventData {
	merged := make(map[time.Time]ParsedEventData)
	for _, s := range splits {
		e := merged[s.Date]
		e.Date = s.Date
		e.SplitCoefficient = s.SplitCoefficient
		merged[s.Date] = e
	}
	for _, d := range dividends {
		e := merged[d.Date]
		e.Date = d.Date
		if e.SplitCoefficient == 0 {
			e.SplitCoefficient = 1.0
		}
		e.Dividend = d.Dividend
		merged[d.Date] = e
	}
	result := make([]ParsedEventData, 0, len(merged))
	for _, e := range merged {
		result = append(result, e)
	}
	return result
}

// ParsedEventData represents a corporate action event (split or dividend) for a security.
type ParsedEventData struct {
	Date             time.Time
	Dividend         float64
	SplitCoefficient float64
}

// ParsedPriceData represents parsed price data ready for use by pricing services.
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

// ParsedETFHolding represents a parsed ETF holding.
type ParsedETFHolding struct {
	Ticker     string
	Name       string
	Percentage float64
}

// ListingStatusEntry represents a row from the LISTING_STATUS CSV endpoint.
type ListingStatusEntry struct {
	Ticker        string
	Name          string
	Exchange      string
	AssetType     string
	IPODate       *time.Time
	DelistingDate *time.Time
	Status        string
}

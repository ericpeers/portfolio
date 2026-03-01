package providers

import "time"

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
	Symbol     string
	Name       string
	Percentage float64
}

// ListingStatusEntry represents a row from the LISTING_STATUS CSV endpoint.
type ListingStatusEntry struct {
	Symbol        string
	Name          string
	Exchange      string
	AssetType     string
	IPODate       *time.Time
	DelistingDate *time.Time
	Status        string
}

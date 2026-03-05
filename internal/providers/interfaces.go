package providers

import (
	"context"
	"time"

	"github.com/epeers/portfolio/internal/models"
)

// StockPriceFetcher fetches daily OHLCV prices for a stock.
type StockPriceFetcher interface {
	GetDailyPrices(ctx context.Context, security *models.SecurityWithCountry, startDT time.Time, endDT time.Time) ([]ParsedPriceData, error)
}

// TreasuryRateFetcher fetches US 10-year treasury rate data.
// startDate and endDate specify the date range to fetch; providers may ignore them
// and return full history (e.g. AlphaVantage) or use them for incremental fetches (e.g. FRED).
type TreasuryRateFetcher interface {
	GetTreasuryRate(ctx context.Context, startDate, endDate time.Time) ([]ParsedPriceData, error)
}

// StockEventFetcher fetches corporate action events (splits + dividends) for a security.
// Returns nil, nil for unsupported security types (OTC, international).
type StockEventFetcher interface {
	GetStockEvents(ctx context.Context, security *models.SecurityWithCountry) ([]ParsedEventData, error)
}

// BulkPriceFetcher fetches end-of-day prices for all securities on an exchange for a given date.
type BulkPriceFetcher interface {
	GetBulkEOD(ctx context.Context, exchange string, date time.Time) ([]BulkEODRecord, error)
}

// ETFHoldingsFetcher fetches holdings for an ETF.
type ETFHoldingsFetcher interface {
	GetETFHoldings(ctx context.Context, ticker string) ([]ParsedETFHolding, error)
}

// ListingStatusFetcher fetches listing status for securities.
type ListingStatusFetcher interface {
	GetListingStatus(ctx context.Context, state string) ([]ListingStatusEntry, error)
}

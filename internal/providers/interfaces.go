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

// BulkEventFetcher fetches corporate actions (splits and dividends) for all securities
// on an exchange for a given date.
type BulkEventFetcher interface {
	GetBulkSplits(ctx context.Context, exchange string, date time.Time) ([]BulkEventRecord, error)
	GetBulkDividends(ctx context.Context, exchange string, date time.Time) ([]BulkEventRecord, error)
	GetBulkEvents(ctx context.Context, exchange string, date time.Time) ([]BulkEventRecord, error)
}

// BulkFetcher combines BulkPriceFetcher and BulkEventFetcher.
// The EODHD client is the only provider that implements this.
type BulkFetcher interface {
	BulkPriceFetcher
	BulkEventFetcher
}

// SecurityListFetcher fetches exchange and symbol lists for bulk security seeding.
type SecurityListFetcher interface {
	GetExchangeList(ctx context.Context) ([]ExchangeInfo, error)
	GetExchangeSymbolList(ctx context.Context, exchangeCode string) ([]SymbolRecord, error)
}

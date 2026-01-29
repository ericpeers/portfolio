package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PriceCacheRepository handles database operations for price caching
type PriceCacheRepository struct {
	pool *pgxpool.Pool
}

// NewPriceCacheRepository creates a new PriceCacheRepository
func NewPriceCacheRepository(pool *pgxpool.Pool) *PriceCacheRepository {
	return &PriceCacheRepository{pool: pool}
}

// GetDailyPrices retrieves cached daily prices for a security within a date range
func (r *PriceCacheRepository) GetDailyPrices(ctx context.Context, securityID int64, startDate, endDate time.Time) ([]models.PriceData, error) {
	query := `
		SELECT security_id, date, open, high, low, close, volume
		FROM price_cache
		WHERE security_id = $1 AND date >= $2 AND date <= $3
		ORDER BY date ASC
	`
	rows, err := r.pool.Query(ctx, query, securityID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query price cache: %w", err)
	}
	defer rows.Close()

	var prices []models.PriceData
	for rows.Next() {
		var p models.PriceData
		if err := rows.Scan(&p.SecurityID, &p.Date, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume); err != nil {
			return nil, fmt.Errorf("failed to scan price data: %w", err)
		}
		prices = append(prices, p)
	}
	return prices, rows.Err()
}

// GetPriceAtDate retrieves the price for a security at a specific date
func (r *PriceCacheRepository) GetPriceAtDate(ctx context.Context, securityID int64, date time.Time) (*models.PriceData, error) {
	query := `
		SELECT security_id, date, open, high, low, close, volume
		FROM price_cache
		WHERE security_id = $1 AND date = $2
	`
	p := &models.PriceData{}
	err := r.pool.QueryRow(ctx, query, securityID, date).Scan(
		&p.SecurityID, &p.Date, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get price: %w", err)
	}
	return p, nil
}

// CacheDailyPrices stores daily prices in the cache
func (r *PriceCacheRepository) CacheDailyPrices(ctx context.Context, prices []models.PriceData) error {
	if len(prices) == 0 {
		return nil
	}

	query := `
		INSERT INTO price_cache (security_id, date, open, high, low, close, volume)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (security_id, date) DO UPDATE
		SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
		    close = EXCLUDED.close, volume = EXCLUDED.volume
	`

	batch := &pgx.Batch{}
	for _, p := range prices {
		batch.Queue(query, p.SecurityID, p.Date, p.Open, p.High, p.Low, p.Close, p.Volume)
	}

	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range prices {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to cache price: %w", err)
		}
	}
	return nil
}

// GetLatestPrice retrieves the most recent price for a security
func (r *PriceCacheRepository) GetLatestPrice(ctx context.Context, securityID int64) (*models.PriceData, error) {
	query := `
		SELECT security_id, date, open, high, low, close, volume
		FROM price_cache
		WHERE security_id = $1
		ORDER BY date DESC
		LIMIT 1
	`
	p := &models.PriceData{}
	err := r.pool.QueryRow(ctx, query, securityID).Scan(
		&p.SecurityID, &p.Date, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest price: %w", err)
	}
	return p, nil
}

// CacheQuote stores a real-time quote
func (r *PriceCacheRepository) CacheQuote(ctx context.Context, quote *models.Quote) error {
	query := `
		INSERT INTO quote_cache (security_id, symbol, price, fetched_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (security_id) DO UPDATE
		SET price = EXCLUDED.price, fetched_at = EXCLUDED.fetched_at
	`
	_, err := r.pool.Exec(ctx, query, quote.SecurityID, quote.Symbol, quote.Price, quote.FetchedAt)
	return err
}

// GetCachedQuote retrieves a cached quote if fresh enough
func (r *PriceCacheRepository) GetCachedQuote(ctx context.Context, securityID int64, maxAge time.Duration) (*models.Quote, error) {
	query := `
		SELECT security_id, symbol, price, fetched_at
		FROM quote_cache
		WHERE security_id = $1 AND fetched_at > $2
	`
	q := &models.Quote{}
	minTime := time.Now().Add(-maxAge)
	err := r.pool.QueryRow(ctx, query, securityID, minTime).Scan(
		&q.SecurityID, &q.Symbol, &q.Price, &q.FetchedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get cached quote: %w", err)
	}
	return q, nil
}

// TreasuryRate represents a treasury rate data point
type TreasuryRate struct {
	Date time.Time
	Rate float64
}

// GetTreasuryRates retrieves cached treasury rates
func (r *PriceCacheRepository) GetTreasuryRates(ctx context.Context, startDate, endDate time.Time) ([]TreasuryRate, error) {
	query := `
		SELECT date, rate
		FROM treasury_rates
		WHERE date >= $1 AND date <= $2
		ORDER BY date ASC
	`
	rows, err := r.pool.Query(ctx, query, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query treasury rates: %w", err)
	}
	defer rows.Close()

	var rates []TreasuryRate
	for rows.Next() {
		var tr TreasuryRate
		if err := rows.Scan(&tr.Date, &tr.Rate); err != nil {
			return nil, fmt.Errorf("failed to scan treasury rate: %w", err)
		}
		rates = append(rates, tr)
	}
	return rates, rows.Err()
}

// CacheTreasuryRates stores treasury rates in the cache
func (r *PriceCacheRepository) CacheTreasuryRates(ctx context.Context, rates []TreasuryRate) error {
	if len(rates) == 0 {
		return nil
	}

	query := `
		INSERT INTO treasury_rates (date, rate)
		VALUES ($1, $2)
		ON CONFLICT (date) DO UPDATE
		SET rate = EXCLUDED.rate
	`

	batch := &pgx.Batch{}
	for _, tr := range rates {
		batch.Queue(query, tr.Date, tr.Rate)
	}

	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range rates {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to cache treasury rate: %w", err)
		}
	}
	return nil
}

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

// PriceRepository handles database operations for price caching
type PriceRepository struct {
	pool *pgxpool.Pool
}

// PriceRange represents the cached date range for a security's prices
type PriceRange struct {
	SecurityID int64
	StartDate  time.Time
	EndDate    time.Time
	NextUpdate time.Time
}

// NewPriceRepository creates a new PriceRepository
func NewPriceRepository(pool *pgxpool.Pool) *PriceRepository {
	return &PriceRepository{pool: pool}
}

// GetDailyPrices retrieves cached daily prices for a security within a date range
func (r *PriceRepository) GetDailyPrices(ctx context.Context, securityID int64, startDate, endDate time.Time) ([]models.PriceData, error) {
	query := `
		SELECT security_id, date, open, high, low, close, volume
		FROM fact_price
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
func (r *PriceRepository) GetPriceAtDate(ctx context.Context, securityID int64, date time.Time) (*models.PriceData, error) {
	query := `
		SELECT security_id, date, open, high, low, close, volume
		FROM fact_price
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

// StoreDailyPrices stores daily prices in postgres
func (r *PriceRepository) StoreDailyPrices(ctx context.Context, prices []models.PriceData) error {
	if len(prices) == 0 {
		return nil
	}

	query := `
		INSERT INTO fact_price (security_id, date, open, high, low, close, volume)
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

func (r *PriceRepository) StoreDailyEvents(ctx context.Context, events []models.EventData) error {
	if len(events) == 0 {
		return nil
	}

	query := `
		INSERT INTO fact_event (security_id, date, dividend, split_coefficient)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (security_id, date) DO UPDATE
		SET dividend=EXCLUDED.dividend, split_coefficient = EXCLUDED.split_coefficient
	`

	batch := &pgx.Batch{}
	for _, p := range events {
		batch.Queue(query, p.SecurityID, p.Date, p.Dividend, p.SplitCoefficient)
	}

	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range events { //this grabs the errors one at a time if there are any.
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to store event %w", err)
		}
	}
	return nil
}

// GetDailySplits retrieves split events (where split_coefficient != 1.0) for a security within a date range
func (r *PriceRepository) GetDailySplits(ctx context.Context, securityID int64, startDate, endDate time.Time) ([]models.EventData, error) {
	query := `
		SELECT security_id, date, dividend, split_coefficient
		FROM fact_event
		WHERE security_id = $1 AND date >= $2 AND date <= $3 AND split_coefficient != 1.0
		ORDER BY date ASC
	`
	rows, err := r.pool.Query(ctx, query, securityID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query split events: %w", err)
	}
	defer rows.Close()

	var events []models.EventData
	for rows.Next() {
		var e models.EventData
		if err := rows.Scan(&e.SecurityID, &e.Date, &e.Dividend, &e.SplitCoefficient); err != nil {
			return nil, fmt.Errorf("failed to scan event data: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// GetLatestPrice retrieves the most recent price for a security
func (r *PriceRepository) GetLatestPrice(ctx context.Context, securityID int64) (*models.PriceData, error) {
	query := `
		SELECT security_id, date, open, high, low, close, volume
		FROM fact_price
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
func (r *PriceRepository) CacheQuote(ctx context.Context, quote *models.Quote) error {
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
func (r *PriceRepository) GetCachedQuote(ctx context.Context, securityID int64, maxAge time.Duration) (*models.Quote, error) {
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

// GetPriceRange retrieves the cached date range for a security
func (r *PriceRepository) GetPriceRange(ctx context.Context, securityID int64) (*PriceRange, error) {
	query := `
		SELECT security_id, start_date, end_date, next_update
		FROM fact_price_range
		WHERE security_id = $1
	`
	pr := &PriceRange{}
	err := r.pool.QueryRow(ctx, query, securityID).Scan(
		&pr.SecurityID, &pr.StartDate, &pr.EndDate, &pr.NextUpdate,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get price range: %w", err)
	}
	return pr, nil
}

// UpsertPriceRange inserts or updates the cached date range for a security
// It expands the range using LEAST/GREATEST to merge with existing data
func (r *PriceRepository) UpsertPriceRange(ctx context.Context, securityID int64, startDate, endDate time.Time, nextUpdate time.Time) error {
	query := `
		INSERT INTO fact_price_range (security_id, start_date, end_date, next_update)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (security_id) DO UPDATE
		SET start_date = LEAST(fact_price_range.start_date, EXCLUDED.start_date),
		    end_date = GREATEST(fact_price_range.end_date, EXCLUDED.end_date),
			next_update = EXCLUDED.next_update
	`
	_, err := r.pool.Exec(ctx, query, securityID, startDate, endDate, nextUpdate)
	if err != nil {
		return fmt.Errorf("failed to upsert price range: %w", err)
	}
	return nil
}

package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
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
		WHERE security_id = $1
		  AND date <= $2
		  AND date >= $2 - INTERVAL '10 days'
		ORDER BY date DESC
		LIMIT 1
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

// GetFirstPriceDates returns the earliest price date for each security in secIDs.
// Uses LATERAL + ORDER BY date LIMIT 1 so the planner does one PK index seek per
// security rather than a full table scan with MIN() aggregation.
// Securities with no price rows are absent from the returned map.
func (r *PriceRepository) GetFirstPriceDates(ctx context.Context, secIDs []int64) (map[int64]*time.Time, error) {
	if len(secIDs) == 0 {
		return map[int64]*time.Time{}, nil
	}
	rows, err := r.pool.Query(ctx, `
		SELECT s.id, fp.date
		FROM unnest($1::bigint[]) AS s(id)
		CROSS JOIN LATERAL (
			SELECT date
			FROM fact_price
			WHERE security_id = s.id
			ORDER BY date ASC
			LIMIT 1
		) fp
	`, secIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to batch-query first price dates: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]*time.Time, len(secIDs))
	for rows.Next() {
		var secID int64
		var t time.Time
		if err := rows.Scan(&secID, &t); err != nil {
			return nil, fmt.Errorf("failed to scan first price date: %w", err)
		}
		result[secID] = &t
	}
	return result, rows.Err()
}

// GetPricesAtDateBatch returns the most-recent closing price on or before date
// for each security in secIDs, within a 10-day lookback window.
// Securities with no price row in that window are absent from the returned map.
func (r *PriceRepository) GetPricesAtDateBatch(ctx context.Context, secIDs []int64, date time.Time) (map[int64]float64, error) {
	if len(secIDs) == 0 {
		return map[int64]float64{}, nil
	}
	query := `
		SELECT DISTINCT ON (security_id) security_id, close
		FROM fact_price
		WHERE security_id = ANY($1)
		  AND date <= $2
		  AND date >= $2 - INTERVAL '10 days'
		ORDER BY security_id, date DESC
	`
	rows, err := r.pool.Query(ctx, query, secIDs, date)
	if err != nil {
		return nil, fmt.Errorf("failed to batch-query prices: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]float64, len(secIDs))
	for rows.Next() {
		var secID int64
		var close float64
		if err := rows.Scan(&secID, &close); err != nil {
			return nil, fmt.Errorf("failed to scan batch price: %w", err)
		}
		result[secID] = close
	}
	return result, rows.Err()
}

// GetLastVolumesBatch returns the most recent volume for each security ID in the list.
// Uses LATERAL to force one PK index seek per security (same pattern as GetPricesAtDateBatch).
// Securities with no price data are absent from the returned map; callers should treat missing as 0.
func (r *PriceRepository) GetLastVolumesBatch(ctx context.Context, ids []int64) (map[int64]int64, error) {
	if len(ids) == 0 {
		return map[int64]int64{}, nil
	}
	rows, err := r.pool.Query(ctx, `
		SELECT s.id, fp.volume
		FROM unnest($1::bigint[]) AS s(id)
		CROSS JOIN LATERAL (
			SELECT volume FROM fact_price
			WHERE security_id = s.id
			ORDER BY date DESC LIMIT 1
		) fp
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to query last volumes: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]int64, len(ids))
	for rows.Next() {
		var secID, volume int64
		if err := rows.Scan(&secID, &volume); err != nil {
			return nil, fmt.Errorf("failed to scan last volume: %w", err)
		}
		result[secID] = volume
	}
	return result, rows.Err()
}

// GetCumulativeSplitCoefficients returns the cumulative split coefficient for each
// security in secIDs where split events exist between startDate and endDate.
// Securities with no qualifying split events are absent from the returned map
// (callers should default absent entries to 1.0).
//
// Naming: "Multi" methods return full per-date event records (map[id][]EventData).
// This method instead collapses events into a single scalar per security by
// multiplying all coefficients together — hence "Cumulative" rather than "Multi".
func (r *PriceRepository) GetCumulativeSplitCoefficients(ctx context.Context, secIDs []int64, startDate, endDate time.Time) (map[int64]float64, error) {
	if len(secIDs) == 0 {
		return map[int64]float64{}, nil
	}
	query := `
		SELECT security_id, split_coefficient
		FROM fact_event
		WHERE security_id = ANY($1)
		  AND date >= $2 AND date <= $3
		  AND split_coefficient != 1.0 AND split_coefficient > 0
		ORDER BY security_id, date ASC
	`
	rows, err := r.pool.Query(ctx, query, secIDs, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to batch-query split coefficients: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]float64)
	for rows.Next() {
		var secID int64
		var coeff float64
		if err := rows.Scan(&secID, &coeff); err != nil {
			return nil, fmt.Errorf("failed to scan split coefficient: %w", err)
		}
		if existing, ok := result[secID]; ok {
			result[secID] = existing * coeff
		} else {
			result[secID] = coeff
		}
	}
	return result, rows.Err()
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

// BulkUpsertPrices inserts or updates a large slice of price rows using a single unnest query.
// Suitable for bulk import; caller should chunk to ~50,000 rows for optimal performance.
func (r *PriceRepository) BulkUpsertPrices(ctx context.Context, prices []models.PriceData) error {
	if len(prices) == 0 {
		return nil
	}
	securityIDs := make([]int64, len(prices))
	dates := make([]time.Time, len(prices))
	opens := make([]float64, len(prices))
	highs := make([]float64, len(prices))
	lows := make([]float64, len(prices))
	closes := make([]float64, len(prices))
	volumes := make([]int64, len(prices))
	for i, p := range prices {
		securityIDs[i] = p.SecurityID
		dates[i] = p.Date
		opens[i] = p.Open
		highs[i] = p.High
		lows[i] = p.Low
		closes[i] = p.Close
		volumes[i] = p.Volume
	}
	query := `
		INSERT INTO fact_price (security_id, date, open, high, low, close, volume)
		SELECT unnest($1::bigint[]), unnest($2::date[]), unnest($3::float8[]),
		       unnest($4::float8[]), unnest($5::float8[]), unnest($6::float8[]), unnest($7::bigint[])
		ON CONFLICT (security_id, date) DO UPDATE
		SET open   = EXCLUDED.open,
		    high   = EXCLUDED.high,
		    low    = EXCLUDED.low,
		    close  = EXCLUDED.close,
		    volume = EXCLUDED.volume
	`
	_, err := r.pool.Exec(ctx, query, securityIDs, dates, opens, highs, lows, closes, volumes)
	if err != nil {
		return fmt.Errorf("failed to bulk upsert prices: %w", err)
	}
	return nil
}

// BulkUpsertEvents inserts or updates a large slice of event rows using a single unnest query.
// Suitable for bulk import; caller should chunk alongside BulkUpsertPrices.
func (r *PriceRepository) BulkUpsertEvents(ctx context.Context, events []models.EventData) error {
	if len(events) == 0 {
		return nil
	}
	securityIDs := make([]int64, len(events))
	dates := make([]time.Time, len(events))
	dividends := make([]float64, len(events))
	splitCoeffs := make([]float64, len(events))
	for i, e := range events {
		securityIDs[i] = e.SecurityID
		dates[i] = e.Date
		dividends[i] = e.Dividend
		splitCoeffs[i] = e.SplitCoefficient
	}
	query := `
		INSERT INTO fact_event (security_id, date, dividend, split_coefficient)
		SELECT unnest($1::bigint[]), unnest($2::date[]), unnest($3::float8[]), unnest($4::float8[])
		ON CONFLICT (security_id, date) DO UPDATE
		SET dividend          = EXCLUDED.dividend,
		    split_coefficient = EXCLUDED.split_coefficient
	`
	_, err := r.pool.Exec(ctx, query, securityIDs, dates, dividends, splitCoeffs)
	if err != nil {
		return fmt.Errorf("failed to bulk upsert events: %w", err)
	}
	return nil
}

// GetDailySplits retrieves split events for a security within a date range.
// Filters split_coefficient != 1.0 (no-op events) and > 0 (bad data guard).
func (r *PriceRepository) GetDailySplits(ctx context.Context, securityID int64, startDate, endDate time.Time) ([]models.EventData, error) {
	query := `
		SELECT security_id, date, dividend, split_coefficient
		FROM fact_event
		WHERE security_id = $1 AND date >= $2 AND date <= $3
		  AND split_coefficient != 1.0 AND split_coefficient > 0
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

// GetAggregatePortfolioDividends blends tables for performance: a JOIN is used
// over a subquery "IN" because EXISTS is faster (stops after first match).
// Multiplication of share count vs. dividends per-share is NOT done here —
// that only works for actual portfolios; ideal portfolios get normalized, so
// the service layer above handles the multiplication.
func (r *PriceRepository) GetAggregatePortfolioDividends(ctx context.Context, portfolioID int64, startDate, endDate time.Time) ([]models.EventData, error) {
	query := `
		SELECT fact_event.security_id, sum(dividend) from fact_event 
		JOIN portfolio_membership ON fact_event.security_id = portfolio_membership.security_id 
		WHERE portfolio_membership.portfolio_id = $1 AND
		fact_event.date >= $2 AND fact_event.date <= $3 
		group by fact_event.security_id
	`
	rows, err := r.pool.Query(ctx, query, portfolioID, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query dividend events: %w", err)
	}
	defer rows.Close()

	var events []models.EventData
	for rows.Next() {
		var e models.EventData
		if err := rows.Scan(&e.SecurityID, &e.Dividend); err != nil {
			return nil, fmt.Errorf("failed to scan event data: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// SecurityPriceMetadata combines inception date from dim_security with the cached
// price range from fact_price_range. PriceRange is nil when no range row exists.
type SecurityPriceMetadata struct {
	SecurityID int64
	Inception  *time.Time
	PriceRange *PriceRange // nil if no fact_price_range row exists
}

// GetPriceRangesWithInception fetches inception dates and cached price ranges for
// multiple securities in a single query. Securities with no fact_price_range row
// are returned with PriceRange nil (caller should treat as "needs fetch").
func (r *PriceRepository) GetPriceRangesWithInception(ctx context.Context, secIDs []int64) (map[int64]*SecurityPriceMetadata, error) {
	if len(secIDs) == 0 {
		return map[int64]*SecurityPriceMetadata{}, nil
	}
	query := `
		SELECT ds.id, ds.inception,
		       fpr.security_id, fpr.start_date, fpr.end_date, fpr.next_update
		FROM   unnest($1::bigint[]) AS s(id)
		JOIN   dim_security ds ON ds.id = s.id
		LEFT JOIN fact_price_range fpr ON fpr.security_id = ds.id
	`
	rows, err := r.pool.Query(ctx, query, secIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to query price ranges with inception: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]*SecurityPriceMetadata, len(secIDs))
	for rows.Next() {
		var secID int64
		var inception *time.Time
		var rangeSecID *int64
		var startDate, endDate, nextUpdate *time.Time
		if err := rows.Scan(&secID, &inception, &rangeSecID, &startDate, &endDate, &nextUpdate); err != nil {
			return nil, fmt.Errorf("failed to scan price range with inception: %w", err)
		}
		meta := &SecurityPriceMetadata{SecurityID: secID, Inception: inception}
		if rangeSecID != nil {
			meta.PriceRange = &PriceRange{
				SecurityID: *rangeSecID,
				StartDate:  *startDate,
				EndDate:    *endDate,
				NextUpdate: *nextUpdate,
			}
		}
		result[secID] = meta
	}
	return result, rows.Err()
}

// GetDailyPricesMulti retrieves cached daily prices for multiple securities in one query.
// Returns a map from security ID to its price slice. Securities with no prices in the
// range are absent from the map.
func (r *PriceRepository) GetDailyPricesMulti(ctx context.Context, secIDs []int64, startDate, endDate time.Time) (map[int64][]models.PriceData, error) {
	if len(secIDs) == 0 {
		return map[int64][]models.PriceData{}, nil
	}
	query := `
		SELECT security_id, date, open, high, low, close, volume
		FROM   fact_price
		WHERE  security_id = ANY($1) AND date >= $2 AND date <= $3
		ORDER  BY security_id, date ASC
	`
	rows, err := r.pool.Query(ctx, query, secIDs, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query multi-security prices: %w", err)
	}
	defer rows.Close()

	result := make(map[int64][]models.PriceData)
	for rows.Next() {
		var p models.PriceData
		if err := rows.Scan(&p.SecurityID, &p.Date, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume); err != nil {
			return nil, fmt.Errorf("failed to scan multi-security price: %w", err)
		}
		result[p.SecurityID] = append(result[p.SecurityID], p)
	}
	return result, rows.Err()
}

// GetLastPricesBeforeMulti returns the most recent price (date + close) strictly before
// beforeDate for each security in secIDs. Uses LATERAL + ORDER BY date DESC LIMIT 1 so the
// planner does one PK index seek per security. Securities with no price before beforeDate are
// absent from the returned map. The date is returned so callers can filter gap splits
// correctly (splits before the returned date are already baked into the close price).
func (r *PriceRepository) GetLastPricesBeforeMulti(ctx context.Context, secIDs []int64, beforeDate time.Time) (map[int64]models.PriceData, error) {
	if len(secIDs) == 0 {
		return map[int64]models.PriceData{}, nil
	}
	rows, err := r.pool.Query(ctx, `
		SELECT s.id, fp.date, fp.close
		FROM unnest($1::bigint[]) AS s(id)
		CROSS JOIN LATERAL (
			SELECT date, close
			FROM fact_price
			WHERE security_id = s.id AND date < $2
			ORDER BY date DESC
			LIMIT 1
		) fp
	`, secIDs, beforeDate)
	if err != nil {
		return nil, fmt.Errorf("failed to batch-query last prices before %s: %w", beforeDate.Format("2006-01-02"), err)
	}
	defer rows.Close()

	result := make(map[int64]models.PriceData, len(secIDs))
	for rows.Next() {
		var p models.PriceData
		if err := rows.Scan(&p.SecurityID, &p.Date, &p.Close); err != nil {
			return nil, fmt.Errorf("failed to scan last price: %w", err)
		}
		result[p.SecurityID] = p
	}
	return result, rows.Err()
}

// GetDailySplitsMulti retrieves split events for multiple securities in one query.
// Returns a map from security ID to its event slice. Securities with no qualifying
// splits in the range are absent from the map.
// Filters split_coefficient != 1.0 (no-op events) and > 0 (bad data guard),
// consistent with GetDailySplits and GetCumulativeSplitCoefficients.
func (r *PriceRepository) GetDailySplitsMulti(ctx context.Context, secIDs []int64, startDate, endDate time.Time) (map[int64][]models.EventData, error) {
	if len(secIDs) == 0 {
		return map[int64][]models.EventData{}, nil
	}
	query := `
		SELECT security_id, date, dividend, split_coefficient
		FROM   fact_event
		WHERE  security_id = ANY($1) AND date >= $2 AND date <= $3
		       AND split_coefficient != 1.0 AND split_coefficient > 0
		ORDER  BY security_id, date ASC
	`
	rows, err := r.pool.Query(ctx, query, secIDs, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query multi-security splits: %w", err)
	}
	defer rows.Close()

	result := make(map[int64][]models.EventData)
	for rows.Next() {
		var e models.EventData
		if err := rows.Scan(&e.SecurityID, &e.Date, &e.Dividend, &e.SplitCoefficient); err != nil {
			return nil, fmt.Errorf("failed to scan multi-security split: %w", err)
		}
		result[e.SecurityID] = append(result[e.SecurityID], e)
	}
	return result, rows.Err()
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

// BatchUpsertPriceRange upserts fact_price_range for multiple securities in one batch.
// Uses the same LEAST/GREATEST expansion logic as UpsertPriceRange.
func (r *PriceRepository) BatchUpsertPriceRange(ctx context.Context, ranges []models.PriceRangeData) error {
	if len(ranges) == 0 {
		return nil
	}
	query := `
		INSERT INTO fact_price_range (security_id, start_date, end_date, next_update)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (security_id) DO UPDATE
		SET start_date  = LEAST(fact_price_range.start_date, EXCLUDED.start_date),
		    end_date    = GREATEST(fact_price_range.end_date, EXCLUDED.end_date),
		    next_update = GREATEST(fact_price_range.next_update, EXCLUDED.next_update)
	`
	batch := &pgx.Batch{}
	for _, rng := range ranges {
		batch.Queue(query, rng.SecurityID, rng.StartDate, rng.EndDate, rng.NextUpdate)
	}
	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()
	for i := range ranges {
		if _, err := br.Exec(); err != nil {
			// Log and continue — a single range upsert failure should not abort the entire batch.
			// The LEAST/GREATEST conflict handler rarely fails; logging gives visibility without
			// halting progress for thousands of securities in a bulk update.
			log.Errorf("BatchUpsertPriceRange: skipping secID=%d: %v", ranges[i].SecurityID, err)
		}
	}
	return nil
}

// GetEventsForExport pre-fetches the sparse fact_event table into a lookup closure.
// The closure returns (dividend, splitCoefficient) for a given security_id + date,
// defaulting to (0, 1.0) when no event exists. Apply the same optional filters as
// StreamPricesForExport so the two are consistent.
func (r *PriceRepository) GetEventsForExport(ctx context.Context, ticker *string, startDate, endDate *time.Time) (func(secID int64, date time.Time) (float64, float64), error) {
	query := `
		SELECT fe.security_id, fe.date, fe.dividend, fe.split_coefficient
		FROM fact_event fe
		JOIN dim_security ds ON ds.id = fe.security_id
		WHERE ($1::text IS NULL OR ds.ticker = $1)
		  AND ($2::date IS NULL OR fe.date >= $2)
		  AND ($3::date IS NULL OR fe.date <= $3)
	`
	rows, err := r.pool.Query(ctx, query, ticker, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query events for export: %w", err)
	}
	defer rows.Close()

	type eventKey struct {
		securityID int64
		date       time.Time
	}
	type eventVals struct{ dividend, splitCoefficient float64 }
	m := make(map[eventKey]eventVals)
	for rows.Next() {
		var key eventKey
		var vals eventVals
		if err := rows.Scan(&key.securityID, &key.date, &vals.dividend, &vals.splitCoefficient); err != nil {
			return nil, fmt.Errorf("failed to scan event row: %w", err)
		}
		m[key] = vals
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return func(secID int64, date time.Time) (float64, float64) {
		if v, ok := m[eventKey{secID, date}]; ok {
			return v.dividend, v.splitCoefficient
		}
		return 0, 1.0
	}, nil
}

// StreamPricesForExport streams fact_price rows (joined with ticker and exchange name)
// to the provided callback without accumulating them in memory. Does not join fact_event;
// call GetEventsForExport first and merge in the callback. All filter params are pointers;
// nil means no filter. Results are ordered by ticker, exchange name, date.
func (r *PriceRepository) StreamPricesForExport(
	ctx context.Context,
	ticker *string,
	startDate, endDate *time.Time,
	fn func(secID int64, ticker, exchange string, date time.Time, open, high, low, closeVal float64, volume int64) error,
) error {
	query := `
		SELECT ds.ticker, de.name, fp.security_id, fp.date,
		       fp.open, fp.high, fp.low, fp.close, fp.volume
		FROM fact_price fp
		JOIN dim_security ds ON ds.id = fp.security_id
		JOIN dim_exchanges de ON de.id = ds.exchange
		WHERE ($1::text IS NULL OR ds.ticker = $1)
		  AND ($2::date IS NULL OR fp.date >= $2)
		  AND ($3::date IS NULL OR fp.date <= $3)
		ORDER BY ds.ticker ASC, de.name ASC, fp.date ASC
	`
	rows, err := r.pool.Query(ctx, query, ticker, startDate, endDate)
	if err != nil {
		return fmt.Errorf("failed to query prices for export: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			tick, exchange      string
			secID               int64
			date                time.Time
			open, high, low, cl float64
			volume              int64
		)
		if err := rows.Scan(&tick, &exchange, &secID, &date, &open, &high, &low, &cl, &volume); err != nil {
			return fmt.Errorf("failed to scan export row: %w", err)
		}
		if err := fn(secID, tick, exchange, date, open, high, low, cl, volume); err != nil {
			return err
		}
	}
	return rows.Err()
}

// GetLastBulkFetchDate returns the most recent end_date in fact_price_range for which
// at least models.MinBulkFetchPrices securities share that date — the same threshold
// BulkFetchPrices uses to decide a fetch is complete.
//
// NOTE: An earlier implementation used COALESCE(MODE() WITHIN GROUP (ORDER BY end_date))
// from fact_price_range. MODE shifts to the wrong date whenever a partial dataset (e.g. an
// incomplete fetch or a large singleton backfill) makes up more than 50% of total rows. For
// databases with ~10,000–15,000 securities an incomplete fetch of 10,000 rows is enough to
// corrupt the watermark. COUNT(*) >= MinBulkFetchPrices is an absolute threshold that does
// not depend on DB size and cannot be satisfied by a partial fetch.
//
// Returns time.Time{} (zero value) when no qualifying date exists (never successfully
// bulk-fetched). Callers must check t.IsZero().
func (r *PriceRepository) GetLastBulkFetchDate(ctx context.Context) (time.Time, error) {
	var t *time.Time
	err := r.pool.QueryRow(ctx, `
		SELECT MAX(end_date)
		FROM (
			SELECT end_date
			FROM fact_price_range
			GROUP BY end_date
			HAVING COUNT(*) >= $1
		) q
	`, models.MinBulkFetchPrices).Scan(&t)
	if err != nil {
		return time.Time{}, fmt.Errorf("GetLastBulkFetchDate: %w", err)
	}
	if t == nil {
		return time.Time{}, nil // no complete bulk fetch on record
	}
	return *t, nil
}

func (r *PriceRepository) UpsertPriceRange(ctx context.Context, securityID int64, startDate, endDate time.Time, nextUpdate time.Time) error {
	//use a GREATEST function on next_update so we don't inadvertently write a value from a year ago when backfilling.
	query := `
		INSERT INTO fact_price_range (security_id, start_date, end_date, next_update)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (security_id) DO UPDATE
		SET start_date  = LEAST(fact_price_range.start_date, EXCLUDED.start_date),
		    end_date    = GREATEST(fact_price_range.end_date, EXCLUDED.end_date),
		    next_update = GREATEST(fact_price_range.next_update, EXCLUDED.next_update)
	`
	_, err := r.pool.Exec(ctx, query, securityID, startDate, endDate, nextUpdate)
	if err != nil {
		return fmt.Errorf("failed to upsert price range: %w", err)
	}
	return nil
}

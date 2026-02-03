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

var ErrSecurityNotFound = errors.New("security not found")

// SecurityRepository handles database operations for securities
type SecurityRepository struct {
	pool *pgxpool.Pool
}

// NewSecurityRepository creates a new SecurityRepository
func NewSecurityRepository(pool *pgxpool.Pool) *SecurityRepository {
	return &SecurityRepository{pool: pool}
}

// GetByID retrieves a security by ID
func (r *SecurityRepository) GetByID(ctx context.Context, id int64) (*models.Security, error) {
	query := `
		SELECT id, ticker, name, exchange, inception, url, type
		FROM dim_security
		WHERE id = $1
	`
	s := &models.Security{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.TypeID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}
	return s, nil
}

// GetBySymbol retrieves a security by symbol (ticker)
func (r *SecurityRepository) GetBySymbol(ctx context.Context, symbol string) (*models.Security, error) {
	query := `
		SELECT id, ticker, name, exchange, inception, url, type
		FROM dim_security
		WHERE ticker = $1
	`
	s := &models.Security{}
	err := r.pool.QueryRow(ctx, query, symbol).Scan(
		&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.TypeID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}
	return s, nil
}

// GetByTicker retrieves a security by ticker from the dim_security table
func (r *SecurityRepository) GetByTicker(ctx context.Context, ticker string) (*models.Security, error) {
	query := `
		SELECT id, ticker, name, exchange, inception, url, type
		FROM dim_security
		WHERE ticker = $1
	`
	s := &models.Security{}
	err := r.pool.QueryRow(ctx, query, ticker).Scan(
		&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.TypeID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security by ticker: %w", err)
	}
	return s, nil
}

// IsETFOrMutualFund checks if a security is an ETF or mutual fund
func (r *SecurityRepository) IsETFOrMutualFund(ctx context.Context, securityID int64) (bool, error) {
	query := `
		SELECT t.name
		FROM dim_security s
		JOIN dim_security_types t ON s.type = t.id
		WHERE s.id = $1
	`
	var typeName string
	err := r.pool.QueryRow(ctx, query, securityID).Scan(&typeName)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrSecurityNotFound
	}
	if err != nil {
		return false, fmt.Errorf("failed to check security type: %w", err)
	}
	return typeName == "etf" || typeName == "mutual fund", nil
}

// GetETFMembership retrieves the holdings of an ETF from dim_etf_membership
func (r *SecurityRepository) GetETFMembership(ctx context.Context, etfID int64) ([]models.ETFMembership, error) {
	query := `
		SELECT dim_security_id, dim_composite_id, percentage
		FROM dim_etf_membership
		WHERE dim_composite_id = $1
	`
	rows, err := r.pool.Query(ctx, query, etfID)
	if err != nil {
		return nil, fmt.Errorf("failed to query ETF memberships: %w", err)
	}
	defer rows.Close()

	var memberships []models.ETFMembership
	for rows.Next() {
		var m models.ETFMembership
		if err := rows.Scan(&m.SecurityID, &m.ETFID, &m.Percentage); err != nil {
			return nil, fmt.Errorf("failed to scan ETF membership: %w", err)
		}
		memberships = append(memberships, m)
	}
	return memberships, rows.Err()
}

// UpsertETFMembership inserts or updates ETF holdings in dim_etf_membership and dim_etf_pull_range
func (r *SecurityRepository) UpsertETFMembership(ctx context.Context, tx pgx.Tx, etfID int64, holdings []models.ETFMembership, nextUpdate time.Time) error {
	// Delete existing holdings for this ETF
	deleteQuery := `DELETE FROM dim_etf_membership WHERE dim_composite_id = $1`
	if _, err := tx.Exec(ctx, deleteQuery, etfID); err != nil {
		return fmt.Errorf("failed to delete existing ETF memberships: %w", err)
	}

	// Insert new holdings
	insertQuery := `
		INSERT INTO dim_etf_membership (dim_security_id, dim_composite_id, percentage)
		VALUES ($1, $2, $3)
	`
	for _, h := range holdings {
		if _, err := tx.Exec(ctx, insertQuery, h.SecurityID, etfID, h.Percentage); err != nil {
			return fmt.Errorf("failed to insert ETF membership: %w", err)
		}
	}

	// Update the pull range tracking
	pullRangeQuery := `
		INSERT INTO dim_etf_pull_range (composite_id, pull_date, next_update)
		VALUES ($1, $2, $3)
		ON CONFLICT (composite_id) DO UPDATE SET
			pull_date = EXCLUDED.pull_date,
			next_update = EXCLUDED.next_update
	`
	if _, err := tx.Exec(ctx, pullRangeQuery, etfID, time.Now().UTC().Truncate(24*time.Hour), nextUpdate); err != nil {
		return fmt.Errorf("failed to upsert ETF pull range: %w", err)
	}

	return nil
}

// ETFPullRange represents the pull range metadata for an ETF
type ETFPullRange struct {
	CompositeID int64
	PullDate    time.Time
	NextUpdate  time.Time
}

// GetETFPullRange returns the pull range for an ETF (when data was last fetched and next expected update)
func (r *SecurityRepository) GetETFPullRange(ctx context.Context, etfID int64) (*ETFPullRange, error) {
	query := `
		SELECT composite_id, pull_date, next_update
		FROM dim_etf_pull_range
		WHERE composite_id = $1
	`
	var pr ETFPullRange
	err := r.pool.QueryRow(ctx, query, etfID).Scan(&pr.CompositeID, &pr.PullDate, &pr.NextUpdate)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get ETF pull range: %w", err)
	}
	return &pr, nil
}

// GetMultipleByIDs retrieves multiple securities by their IDs
func (r *SecurityRepository) GetMultipleByIDs(ctx context.Context, ids []int64) (map[int64]*models.Security, error) {
	if len(ids) == 0 {
		return make(map[int64]*models.Security), nil
	}

	query := `
		SELECT id, ticker, name, exchange, inception, url, type
		FROM dim_security
		WHERE id = ANY($1)
	`
	rows, err := r.pool.Query(ctx, query, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to query securities: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]*models.Security)
	for rows.Next() {
		s := &models.Security{}
		if err := rows.Scan(&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.TypeID); err != nil {
			return nil, fmt.Errorf("failed to scan security: %w", err)
		}
		result[s.ID] = s
	}
	return result, rows.Err()
}

// BeginTx starts a new transaction
func (r *SecurityRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

// CreateDimSecurity inserts a security into dim_security if it doesn't exist
// Returns (id, wasCreated, error)
func (r *SecurityRepository) CreateDimSecurity(
	ctx context.Context,
	ticker, name string,
	exchangeID, typeID int,
	inception *time.Time,
) (int64, bool, error) {
	query := `
		INSERT INTO dim_security (ticker, name, exchange, type, inception)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
		RETURNING id
	`
	//placing ON CONFLICT (only_one_ticker_per_exchange) DO NOTHING
	//results in SQL errs even though there is a constraint that prevent dupes from inserting.
	//I think it only works on index column names.
	var id int64
	err := r.pool.QueryRow(ctx, query, ticker, name, exchangeID, typeID, inception).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Conflict occurred, ticker already exists
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("failed to insert dim_security: %w", err)
	}

	return id, true, nil
}

// DimSecurityInput represents input for bulk security creation
type DimSecurityInput struct {
	Ticker     string
	Name       string
	ExchangeID int
	TypeID     int
	Inception  *time.Time
}

// BulkCreateDimSecurities inserts multiple securities using batch operations.
// Returns the count of inserted and skipped securities, plus any errors.
func (r *SecurityRepository) BulkCreateDimSecurities(
	ctx context.Context,
	securities []DimSecurityInput,
) (inserted int, skipped int, errs []error) {
	if len(securities) == 0 {
		return 0, 0, nil
	}

	query := `
		INSERT INTO dim_security (ticker, name, exchange, type, inception)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
		RETURNING id
	`

	batch := &pgx.Batch{}
	for _, s := range securities {
		batch.Queue(query, s.Ticker, s.Name, s.ExchangeID, s.TypeID, s.Inception)
	}

	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i, s := range securities {
		var id int64
		err := br.QueryRow().Scan(&id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Conflict occurred, ticker already exists - this is a skip, not an error
				skipped++
				continue
			}
			errs = append(errs, fmt.Errorf("failed to insert security %d (%s): %w", i, s.Ticker, err))
			continue
		}
		inserted++
	}

	return inserted, skipped, errs
}

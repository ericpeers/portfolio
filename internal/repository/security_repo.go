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
		SELECT id, symbol, name, security_type, created_at
		FROM securities
		WHERE id = $1
	`
	s := &models.Security{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&s.ID, &s.Symbol, &s.Name, &s.SecurityType, &s.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}
	return s, nil
}

// GetBySymbol retrieves a security by symbol
func (r *SecurityRepository) GetBySymbol(ctx context.Context, symbol string) (*models.Security, error) {
	query := `
		SELECT id, symbol, name, security_type, created_at
		FROM securities
		WHERE symbol = $1
	`
	s := &models.Security{}
	err := r.pool.QueryRow(ctx, query, symbol).Scan(
		&s.ID, &s.Symbol, &s.Name, &s.SecurityType, &s.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}
	return s, nil
}

// IsETFOrMutualFund checks if a security is an ETF or mutual fund
func (r *SecurityRepository) IsETFOrMutualFund(ctx context.Context, securityID int64) (bool, error) {
	query := `
		SELECT security_type
		FROM securities
		WHERE id = $1
	`
	var securityType models.SecurityType
	err := r.pool.QueryRow(ctx, query, securityID).Scan(&securityType)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrSecurityNotFound
	}
	if err != nil {
		return false, fmt.Errorf("failed to check security type: %w", err)
	}
	return securityType == models.SecurityTypeETF || securityType == models.SecurityTypeMutualFund, nil
}

// GetETFMembership retrieves the holdings of an ETF
func (r *SecurityRepository) GetETFMembership(ctx context.Context, etfID int64) ([]models.ETFMembership, error) {
	query := `
		SELECT id, etf_id, security_id, percentage, fetched_at
		FROM etf_memberships
		WHERE etf_id = $1
	`
	rows, err := r.pool.Query(ctx, query, etfID)
	if err != nil {
		return nil, fmt.Errorf("failed to query ETF memberships: %w", err)
	}
	defer rows.Close()

	var memberships []models.ETFMembership
	for rows.Next() {
		var m models.ETFMembership
		if err := rows.Scan(&m.ID, &m.ETFID, &m.SecurityID, &m.Percentage, &m.FetchedAt); err != nil {
			return nil, fmt.Errorf("failed to scan ETF membership: %w", err)
		}
		memberships = append(memberships, m)
	}
	return memberships, rows.Err()
}

// UpsertETFMembership inserts or updates ETF holdings
func (r *SecurityRepository) UpsertETFMembership(ctx context.Context, tx pgx.Tx, etfID int64, holdings []models.ETFMembership) error {
	// Delete existing holdings
	deleteQuery := `DELETE FROM etf_memberships WHERE etf_id = $1`
	if _, err := tx.Exec(ctx, deleteQuery, etfID); err != nil {
		return fmt.Errorf("failed to delete existing ETF memberships: %w", err)
	}

	// Insert new holdings
	insertQuery := `
		INSERT INTO etf_memberships (etf_id, security_id, percentage, fetched_at)
		VALUES ($1, $2, $3, $4)
	`
	now := time.Now()
	for _, h := range holdings {
		if _, err := tx.Exec(ctx, insertQuery, etfID, h.SecurityID, h.Percentage, now); err != nil {
			return fmt.Errorf("failed to insert ETF membership: %w", err)
		}
	}
	return nil
}

// GetETFMembershipFetchedAt returns when the ETF holdings were last fetched
func (r *SecurityRepository) GetETFMembershipFetchedAt(ctx context.Context, etfID int64) (time.Time, error) {
	query := `
		SELECT fetched_at
		FROM etf_memberships
		WHERE etf_id = $1
		LIMIT 1
	`
	var fetchedAt time.Time
	err := r.pool.QueryRow(ctx, query, etfID).Scan(&fetchedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get ETF membership fetched_at: %w", err)
	}
	return fetchedAt, nil
}

// GetMultipleByIDs retrieves multiple securities by their IDs
func (r *SecurityRepository) GetMultipleByIDs(ctx context.Context, ids []int64) (map[int64]*models.Security, error) {
	if len(ids) == 0 {
		return make(map[int64]*models.Security), nil
	}

	query := `
		SELECT id, symbol, name, security_type, created_at
		FROM securities
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
		if err := rows.Scan(&s.ID, &s.Symbol, &s.Name, &s.SecurityType, &s.CreatedAt); err != nil {
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

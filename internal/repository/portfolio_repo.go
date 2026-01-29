package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/epeers/portfolio/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrPortfolioNotFound = errors.New("portfolio not found")
	ErrConflict          = errors.New("portfolio with same name and type already exists for this user")
)

// PortfolioRepository handles database operations for portfolios
type PortfolioRepository struct {
	pool *pgxpool.Pool
}

// NewPortfolioRepository creates a new PortfolioRepository
func NewPortfolioRepository(pool *pgxpool.Pool) *PortfolioRepository {
	return &PortfolioRepository{pool: pool}
}

// Create creates a new portfolio
func (r *PortfolioRepository) Create(ctx context.Context, tx pgx.Tx, p *models.Portfolio) error {
	query := `
		INSERT INTO portfolio (portfolio_type, name, owner, created, updated)
		VALUES ($1, $2, $3, NOW(), NOW())
		RETURNING id, created, updated
	`
	return tx.QueryRow(ctx, query, p.PortfolioType, p.Name, p.OwnerID).
		Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt)
}

// GetByID retrieves a portfolio by ID
func (r *PortfolioRepository) GetByID(ctx context.Context, id int64) (*models.Portfolio, error) {
	query := `
		SELECT id, portfolio_type, name, owner, created, updated
		FROM portfolio
		WHERE id = $1
	`
	p := &models.Portfolio{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&p.ID, &p.PortfolioType, &p.Name, &p.OwnerID, &p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPortfolioNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get portfolio: %w", err)
	}
	return p, nil
}

// GetByNameAndType checks if a portfolio with the same name and type exists for a user
func (r *PortfolioRepository) GetByNameAndType(ctx context.Context, ownerID int64, name string, portfolioType models.PortfolioType) (*models.Portfolio, error) {
	query := `
		SELECT id, portfolio_type, name, owner, created, updated
		FROM portfolio
		WHERE owner = $1 AND name = $2 AND portfolio_type = $3
	`
	p := &models.Portfolio{}
	err := r.pool.QueryRow(ctx, query, ownerID, name, portfolioType).Scan(
		&p.ID, &p.PortfolioType, &p.Name, &p.OwnerID, &p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to check portfolio: %w", err)
	}
	return p, nil
}

// Update updates a portfolio
func (r *PortfolioRepository) Update(ctx context.Context, tx pgx.Tx, p *models.Portfolio) error {
	query := `
		UPDATE portfolio
		SET name = $1, updated = NOW()
		WHERE id = $2
		RETURNING updated
	`
	err := tx.QueryRow(ctx, query, p.Name, p.ID).Scan(&p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrPortfolioNotFound
	}
	return err
}

// Delete deletes a portfolio
func (r *PortfolioRepository) Delete(ctx context.Context, tx pgx.Tx, id int64) error {
	query := `DELETE FROM portfolio WHERE id = $1`
	result, err := tx.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete portfolio: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrPortfolioNotFound
	}
	return nil
}

// GetByUserID retrieves all portfolios for a user (metadata only)
func (r *PortfolioRepository) GetByUserID(ctx context.Context, userID int64) ([]models.PortfolioListItem, error) {
	query := `
		SELECT id, portfolio_type, name, created, updated
		FROM portfolio
		WHERE owner = $1
		ORDER BY created DESC
	`
	rows, err := r.pool.Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to query portfolios: %w", err)
	}
	defer rows.Close()

	var portfolios []models.PortfolioListItem
	for rows.Next() {
		var p models.PortfolioListItem
		if err := rows.Scan(&p.ID, &p.PortfolioType, &p.Name, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan portfolio: %w", err)
		}
		portfolios = append(portfolios, p)
	}
	return portfolios, rows.Err()
}

// CreateMemberships creates portfolio memberships
func (r *PortfolioRepository) CreateMemberships(ctx context.Context, tx pgx.Tx, portfolioID int64, memberships []models.MembershipRequest) ([]models.PortfolioMembership, error) {
	if len(memberships) == 0 {
		return nil, nil
	}

	query := `
		INSERT INTO portfolio_membership (portfolio_id, security_id, percentage_or_shares)
		VALUES ($1, $2, $3)
		RETURNING portfolio_id, security_id, percentage_or_shares
	`

	result := make([]models.PortfolioMembership, 0, len(memberships))
	for _, m := range memberships {
		var pm models.PortfolioMembership
		err := tx.QueryRow(ctx, query, portfolioID, m.SecurityID, m.PercentageOrShares).
			Scan(&pm.PortfolioID, &pm.SecurityID, &pm.PercentageOrShares)
		if err != nil {
			return nil, fmt.Errorf("failed to create membership: %w", err)
		}
		result = append(result, pm)
	}
	return result, nil
}

// GetMemberships retrieves all memberships for a portfolio
func (r *PortfolioRepository) GetMemberships(ctx context.Context, portfolioID int64) ([]models.PortfolioMembership, error) {
	query := `
		SELECT portfolio_id, security_id, percentage_or_shares
		FROM portfolio_membership
		WHERE portfolio_id = $1
	`
	rows, err := r.pool.Query(ctx, query, portfolioID)
	if err != nil {
		return nil, fmt.Errorf("failed to query memberships: %w", err)
	}
	defer rows.Close()

	var memberships []models.PortfolioMembership
	for rows.Next() {
		var m models.PortfolioMembership
		if err := rows.Scan(&m.PortfolioID, &m.SecurityID, &m.PercentageOrShares); err != nil {
			return nil, fmt.Errorf("failed to scan membership: %w", err)
		}
		memberships = append(memberships, m)
	}
	return memberships, rows.Err()
}

// DeleteMemberships deletes all memberships for a portfolio
func (r *PortfolioRepository) DeleteMemberships(ctx context.Context, tx pgx.Tx, portfolioID int64) error {
	query := `DELETE FROM portfolio_membership WHERE portfolio_id = $1`
	_, err := tx.Exec(ctx, query, portfolioID)
	return err
}

// BeginTx starts a new transaction
func (r *PortfolioRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

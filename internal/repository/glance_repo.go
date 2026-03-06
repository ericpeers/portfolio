package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrGlanceEntryNotFound is returned when a portfolio_glance row does not exist.
var ErrGlanceEntryNotFound = errors.New("glance entry not found")

// GlanceRepository handles database operations for portfolio_glance.
type GlanceRepository struct {
	pool *pgxpool.Pool
}

// NewGlanceRepository creates a new GlanceRepository.
func NewGlanceRepository(pool *pgxpool.Pool) *GlanceRepository {
	return &GlanceRepository{pool: pool}
}

// Add inserts a (user_id, portfolio_id) row.
// Returns (true, nil) if newly inserted, (false, nil) if it already existed.
func (r *GlanceRepository) Add(ctx context.Context, userID, portfolioID int64) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`INSERT INTO portfolio_glance (user_id, portfolio_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		userID, portfolioID,
	)
	if err != nil {
		return false, fmt.Errorf("failed to add glance entry: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Remove deletes a (user_id, portfolio_id) row.
// Returns ErrGlanceEntryNotFound if no row was deleted.
func (r *GlanceRepository) Remove(ctx context.Context, userID, portfolioID int64) error {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM portfolio_glance WHERE user_id = $1 AND portfolio_id = $2`,
		userID, portfolioID,
	)
	if err != nil {
		return fmt.Errorf("failed to remove glance entry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrGlanceEntryNotFound
	}
	return nil
}

// ListPortfolioIDs returns all portfolio IDs in the user's glance list.
func (r *GlanceRepository) ListPortfolioIDs(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT portfolio_id FROM portfolio_glance WHERE user_id = $1 ORDER BY portfolio_id`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query glance list: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan glance entry: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

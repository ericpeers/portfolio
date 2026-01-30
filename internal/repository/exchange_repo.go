package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ExchangeRepository handles database operations for exchanges
type ExchangeRepository struct {
	pool *pgxpool.Pool
}

// NewExchangeRepository creates a new ExchangeRepository
func NewExchangeRepository(pool *pgxpool.Pool) *ExchangeRepository {
	return &ExchangeRepository{pool: pool}
}

// GetAllExchanges returns a map of exchange name to ID
func (r *ExchangeRepository) GetAllExchanges(ctx context.Context) (map[string]int, error) {
	query := `SELECT id, name FROM dim_exchanges`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query exchanges: %w", err)
	}
	defer rows.Close()

	exchanges := make(map[string]int)
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("failed to scan exchange: %w", err)
		}
		exchanges[name] = id
	}

	return exchanges, rows.Err()
}

// CreateExchange inserts a new exchange and returns its ID
func (r *ExchangeRepository) CreateExchange(ctx context.Context, name, country string) (int, error) {
	query := `INSERT INTO dim_exchanges (name, country) VALUES ($1, $2) RETURNING id`

	var id int
	err := r.pool.QueryRow(ctx, query, name, country).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to create exchange: %w", err)
	}

	return id, nil
}

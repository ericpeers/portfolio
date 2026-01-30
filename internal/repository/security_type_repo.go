package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SecurityTypeRepository handles database operations for security types
type SecurityTypeRepository struct {
	pool *pgxpool.Pool
}

// NewSecurityTypeRepository creates a new SecurityTypeRepository
func NewSecurityTypeRepository(pool *pgxpool.Pool) *SecurityTypeRepository {
	return &SecurityTypeRepository{pool: pool}
}

// GetAllSecurityTypes returns a map of lowercase security type name to ID
func (r *SecurityTypeRepository) GetAllSecurityTypes(ctx context.Context) (map[string]int, error) {
	query := `SELECT id, name FROM dim_security_types`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query security types: %w", err)
	}
	defer rows.Close()

	types := make(map[string]int)
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("failed to scan security type: %w", err)
		}
		types[strings.ToLower(name)] = id
	}

	return types, rows.Err()
}

package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

var requiredTables = []string{
	"dim_exchanges", "dim_security", "dim_user",
	"dim_etf_membership", "dim_etf_pull_range",
	"portfolio", "portfolio_membership", "portfolio_glance",
	"fact_price", "fact_price_range", "fact_event",
}

// VerifySchema checks that every required table exists in the public schema.
// Returns a descriptive error listing missing tables so the caller can
// log.Fatal rather than silently serving errors from an incomplete pg_restore.
func VerifySchema(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`)
	if err != nil {
		return fmt.Errorf("schema check query failed: %w", err)
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("schema check scan failed: %w", err)
		}
		existing[name] = true
	}

	var missing []string
	for _, t := range requiredTables {
		if !existing[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("incomplete schema — missing tables: %s (pg_restore may still be in progress)",
			strings.Join(missing, ", "))
	}
	return nil
}

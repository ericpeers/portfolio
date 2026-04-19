package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	HintLastUSBulkPriceFetchDate    = "last_us_bulk_price_fetch_date"
	HintLastUSPartialPriceFetchDate = "last_us_partial_price_fetch_date"
	HintLastSecuritiesSyncDate      = "last_securities_sync_date"
	HintLastN2CorrectionFetchDate   = "last_n2_correction_fetch_date"
)

// HintsRepository provides access to the app_hints key/value store.
// Any service may read or write hints; the table is not owned by a single domain.
type HintsRepository struct {
	pool *pgxpool.Pool
}

func NewHintsRepository(pool *pgxpool.Pool) *HintsRepository {
	return &HintsRepository{pool: pool}
}

// GetDateHint reads a hint and parses its value as a date (YYYY-MM-DD).
// Returns time.Time{} (zero) when the key is absent — callers must check t.IsZero().
func (r *HintsRepository) GetDateHint(ctx context.Context, key string) (time.Time, error) {
	var value *string
	err := r.pool.QueryRow(ctx, `SELECT value FROM app_hints WHERE key = $1`, key).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("GetDateHint %q: %w", key, err)
	}
	if value == nil {
		return time.Time{}, nil
	}
	t, err := time.Parse("2006-01-02", *value)
	if err != nil {
		return time.Time{}, fmt.Errorf("GetDateHint %q: invalid date %q: %w", key, *value, err)
	}
	return t, nil
}

// SetDateHint upserts a date hint as "YYYY-MM-DD".
// Uses GREATEST semantics: the stored value only advances, never goes backward.
// This makes concurrent writes safe — the winner is always the later date.
func (r *HintsRepository) SetDateHint(ctx context.Context, key string, t time.Time) error {
	value := t.Format("2006-01-02")
	_, err := r.pool.Exec(ctx, `
		INSERT INTO app_hints (key, value, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (key) DO UPDATE
		    SET value      = GREATEST(app_hints.value, EXCLUDED.value),
		        updated_at = NOW()
	`, key, value)
	if err != nil {
		return fmt.Errorf("SetDateHint %q: %w", key, err)
	}
	return nil
}

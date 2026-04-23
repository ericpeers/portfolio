package tests

import (
	"context"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/repository"
)

// TestFinancialsHistorySharesOutstanding verifies that UpsertFinancialsHistory
// correctly stores and reads back increasing shares_outstanding over 3 annual periods.
func TestFinancialsHistorySharesOutstanding(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Skip if the table or column hasn't been created/migrated yet.
	var exists bool
	err := testPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'fact_financials_history'
			  AND column_name = 'shares_outstanding'
		)`).Scan(&exists)
	if err != nil || !exists {
		t.Skip("fact_financials_history.shares_outstanding not found — apply ALTER TABLE first")
	}

	ticker := nextTicker()
	secID, err := createTestStock(testPool, ticker, "Shares Outstanding Test Co")
	if err != nil {
		t.Fatalf("createTestStock: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(ctx, `DELETE FROM fact_financials_history WHERE security_id = $1`, secID)
		cleanupTestSecurity(testPool, ticker)
	})

	shares := func(n int64) *int64 { return &n }

	rows := []providers.ParsedFinancialsRow{
		{PeriodEnd: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), PeriodType: "A", SharesOutstanding: shares(1_000_000_000)},
		{PeriodEnd: time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC), PeriodType: "A", SharesOutstanding: shares(1_100_000_000)},
		{PeriodEnd: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC), PeriodType: "A", SharesOutstanding: shares(1_200_000_000)},
	}

	repo := repository.NewFundamentalsRepository(testPool)
	if err := repo.UpsertFinancialsHistory(ctx, secID, rows); err != nil {
		t.Fatalf("UpsertFinancialsHistory: %v", err)
	}

	dbRows, err := testPool.Query(ctx, `
		SELECT period_end, shares_outstanding
		FROM fact_financials_history
		WHERE security_id = $1 AND period_type = 'A'
		ORDER BY period_end ASC
	`, secID)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer dbRows.Close()

	type result struct {
		periodEnd         time.Time
		sharesOutstanding *int64
	}
	var got []result
	for dbRows.Next() {
		var r result
		if err := dbRows.Scan(&r.periodEnd, &r.sharesOutstanding); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := dbRows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(got) != len(rows) {
		t.Fatalf("got %d rows, want %d", len(got), len(rows))
	}

	for i, want := range rows {
		if !got[i].periodEnd.Equal(want.PeriodEnd) {
			t.Errorf("row %d: period_end = %s, want %s", i, got[i].periodEnd.Format(time.DateOnly), want.PeriodEnd.Format(time.DateOnly))
		}
		if got[i].sharesOutstanding == nil {
			t.Errorf("row %d (%s): shares_outstanding is nil", i, want.PeriodEnd.Format(time.DateOnly))
			continue
		}
		if *got[i].sharesOutstanding != *want.SharesOutstanding {
			t.Errorf("row %d (%s): shares_outstanding = %d, want %d",
				i, want.PeriodEnd.Format(time.DateOnly), *got[i].sharesOutstanding, *want.SharesOutstanding)
		}
		// Verify each year is strictly greater than the previous.
		if i > 0 && got[i-1].sharesOutstanding != nil && *got[i].sharesOutstanding <= *got[i-1].sharesOutstanding {
			t.Errorf("row %d: shares_outstanding %d not greater than previous year %d",
				i, *got[i].sharesOutstanding, *got[i-1].sharesOutstanding)
		}
	}
}

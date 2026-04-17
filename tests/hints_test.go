package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/repository"
)

func TestHints_GetMissingKeyReturnsZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := repository.NewHintsRepository(testPool)

	got, err := repo.GetDateHint(ctx, "test_nonexistent_key_zzzz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("expected zero time for missing key, got %v", got)
	}
}

func TestHints_SetAndGetRoundtrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := repository.NewHintsRepository(testPool)
	key := "test_hint_roundtrip_zzzz"

	want := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	if err := repo.SetDateHint(ctx, key, want); err != nil {
		t.Fatalf("SetDateHint: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(ctx, `DELETE FROM app_hints WHERE key = $1`, key)
	})

	got, err := repo.GetDateHint(ctx, key)
	if err != nil {
		t.Fatalf("GetDateHint: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

// TestHints_GreatestNeverGoesBackward verifies that SetDateHint will not overwrite
// a newer date with an older one (GREATEST semantics in the upsert).
func TestHints_GreatestNeverGoesBackward(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := repository.NewHintsRepository(testPool)
	key := "test_hint_greatest_zzzz"

	newer := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)
	older := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	if err := repo.SetDateHint(ctx, key, newer); err != nil {
		t.Fatalf("SetDateHint newer: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(ctx, `DELETE FROM app_hints WHERE key = $1`, key)
	})

	// Writing an older date must not roll back the watermark.
	if err := repo.SetDateHint(ctx, key, older); err != nil {
		t.Fatalf("SetDateHint older: %v", err)
	}

	got, err := repo.GetDateHint(ctx, key)
	if err != nil {
		t.Fatalf("GetDateHint: %v", err)
	}
	if !got.Equal(newer) {
		t.Fatalf("expected %v (newer wins), got %v", newer, got)
	}
}

// TestHints_ConcurrentWritesSafe fires N goroutines writing different dates and
// asserts that the final stored value equals the maximum date written.
func TestHints_ConcurrentWritesSafe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := repository.NewHintsRepository(testPool)
	key := "test_hint_concurrent_zzzz"

	t.Cleanup(func() {
		testPool.Exec(ctx, `DELETE FROM app_hints WHERE key = $1`, key)
	})

	dates := []time.Time{
		time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 7, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 11, 30, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 9, 5, 0, 0, 0, 0, time.UTC),
	}

	var wg sync.WaitGroup
	for _, d := range dates {
		d := d
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := repo.SetDateHint(ctx, key, d); err != nil {
				t.Errorf("SetDateHint %v: %v", d, err)
			}
		}()
	}
	wg.Wait()

	got, err := repo.GetDateHint(ctx, key)
	if err != nil {
		t.Fatalf("GetDateHint: %v", err)
	}

	want := time.Date(2025, 11, 30, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected max date %v, got %v", want, got)
	}
}

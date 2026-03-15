package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/epeers/portfolio/internal/repository"
)

// TestSnapshotCacheHit verifies that a second GetAllSecurities call within the TTL
// returns the exact same snapshot (same map pointers, not a fresh DB fetch).
func TestSnapshotCacheHit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()
	secRepo := repository.NewSecurityRepository(testPool)

	byID1, byTicker1, err := secRepo.GetAllSecurities(ctx)
	if err != nil {
		t.Fatalf("first GetAllSecurities: %v", err)
	}
	if len(byID1) == 0 {
		t.Fatal("expected non-empty byID map")
	}

	byID2, byTicker2, err := secRepo.GetAllSecurities(ctx)
	if err != nil {
		t.Fatalf("second GetAllSecurities: %v", err)
	}

	// Pointer equality of a specific entry proves the same snapshot was returned.
	var someID int64
	for id := range byID1 {
		someID = id
		break
	}
	if byID1[someID] != byID2[someID] {
		t.Error("expected cache hit: byID pointers differ across calls (snapshot not reused)")
	}

	var someTicker string
	for ticker := range byTicker1 {
		someTicker = ticker
		break
	}
	if len(byTicker1[someTicker]) > 0 && len(byTicker2[someTicker]) > 0 {
		if byTicker1[someTicker][0] != byTicker2[someTicker][0] {
			t.Error("expected cache hit: byTicker pointers differ across calls (snapshot not reused)")
		}
	}
}

// TestSnapshotCacheInvalidation verifies that ClearCache causes the next
// GetAllSecurities call to rebuild from the database (new snapshot, new pointers).
func TestSnapshotCacheInvalidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()
	secRepo := repository.NewSecurityRepository(testPool)

	byID1, _, err := secRepo.GetAllSecurities(ctx)
	if err != nil {
		t.Fatalf("first GetAllSecurities: %v", err)
	}

	// Pick a known ID from the first snapshot.
	var someID int64
	for id := range byID1 {
		someID = id
		break
	}

	secRepo.ClearCache()

	byID2, _, err := secRepo.GetAllSecurities(ctx)
	if err != nil {
		t.Fatalf("second GetAllSecurities after ClearCache: %v", err)
	}

	// After ClearCache the snapshot is rebuilt: new map, new pointer for same entry.
	if byID1[someID] == byID2[someID] {
		t.Error("expected cache miss after ClearCache: byID pointers are identical (snapshot was not rebuilt)")
	}
}

// TestGetByIDSnapshotFastPath verifies that GetByID correctly resolves against
// the in-memory snapshot and returns ErrSecurityNotFound for unknown IDs.
func TestGetByIDSnapshotFastPath(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()
	secRepo := repository.NewSecurityRepository(testPool)

	// Warm the snapshot.
	byID, _, err := secRepo.GetAllSecurities(ctx)
	if err != nil {
		t.Fatalf("GetAllSecurities: %v", err)
	}

	// Pick a real ID and verify GetByID returns the same data.
	var testID int64
	for id := range byID {
		testID = id
		break
	}
	got, err := secRepo.GetByID(ctx, testID)
	if err != nil {
		t.Fatalf("GetByID(%d): %v", testID, err)
	}
	want := byID[testID]
	if got.ID != want.ID || got.Ticker != want.Ticker {
		t.Errorf("GetByID returned %+v; want %+v", got, want)
	}

	// Non-existent ID must return ErrSecurityNotFound (not a DB error).
	const nonExistentID int64 = -999999
	_, err = secRepo.GetByID(ctx, nonExistentID)
	if !errors.Is(err, repository.ErrSecurityNotFound) {
		t.Errorf("GetByID(nonexistent) = %v; want ErrSecurityNotFound", err)
	}
}

// TestGetByTickerSnapshotFastPath verifies that GetByTicker correctly resolves
// against the snapshot and returns ErrSecurityNotFound for unknown tickers.
func TestGetByTickerSnapshotFastPath(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	ctx := context.Background()
	secRepo := repository.NewSecurityRepository(testPool)

	// Warm the snapshot and find a US-listed ticker for a reliable resolution.
	_, byTicker, err := secRepo.GetAllSecurities(ctx)
	if err != nil {
		t.Fatalf("GetAllSecurities: %v", err)
	}

	var testTicker string
	for ticker, candidates := range byTicker {
		for _, c := range candidates {
			if c.Country == "USA" {
				testTicker = ticker
				break
			}
		}
		if testTicker != "" {
			break
		}
	}
	if testTicker == "" {
		t.Skip("no USA-listed security found in DB; skipping")
	}

	got, err := secRepo.GetByTicker(ctx, testTicker)
	if err != nil {
		t.Fatalf("GetByTicker(%q): %v", testTicker, err)
	}
	want := repository.PreferUSListing(byTicker[testTicker])
	if want == nil {
		t.Fatalf("PreferUSListing returned nil for %q", testTicker)
	}
	if got.ID != want.ID {
		t.Errorf("GetByTicker(%q) returned ID %d; want %d", testTicker, got.ID, want.ID)
	}

	// Unknown ticker must return ErrSecurityNotFound.
	_, err = secRepo.GetByTicker(ctx, "TSTNOTAREALTICKERXXX")
	if !errors.Is(err, repository.ErrSecurityNotFound) {
		t.Errorf("GetByTicker(unknown) = %v; want ErrSecurityNotFound", err)
	}
}


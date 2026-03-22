package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/epeers/portfolio/internal/models"
)

// fuzzyTestIDs holds the security IDs created by setupFuzzyTestSecurities.
type fuzzyTestIDs struct {
	dashVariantID int64 // ID for TSTBRK-B
	plainID       int64 // ID for TSTBRKPLAIN
}

// setupFuzzyTestSecurities creates:
//   - TSTBRK-B  (the dash variant stored in the DB)
//   - TSTBRKPLAIN (a normal ticker that resolves without fuzzy logic)
//
// The test sends TSTBRKB (no dash), which should fuzzy-match to TSTBRK-B.
func setupFuzzyTestSecurities(t *testing.T) fuzzyTestIDs {
	t.Helper()
	pool := getTestPool(t)
	dashID, err := createTestStock(pool, "TSTBRK-B", "Fuzzy Test B Share")
	if err != nil {
		t.Fatalf("failed to create TSTBRK-B: %v", err)
	}
	plainID, err := createTestStock(pool, "TSTBRKPLAIN", "Fuzzy Test Plain")
	if err != nil {
		t.Fatalf("failed to create TSTBRKPLAIN: %v", err)
	}
	return fuzzyTestIDs{dashVariantID: dashID, plainID: plainID}
}

func cleanupFuzzyTestSecurities(t *testing.T) {
	t.Helper()
	pool := getTestPool(t)
	cleanupTestSecurity(pool, "TSTBRK-B")
	cleanupTestSecurity(pool, "TSTBRKPLAIN")
}

// TestFuzzyTickerMatchOnCreate verifies that a ticker ending in B (e.g. TSTBRKB)
// that has no exact DB match is retried as TSTBRK-B, succeeds, and emits a W2001 warning.
func TestFuzzyTickerMatchOnCreate(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	ids := setupFuzzyTestSecurities(t)
	defer cleanupFuzzyTestSecurities(t)

	const portfolioName = "Fuzzy Match Create Test"
	cleanupTestPortfolio(pool, portfolioName, 1)
	defer cleanupTestPortfolio(pool, portfolioName, 1)

	metadata := `{"portfolio_type":"Active","objective":"Growth","name":"Fuzzy Match Create Test","owner_id":1}`
	// TSTBRKB has no exact match — fuzzy should resolve it to TSTBRK-B.
	// TSTBRKPLAIN resolves exactly and should produce no warning.
	csv := "symbol,quantity\nTSTBRKB,10\nTSTBRKPLAIN,5\n"

	req := buildMultipartRequest(t, "POST", "/portfolios", metadata, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var response models.PortfolioWithMemberships
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// Should have exactly one W2001 warning for TSTBRKB → TSTBRK-B.
	var fuzzyWarnings []models.Warning
	for _, w := range response.Warnings {
		if w.Code == models.WarnFuzzyMatchSubstituted {
			fuzzyWarnings = append(fuzzyWarnings, w)
		}
	}
	if len(fuzzyWarnings) != 1 {
		t.Errorf("expected 1 W2001 warning, got %d (all warnings: %+v)", len(fuzzyWarnings), response.Warnings)
	} else {
		msg := fuzzyWarnings[0].Message
		if !strings.Contains(msg, "TSTBRKB") || !strings.Contains(msg, "TSTBRK-B") {
			t.Errorf("W2001 message should mention both tickers, got: %q", msg)
		}
	}

	// Should have 2 memberships, one for each security.
	// CreateMemberships does not JOIN back the ticker, so assert on SecurityID.
	if len(response.Memberships) != 2 {
		t.Fatalf("expected 2 memberships, got %d", len(response.Memberships))
	}
	securityIDs := make(map[int64]bool)
	for _, m := range response.Memberships {
		securityIDs[m.SecurityID] = true
	}
	if !securityIDs[ids.dashVariantID] {
		t.Errorf("expected membership with security ID %d (TSTBRK-B), got IDs: %v", ids.dashVariantID, securityIDs)
	}
	if !securityIDs[ids.plainID] {
		t.Errorf("expected membership with security ID %d (TSTBRKPLAIN), got IDs: %v", ids.plainID, securityIDs)
	}
}

// TestFuzzyTickerNoMatchFails verifies that a ticker ending in B where neither
// the exact nor the dash variant exists still returns a 400 error.
func TestFuzzyTickerNoMatchFails(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	// Ensure neither TSTBADTCKRB nor TSTBADTCKR-B exist.
	cleanupTestSecurity(pool, "TSTBADTCKRB")
	cleanupTestSecurity(pool, "TSTBADTCKR-B")

	metadata := `{"portfolio_type":"Active","objective":"Growth","name":"Fuzzy No Match Test","owner_id":1}`
	csv := "symbol,quantity\nTSTBADTCKRB,10\n"

	req := buildMultipartRequest(t, "POST", "/portfolios", metadata, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unresolvable fuzzy ticker, got %d: %s", w.Code, w.Body.String())
	}

	var errResp models.ErrorResponse
	json.Unmarshal(w.Body.Bytes(), &errResp)
	if !strings.Contains(errResp.Message, "TSTBADTCKRB") {
		t.Errorf("error message should mention the unresolved ticker, got: %q", errResp.Message)
	}
}

// TestCSVBodySentAsJSONReturnsHint verifies that sending a raw CSV file with
// Content-Type: application/json returns a clear message suggesting multipart/form-data.
func TestCSVBodySentAsJSONReturnsHint(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupTestRouter(pool)

	csvBody := "Symbol,Quantity\nAAPL,10\nMSFT,5\n"
	req, _ := http.NewRequest("POST", "/portfolios", bytes.NewBufferString(csvBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "1")

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	var errResp models.ErrorResponse
	json.Unmarshal(w.Body.Bytes(), &errResp)
	if !strings.Contains(errResp.Message, "multipart") {
		t.Errorf("expected hint about multipart/form-data in error, got: %q", errResp.Message)
	}
}

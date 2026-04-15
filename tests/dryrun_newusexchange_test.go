package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/epeers/portfolio/internal/models"
)

func buildDryRunRequest(t *testing.T, csvContent string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	part, _ := w.CreateFormFile("file", "securities.csv")
	part.Write([]byte(csvContent))
	w.WriteField("dry_run", "true")
	w.Close()
	req, err := http.NewRequest("POST", "/admin/securities/load_csv", &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req
}

// TestLoadSecurities_DryRunNewUSExchangeSkipsExistingTicker verifies that the
// dry-run correctly predicts "skipped_existing" for a ticker that already exists
// on a US exchange when the CSV targets a brand-new US exchange.
//
// Regression: before the fix, dry-run reported inserted=1 while the real run
// correctly reported skipped_existing=1, because new US exchanges were assigned
// sentinel ID 0, which was never added to usExchangeIDs, bypassing the
// cross-exchange dupe guard.
func TestLoadSecurities_DryRunNewUSExchangeSkipsExistingTicker(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	router := setupLoadSecuritiesRouter(pool)
	ctx := context.Background()

	ticker := "TSTNWUSEX1TST"
	newExchange := "TST_NEW_US_EX_TST"

	// Cleanup before and after
	pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
	pool.Exec(ctx, `DELETE FROM dim_exchanges WHERE name = $1`, newExchange)
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM dim_security WHERE ticker = $1`, ticker)
		pool.Exec(ctx, `DELETE FROM dim_exchanges WHERE name = $1`, newExchange)
	})

	// Pre-insert ticker on an existing US exchange (NASDAQ)
	pool.Exec(ctx, `
		INSERT INTO dim_security (ticker, name, exchange, type)
		SELECT $1, 'New US Ex TST', id, 'COMMON STOCK'
		FROM dim_exchanges WHERE name = 'NASDAQ' LIMIT 1
		ON CONFLICT DO NOTHING
	`, ticker)

	var count int
	pool.QueryRow(ctx, `SELECT count(*) FROM dim_security WHERE ticker=$1`, ticker).Scan(&count)
	if count == 0 {
		t.Fatal("setup failed: pre-inserted security not found in DB")
	}

	// CSV sends the same ticker to a NEW US exchange that doesn't exist yet.
	// The ticker already exists on NASDAQ (a US exchange), so it should be skipped.
	csv := "ticker,name,exchange,type,country\n" +
		ticker + ",New US Ex TST," + newExchange + ",COMMON STOCK,USA\n"

	// --- Dry-run ---
	req := buildDryRunRequest(t, csv)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("dry-run: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var dryResp models.LoadSecuritiesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &dryResp); err != nil {
		t.Fatal(err)
	}

	if dryResp.Inserted != 0 {
		t.Errorf("dry-run: expected inserted=0, got %d", dryResp.Inserted)
	}
	if dryResp.SkippedExisting != 1 {
		t.Errorf("dry-run: expected skipped_existing=1, got %d", dryResp.SkippedExisting)
	}

	// --- Real run (re-clean exchange so it's also new for real run) ---
	pool.Exec(ctx, `DELETE FROM dim_exchanges WHERE name = $1`, newExchange)
	req = buildLoadSecuritiesRequest(t, csv)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("real: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var realResp models.LoadSecuritiesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &realResp); err != nil {
		t.Fatal(err)
	}

	if realResp.Inserted != 0 {
		t.Errorf("real: expected inserted=0, got %d", realResp.Inserted)
	}
	if realResp.SkippedExisting != 1 {
		t.Errorf("real: expected skipped_existing=1, got %d", realResp.SkippedExisting)
	}
}

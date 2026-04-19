package tests

import (
	"bytes"
	"strings"
	"testing"

	"github.com/epeers/portfolio/internal/handlers"
)

// TestParseSecuritiesCSV_HeaderError verifies that an empty reader returns an error
// on the header read.
func TestParseSecuritiesCSV_HeaderError(t *testing.T) {
	t.Parallel()
	_, err := handlers.ParseSecuritiesCSV(bytes.NewReader(nil))
	if err == nil {
		t.Error("expected error for empty reader, got nil")
	}
}

// TestParseSecuritiesCSV_EmptyTicker verifies that rows with an empty ticker
// are silently skipped (continue branch).
func TestParseSecuritiesCSV_EmptyTicker(t *testing.T) {
	t.Parallel()
	csv := "ticker,name,exchange,type\n,Empty Corp,NYSE,Stock\nAAPL,Apple,NASDAQ,Stock"
	rows, err := handlers.ParseSecuritiesCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 || rows[0].Ticker != "AAPL" {
		t.Errorf("expected 1 row (AAPL), got %d rows", len(rows))
	}
}

// TestParseMembershipCSV_HeaderError verifies that an empty reader returns an error.
func TestParseMembershipCSV_HeaderError(t *testing.T) {
	t.Parallel()
	_, err := handlers.ParseMembershipCSV(bytes.NewReader(nil))
	if err == nil {
		t.Error("expected error for empty reader, got nil")
	}
}

// TestParseMembershipCSV_NoTickerColumn verifies that a CSV without ticker/symbol/security
// column returns a missing-column error.
func TestParseMembershipCSV_NoTickerColumn(t *testing.T) {
	t.Parallel()
	csv := "name,amount\nApple,100"
	_, err := handlers.ParseMembershipCSV(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for missing ticker column, got nil")
	}
}

// TestScanPriceCSVTickers_HeaderError verifies that an empty reader returns an error.
func TestScanPriceCSVTickers_HeaderError(t *testing.T) {
	t.Parallel()
	_, err := handlers.ScanPriceCSVTickers(bytes.NewReader(nil))
	if err == nil {
		t.Error("expected error for empty reader, got nil")
	}
}

// TestScanPriceCSVTickers_NoTickerColumn verifies that a CSV without a ticker column
// returns a missing-column error.
func TestScanPriceCSVTickers_NoTickerColumn(t *testing.T) {
	t.Parallel()
	csv := "date,close\n2025-01-06,100"
	_, err := handlers.ScanPriceCSVTickers(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for missing ticker column, got nil")
	}
}

// TestParseIPODatesCSV_HeaderError verifies that an empty reader returns an error.
func TestParseIPODatesCSV_HeaderError(t *testing.T) {
	t.Parallel()
	_, err := handlers.ParseIPODatesCSV(bytes.NewReader(nil))
	if err == nil {
		t.Error("expected error for empty reader, got nil")
	}
}

// TestParseIPODatesCSV_MissingIPODateColumn verifies that a CSV without the "ipo date"
// column returns a missing-column error.
func TestParseIPODatesCSV_MissingIPODateColumn(t *testing.T) {
	t.Parallel()
	csv := "ticker,name,exchange\nAAPL,Apple,NASDAQ"
	_, err := handlers.ParseIPODatesCSV(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for missing ipo date column, got nil")
	}
}

// TestParseIPODatesCSV_OptionalColumns verifies that rows with missing optional
// columns (name, exchange) are parsed without error and optional fields default
// to empty string. Covers the optionalCol fallback path.
func TestParseIPODatesCSV_OptionalColumns(t *testing.T) {
	t.Parallel()
	csv := "ticker,ipo date\nAAPL,2020-01-02"
	rows, err := handlers.ParseIPODatesCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Name != "" || rows[0].Exchange != "" {
		t.Errorf("expected empty optional fields, got name=%q exchange=%q", rows[0].Name, rows[0].Exchange)
	}
}

// TestParseSecuritiesCSV_RowError verifies that a data row with a mismatched
// field count returns a per-row error.
func TestParseSecuritiesCSV_RowError(t *testing.T) {
	t.Parallel()
	csv := "ticker,name,exchange,type\nAAPL,Apple"
	_, err := handlers.ParseSecuritiesCSV(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for malformed data row, got nil")
	}
}

// TestParseMembershipCSV_RowError verifies that a data row with a mismatched
// field count returns a per-row error.
func TestParseMembershipCSV_RowError(t *testing.T) {
	t.Parallel()
	csv := "ticker,shares\nAAPL"
	_, err := handlers.ParseMembershipCSV(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for malformed data row, got nil")
	}
}

// TestScanPriceCSVTickers_RowError verifies that a data row with a mismatched
// field count returns a per-row error.
func TestScanPriceCSVTickers_RowError(t *testing.T) {
	t.Parallel()
	csv := "ticker,date\nAAPL"
	_, err := handlers.ScanPriceCSVTickers(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for malformed data row, got nil")
	}
}

// TestParseIPODatesCSV_RowError verifies that a data row with a mismatched
// field count returns a per-row error.
func TestParseIPODatesCSV_RowError(t *testing.T) {
	t.Parallel()
	csv := "ticker,ipo date\nAAPL"
	_, err := handlers.ParseIPODatesCSV(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for malformed data row, got nil")
	}
}

// TestParseETFHoldingsCSV_HeaderError verifies that an empty reader returns an error.
func TestParseETFHoldingsCSV_HeaderError(t *testing.T) {
	t.Parallel()
	_, err := handlers.ParseETFHoldingsCSV(bytes.NewReader(nil))
	if err == nil {
		t.Error("expected error for empty reader, got nil")
	}
}

// TestParseETFHoldingsCSV_RowError verifies that a data row with a mismatched
// field count returns a per-row error.
func TestParseETFHoldingsCSV_RowError(t *testing.T) {
	t.Parallel()
	csv := "symbol,company,weight\nAAPL"
	_, err := handlers.ParseETFHoldingsCSV(strings.NewReader(csv))
	if err == nil {
		t.Error("expected error for malformed data row, got nil")
	}
}


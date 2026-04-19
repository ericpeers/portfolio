package tests

import (
	"os"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/providers/eodhd"
)

// TestParseEarningsCalendar verifies parsing of a real EODHD earnings calendar response
// that spans both past (reported) and future (unreported) entries.
// File: fetched 2026-04-17 for range 2026-01-01 → 2026-08-01, covering NVDA, AAPL, MSFT.
func TestParseEarningsCalendar(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/earnings_NVDA_AAPL_MSFT_fetched_2026_04_17_from_2026_01_01_to_2026_08_01.json")
	if err != nil {
		t.Fatalf("testdata file not found: %v", err)
	}

	entries, err := eodhd.ParseEarningsCalendarJSON(data)
	if err != nil {
		t.Fatalf("ParseEarningsCalendarJSON: %v", err)
	}

	// File contains 2 entries each for AAPL, NVDA, MSFT = 6 total.
	if len(entries) != 6 {
		t.Errorf("len(entries) = %d, want 6", len(entries))
	}

	// Build lookup by ticker for targeted assertions.
	byTicker := make(map[string][]time.Time)
	for _, e := range entries {
		if e.Ticker == "" {
			t.Errorf("entry has empty Ticker: %+v", e)
		}
		if e.ExchangeCode == "" {
			t.Errorf("entry for %s has empty ExchangeCode", e.Ticker)
		}
		if e.ReportDate.IsZero() {
			t.Errorf("entry for %s has zero ReportDate", e.Ticker)
		}
		byTicker[e.Ticker] = append(byTicker[e.Ticker], e.ReportDate)
	}

	// All three tickers must be present.
	for _, want := range []string{"AAPL", "NVDA", "MSFT"} {
		if _, ok := byTicker[want]; !ok {
			t.Errorf("ticker %q missing from parsed entries", want)
		}
	}

	// Exchange code stripped correctly from "TICKER.US" format.
	for _, e := range entries {
		if e.ExchangeCode != "US" {
			t.Errorf("ExchangeCode = %q for %s, want %q", e.ExchangeCode, e.Ticker, "US")
		}
	}

	// Spot-check specific report dates.
	wantDates := map[string][]string{
		"AAPL": {"2026-01-29", "2026-04-29"},
		"NVDA": {"2026-02-25", "2026-05-27"},
		"MSFT": {"2026-01-28", "2026-04-29"},
	}
	for ticker, dates := range wantDates {
		got := byTicker[ticker]
		if len(got) != len(dates) {
			t.Errorf("%s: got %d entries, want %d", ticker, len(got), len(dates))
			continue
		}
		gotSet := make(map[string]bool)
		for _, d := range got {
			gotSet[d.Format("2006-01-02")] = true
		}
		for _, d := range dates {
			if !gotSet[d] {
				t.Errorf("%s: missing expected report_date %s", ticker, d)
			}
		}
	}

	// Future entries (report_date > fetch date 2026-04-17) must parse without error
	// even though EODHD sends actual=0 as a placeholder for unreported earnings.
	// Verify they're present — the actual=0 field is ignored by the parser.
	fetchDate := time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC)
	var futureCount int
	for _, e := range entries {
		if e.ReportDate.After(fetchDate) {
			futureCount++
		}
	}
	if futureCount == 0 {
		t.Error("expected at least one future earnings entry in the test file")
	}
}

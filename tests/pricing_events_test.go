package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/financialdata"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

// newEventPricingService creates a PricingService backed by the given mock FD server.
// Both the price client and event client point at the same mock server.
// fredClient is nil because no US10Y securities are used in these tests.
func newEventPricingService(priceRepo *repository.PriceRepository, secRepo *repository.SecurityRepository, fdClient *financialdata.Client) *services.PricingService {
	return services.NewPricingService(priceRepo, secRepo, fdClient, fdClient, nil, nil) // fdClient implements both interfaces
}

// TestFDEventsStoredOnFetch verifies that split events returned by FD are stored in
// fact_event when GetDailyPrices triggers a fresh fetch.
func TestFDEventsStoredOnFetch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	securityID, err := createTestStock(pool, "TSTEVTFETCH", "Test Event Fetch Stock")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTEVTFETCH")

	startDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)
	prices := generateFDPriceData(startDate, endDate)

	splits := []testFDSplitRecord{
		{TradingSymbol: "TSTEVTFETCH", ExecutionDate: "2023-06-15", Multiplier: 2.0},
	}

	mockServer := createMockFDServerWithEvents(prices, splits, nil, nil)
	defer mockServer.Close()

	fdClient := financialdata.NewClientWithBaseURL("test-key", mockServer.URL)
	secRepo := repository.NewSecurityRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := newEventPricingService(priceRepo, secRepo, fdClient)

	_, _, err = pricingSvc.GetDailyPrices(ctx, securityID, startDate, endDate)
	if err != nil {
		t.Fatalf("GetDailyPrices failed: %v", err)
	}

	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_event WHERE security_id = $1`, securityID).Scan(&eventCount); err != nil {
		t.Fatalf("Failed to query fact_event count: %v", err)
	}
	if eventCount == 0 {
		t.Error("Expected at least 1 fact_event row after fetch with splits")
	}

	var splitCoeff float64
	if err := pool.QueryRow(ctx,
		`SELECT split_coefficient FROM fact_event WHERE security_id = $1 AND date = '2023-06-15'`,
		securityID).Scan(&splitCoeff); err != nil {
		t.Fatalf("Failed to find fact_event row for split date 2023-06-15: %v", err)
	}
	if splitCoeff != 2.0 {
		t.Errorf("Expected split_coefficient=2.0, got %v", splitCoeff)
	}
}

// TestFDEventsSameDateMerge verifies that a split and dividend on the same date produce
// exactly one fact_event row with both values populated.
func TestFDEventsSameDateMerge(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	securityID, err := createTestStock(pool, "TSTEVTMRG", "Test Event Merge Stock")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTEVTMRG")

	startDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)
	prices := generateFDPriceData(startDate, endDate)

	splits := []testFDSplitRecord{
		{TradingSymbol: "TSTEVTMRG", ExecutionDate: "2023-09-01", Multiplier: 3.0},
	}
	dividends := []testFDDividendRecord{
		{TradingSymbol: "TSTEVTMRG", Type: "Cash", Amount: 0.50, ExDate: "2023-09-01"},
	}

	mockServer := createMockFDServerWithEvents(prices, splits, dividends, nil)
	defer mockServer.Close()

	fdClient := financialdata.NewClientWithBaseURL("test-key", mockServer.URL)
	secRepo := repository.NewSecurityRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := newEventPricingService(priceRepo, secRepo, fdClient)

	_, _, err = pricingSvc.GetDailyPrices(ctx, securityID, startDate, endDate)
	if err != nil {
		t.Fatalf("GetDailyPrices failed: %v", err)
	}

	var rowCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM fact_event WHERE security_id = $1 AND date = '2023-09-01'`,
		securityID).Scan(&rowCount); err != nil {
		t.Fatalf("Failed to query fact_event: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("Expected exactly 1 fact_event row for same-date split+dividend, got %d", rowCount)
	}

	var splitCoeff, dividend float64
	if err := pool.QueryRow(ctx,
		`SELECT split_coefficient, dividend FROM fact_event WHERE security_id = $1 AND date = '2023-09-01'`,
		securityID).Scan(&splitCoeff, &dividend); err != nil {
		t.Fatalf("Failed to read merged fact_event row: %v", err)
	}
	if splitCoeff != 3.0 {
		t.Errorf("Expected split_coefficient=3.0, got %v", splitCoeff)
	}
	if dividend != 0.50 {
		t.Errorf("Expected dividend=0.50, got %v", dividend)
	}
}

// TestFDEventsSkipped verifies that GetStockEvents returns nil, nil (without any
// HTTP request) for security types that FD does not support events for.
// Previously two separate tests; merged into a table-driven test.
func TestFDEventsSkipped(t *testing.T) {
	cases := []struct {
		name     string
		security models.SecurityWithCountry
	}{
		{
			name: "OTC security",
			security: models.SecurityWithCountry{
				Security:     models.Security{Ticker: "TSOTCUNIT"},
				Country:      "USA",
				ExchangeName: "OTC Bulletin Board",
			},
		},
		{
			name: "international security (GBR)",
			security: models.SecurityWithCountry{
				Security:     models.Security{Ticker: "TSGBUNIT"},
				Country:      "GBR",
				ExchangeName: "London Stock Exchange",
			},
		},
	}

	for _, tc := range cases {
		tc := tc // capture
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("Unexpected HTTP request for %s: %s", tc.name, r.URL.Path)
			}))
			defer server.Close()

			fdClient := financialdata.NewClientWithBaseURL("test-key", server.URL)
			ctx := context.Background()
			events, err := fdClient.GetStockEvents(ctx, &tc.security)
			if err != nil {
				t.Errorf("Expected nil error, got: %v", err)
			}
			if events != nil {
				t.Errorf("Expected nil events, got %d events", len(events))
			}
		})
	}
}

// TestFDEventsFetchSoftFailure verifies that when /stock-splits returns a 500,
// prices are still stored in fact_price and no fact_event rows are written.
func TestFDEventsFetchSoftFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	securityID, err := createTestStock(pool, "TSTEVTFAIL", "Test Event Failure Stock")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTEVTFAIL")

	startDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2023, 6, 30, 0, 0, 0, 0, time.UTC)
	prices := generateFDPriceData(startDate, endDate)

	// Custom mock: 500 for stock-splits, empty for dividends, normal prices otherwise
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/stock-splits":
			w.WriteHeader(http.StatusInternalServerError)
			return
		case "/dividends":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, "[]")
			return
		}
		// Price endpoint
		type fdRecord struct {
			TradingSymbol string  `json:"trading_symbol"`
			Date          string  `json:"date"`
			Open          float64 `json:"open"`
			High          float64 `json:"high"`
			Low           float64 `json:"low"`
			Close         float64 `json:"close"`
			Volume        float64 `json:"volume"`
		}
		ticker := r.URL.Query().Get("ticker")
		var records []fdRecord
		for _, p := range prices {
			records = append(records, fdRecord{
				TradingSymbol: ticker,
				Date:          p.Date.Format("2006-01-02"),
				Open:          p.Open,
				High:          p.High,
				Low:           p.Low,
				Close:         p.Close,
				Volume:        float64(p.Volume),
			})
		}
		if records == nil {
			records = []fdRecord{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(records)
	}))
	defer mockServer.Close()

	fdClient := financialdata.NewClientWithBaseURL("test-key", mockServer.URL)
	secRepo := repository.NewSecurityRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := newEventPricingService(priceRepo, secRepo, fdClient)

	// Should succeed — event fetch failure is non-fatal
	_, _, err = pricingSvc.GetDailyPrices(ctx, securityID, startDate, endDate)
	if err != nil {
		t.Fatalf("GetDailyPrices should succeed even when event fetch fails, got: %v", err)
	}

	// Prices must be stored
	var priceCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_price WHERE security_id = $1`, securityID).Scan(&priceCount); err != nil {
		t.Fatalf("Failed to query fact_price: %v", err)
	}
	if priceCount == 0 {
		t.Error("Expected prices to be stored even when event fetch returns 500")
	}

	// No fact_event rows because the split fetch failed
	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_event WHERE security_id = $1`, securityID).Scan(&eventCount); err != nil {
		t.Fatalf("Failed to query fact_event: %v", err)
	}
	if eventCount != 0 {
		t.Errorf("Expected 0 fact_event rows when split fetch returns 500, got %d", eventCount)
	}
}

// TestFDEventsEmptyResultsNoRows verifies that when FD returns empty arrays for both
// splits and dividends, no fact_event rows are written.
func TestFDEventsEmptyResultsNoRows(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	securityID, err := createTestStock(pool, "TSTEVTEMPTY", "Test Event Empty Stock")
	if err != nil {
		t.Fatalf("Failed to create test security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTEVTEMPTY")

	startDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2023, 3, 31, 0, 0, 0, 0, time.UTC)
	prices := generateFDPriceData(startDate, endDate)

	// nil splits and nil dividends → both return "[]"
	mockServer := createMockFDServerWithEvents(prices, nil, nil, nil)
	defer mockServer.Close()

	fdClient := financialdata.NewClientWithBaseURL("test-key", mockServer.URL)
	secRepo := repository.NewSecurityRepository(pool)
	priceRepo := repository.NewPriceRepository(pool)
	pricingSvc := newEventPricingService(priceRepo, secRepo, fdClient)

	_, _, err = pricingSvc.GetDailyPrices(ctx, securityID, startDate, endDate)
	if err != nil {
		t.Fatalf("GetDailyPrices failed: %v", err)
	}

	var eventCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM fact_event WHERE security_id = $1`, securityID).Scan(&eventCount); err != nil {
		t.Fatalf("Failed to query fact_event: %v", err)
	}
	if eventCount != 0 {
		t.Errorf("Expected 0 fact_event rows when FD returns empty arrays, got %d", eventCount)
	}
}

// TestMergeEventsByDate is a pure unit test for MergeEventsByDate covering all edge cases.
func TestMergeEventsByDate(t *testing.T) {
	d1 := time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		splits    []providers.ParsedEventData
		dividends []providers.ParsedEventData
		wantLen   int
		wantCheck func(t *testing.T, result []providers.ParsedEventData)
	}{
		{
			name:      "empty inputs",
			splits:    nil,
			dividends: nil,
			wantLen:   0,
		},
		{
			name: "split only",
			splits: []providers.ParsedEventData{
				{Date: d1, SplitCoefficient: 2.0},
			},
			dividends: nil,
			wantLen:   1,
			wantCheck: func(t *testing.T, result []providers.ParsedEventData) {
				e := result[0]
				if e.SplitCoefficient != 2.0 {
					t.Errorf("Expected split_coefficient=2.0, got %v", e.SplitCoefficient)
				}
				if e.Dividend != 0 {
					t.Errorf("Expected dividend=0 for split-only, got %v", e.Dividend)
				}
			},
		},
		{
			name:   "dividend only",
			splits: nil,
			dividends: []providers.ParsedEventData{
				{Date: d1, Dividend: 0.75, SplitCoefficient: 1.0},
			},
			wantLen: 1,
			wantCheck: func(t *testing.T, result []providers.ParsedEventData) {
				e := result[0]
				if e.Dividend != 0.75 {
					t.Errorf("Expected dividend=0.75, got %v", e.Dividend)
				}
				if e.SplitCoefficient != 1.0 {
					t.Errorf("Expected split_coefficient=1.0 for dividend-only, got %v", e.SplitCoefficient)
				}
			},
		},
		{
			name: "same date split and dividend merged into one row",
			splits: []providers.ParsedEventData{
				{Date: d1, SplitCoefficient: 3.0},
			},
			dividends: []providers.ParsedEventData{
				{Date: d1, Dividend: 1.25, SplitCoefficient: 1.0},
			},
			wantLen: 1,
			wantCheck: func(t *testing.T, result []providers.ParsedEventData) {
				e := result[0]
				if e.SplitCoefficient != 3.0 {
					t.Errorf("Expected split_coefficient=3.0 (split wins), got %v", e.SplitCoefficient)
				}
				if e.Dividend != 1.25 {
					t.Errorf("Expected dividend=1.25, got %v", e.Dividend)
				}
			},
		},
		{
			name: "different dates produce two rows",
			splits: []providers.ParsedEventData{
				{Date: d1, SplitCoefficient: 2.0},
			},
			dividends: []providers.ParsedEventData{
				{Date: d2, Dividend: 0.50, SplitCoefficient: 1.0},
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := providers.MergeEventsByDate(tt.splits, tt.dividends)
			if len(result) != tt.wantLen {
				t.Errorf("Expected %d results, got %d", tt.wantLen, len(result))
			}
			if tt.wantCheck != nil && len(result) > 0 {
				tt.wantCheck(t, result)
			}
		})
	}
}

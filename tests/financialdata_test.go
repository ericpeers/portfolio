package tests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/financialdata"
)

// makeFDSec builds a SecurityWithCountry for FD routing tests.
func makeFDSec(country, exchangeName string) *models.SecurityWithCountry {
	return &models.SecurityWithCountry{
		Security:     models.Security{Symbol: "TST"},
		Country:      country,
		ExchangeName: exchangeName,
	}
}

// TestFDRouteDomestic verifies that US stocks are routed to the "stock-prices" endpoint.
func TestFDRouteDomestic(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[]"))
	}))
	defer srv.Close()

	client := financialdata.NewClientWithBaseURL("test-key", srv.URL)
	sec := makeFDSec("USA", "NASDAQ")
	_, err := client.GetDailyPrices(context.Background(), sec, "compact")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedPath != "/stock-prices" {
		t.Errorf("expected /stock-prices, got %s", capturedPath)
	}
}

// TestFDRouteOTC verifies that OTC securities are routed to the "otc-prices" endpoint.
func TestFDRouteOTC(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[]"))
	}))
	defer srv.Close()

	client := financialdata.NewClientWithBaseURL("test-key", srv.URL)
	sec := makeFDSec("USA", "OTC Markets")
	_, err := client.GetDailyPrices(context.Background(), sec, "compact")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedPath != "/otc-prices" {
		t.Errorf("expected /otc-prices, got %s", capturedPath)
	}
}

// TestFDRouteInternational verifies non-USA, non-OTC stocks use "international-stock-prices".
func TestFDRouteInternational(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[]"))
	}))
	defer srv.Close()

	client := financialdata.NewClientWithBaseURL("test-key", srv.URL)
	sec := makeFDSec("CAN", "Toronto Stock Exchange")
	_, err := client.GetDailyPrices(context.Background(), sec, "compact")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedPath != "/international-stock-prices" {
		t.Errorf("expected /international-stock-prices, got %s", capturedPath)
	}
}

// TestFDPaginationFull verifies the client fetches multiple pages when a full page (300 records) is returned.
func TestFDPaginationFull(t *testing.T) {
	page1 := makeFDPageJSON(300, "2024-01-01") // full page triggers next fetch
	page2 := makeFDPageJSON(10, "2024-11-05")  // partial page stops the loop

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if callCount == 0 {
			w.Write([]byte(page1))
		} else {
			w.Write([]byte(page2))
		}
		callCount++
	}))
	defer srv.Close()

	client := financialdata.NewClientWithBaseURL("test-key", srv.URL)
	sec := makeFDSec("USA", "NYSE")
	prices, err := client.GetDailyPrices(context.Background(), sec, "full")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 API calls for full pagination, got %d", callCount)
	}
	if len(prices) != 310 {
		t.Errorf("expected 310 price records (300+10), got %d", len(prices))
	}
}

// TestFDPaginationCompact verifies that "compact" mode stops after the first page.
func TestFDPaginationCompact(t *testing.T) {
	page := makeFDPageJSON(300, "2024-01-01") // full page — compact should still stop

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(page))
	}))
	defer srv.Close()

	client := financialdata.NewClientWithBaseURL("test-key", srv.URL)
	sec := makeFDSec("USA", "NYSE")
	prices, err := client.GetDailyPrices(context.Background(), sec, "compact")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected exactly 1 API call in compact mode, got %d", callCount)
	}
	if len(prices) != 300 {
		t.Errorf("expected 300 price records from single page, got %d", len(prices))
	}
}

// TestFD429RateLimit verifies that HTTP 429 returns ErrRateLimited.
func TestFD429RateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	client := financialdata.NewClientWithBaseURL("test-key", srv.URL)
	sec := makeFDSec("USA", "NASDAQ")
	_, err := client.GetDailyPrices(context.Background(), sec, "compact")
	if !errors.Is(err, financialdata.ErrRateLimited) {
		t.Errorf("expected ErrRateLimited, got %v", err)
	}
}

// TestFDEmptyKeyGuard verifies that calling GetDailyPrices with no API key returns an error.
func TestFDEmptyKeyGuard(t *testing.T) {
	client := financialdata.NewClientWithBaseURL("", "http://localhost:9999")
	sec := makeFDSec("USA", "NASDAQ")
	_, err := client.GetDailyPrices(context.Background(), sec, "compact")
	if err == nil {
		t.Error("expected error when API key is empty, got nil")
	}
}

// TestFDParsingCorrect verifies JSON record parsing: date, OHLCV, and that Dividend=0, SplitCoefficient=1.0.
func TestFDParsingCorrect(t *testing.T) {
	record := `[{"trading_symbol":"AAPL","date":"2025-01-06","open":230.5,"high":235.0,"low":229.0,"close":234.0,"volume":50000000}]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(record))
	}))
	defer srv.Close()

	client := financialdata.NewClientWithBaseURL("test-key", srv.URL)
	sec := makeFDSec("USA", "NASDAQ")
	prices, err := client.GetDailyPrices(context.Background(), sec, "compact")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(prices) != 1 {
		t.Fatalf("expected 1 price record, got %d", len(prices))
	}

	p := prices[0]
	expectedDate := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	if !p.Date.Equal(expectedDate) {
		t.Errorf("Date: got %v, want %v", p.Date, expectedDate)
	}
	if p.Open != 230.5 {
		t.Errorf("Open: got %v, want 230.5", p.Open)
	}
	if p.High != 235.0 {
		t.Errorf("High: got %v, want 235.0", p.High)
	}
	if p.Low != 229.0 {
		t.Errorf("Low: got %v, want 229.0", p.Low)
	}
	if p.Close != 234.0 {
		t.Errorf("Close: got %v, want 234.0", p.Close)
	}
	if p.Volume != 50000000 {
		t.Errorf("Volume: got %v, want 50000000", p.Volume)
	}
	if p.Dividend != 0 {
		t.Errorf("Dividend: got %v, want 0 (FD prices are pre-adjusted, no dividends stored)", p.Dividend)
	}
	if p.SplitCoefficient != 1.0 {
		t.Errorf("SplitCoefficient: got %v, want 1.0 (FD prices are pre-adjusted, no splits stored)", p.SplitCoefficient)
	}
}

// makeFDPageJSON creates a JSON array of n FD price records starting from startDate (consecutive days).
func makeFDPageJSON(n int, startDate string) string {
	type fdRecord struct {
		TradingSymbol string  `json:"trading_symbol"`
		Date          string  `json:"date"`
		Open          float64 `json:"open"`
		High          float64 `json:"high"`
		Low           float64 `json:"low"`
		Close         float64 `json:"close"`
		Volume        float64 `json:"volume"`
	}
	d, _ := time.Parse("2006-01-02", startDate)
	records := make([]fdRecord, n)
	for i := range records {
		records[i] = fdRecord{
			TradingSymbol: "TST",
			Date:          d.Format("2006-01-02"),
			Open:          100.0,
			High:          101.0,
			Low:           99.0,
			Close:         100.5,
			Volume:        1000000,
		}
		d = d.AddDate(0, 0, 1)
	}
	b, _ := json.Marshal(records)
	return string(b)
}

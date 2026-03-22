package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/providers/fred"
)

// buildFREDResponse constructs a minimal FRED observations JSON response.
func buildFREDResponse(observations []map[string]string) []byte {
	type obs struct {
		Date  string `json:"date"`
		Value string `json:"value"`
	}
	type resp struct {
		Observations []obs `json:"observations"`
	}
	r := resp{}
	for _, o := range observations {
		r.Observations = append(r.Observations, obs{Date: o["date"], Value: o["value"]})
	}
	b, _ := json.Marshal(r)
	return b
}

// TestFREDParsingCorrect verifies that a valid FRED observation is mapped to ParsedPriceData
// with the correct date, Close == rate, Open=High=Low=Close, Volume=0, Dividend=0, Split=1.0.
func TestFREDParsingCorrect(t *testing.T) {
	t.Parallel()
	observations := []map[string]string{
		{"date": "2024-01-02", "value": "3.97"},
		{"date": "2024-01-03", "value": "4.05"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(buildFREDResponse(observations))
	}))
	defer srv.Close()

	client := fred.NewClient("test-key", srv.URL)
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)

	prices, err := client.GetTreasuryRate(context.Background(), start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(prices) != 2 {
		t.Fatalf("expected 2 prices, got %d", len(prices))
	}

	p := prices[0]
	expectedDate := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	if !p.Date.Equal(expectedDate) {
		t.Errorf("expected date %s, got %s", expectedDate.Format("2006-01-02"), p.Date.Format("2006-01-02"))
	}
	if p.Close != 3.97 {
		t.Errorf("expected Close=3.97, got %v", p.Close)
	}
	if p.Open != p.Close || p.High != p.Close || p.Low != p.Close {
		t.Errorf("expected Open=High=Low=Close=3.97, got Open=%v High=%v Low=%v Close=%v", p.Open, p.High, p.Low, p.Close)
	}
	if p.Volume != 0 {
		t.Errorf("expected Volume=0, got %d", p.Volume)
	}
	if p.Dividend != 0 {
		t.Errorf("expected Dividend=0, got %v", p.Dividend)
	}
	if p.SplitCoefficient != 1.0 {
		t.Errorf("expected SplitCoefficient=1.0, got %v", p.SplitCoefficient)
	}
}

// TestFREDSkipsMissingValues verifies that observations with value "." are excluded.
func TestFREDSkipsMissingValues(t *testing.T) {
	t.Parallel()
	observations := []map[string]string{
		{"date": "2024-01-01", "value": "."},   // holiday — must be skipped
		{"date": "2024-01-02", "value": "4.00"},
		{"date": "2024-01-03", "value": "."},   // another missing day
		{"date": "2024-01-04", "value": "4.10"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(buildFREDResponse(observations))
	}))
	defer srv.Close()

	client := fred.NewClient("test-key", srv.URL)
	prices, err := client.GetTreasuryRate(context.Background(), time.Now(), time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(prices) != 2 {
		t.Fatalf("expected 2 prices (skipping '.'), got %d", len(prices))
	}
	if prices[0].Close != 4.00 || prices[1].Close != 4.10 {
		t.Errorf("unexpected price values: %v", prices)
	}
}

// TestFREDDateRangeInURL verifies that observation_start and observation_end query
// parameters in the request URL match the startDate and endDate arguments.
func TestFREDDateRangeInURL(t *testing.T) {
	t.Parallel()
	var capturedURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedURL = r.URL.String()
		w.Header().Set("Content-Type", "application/json")
		w.Write(buildFREDResponse(nil))
	}))
	defer srv.Close()

	client := fred.NewClient("test-key", srv.URL)
	start := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	_, err := client.GetTreasuryRate(context.Background(), start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(capturedURL, "observation_start=2023-06-01") {
		t.Errorf("expected observation_start=2023-06-01 in URL, got: %s", capturedURL)
	}
	if !strings.Contains(capturedURL, "observation_end=2024-01-31") {
		t.Errorf("expected observation_end=2024-01-31 in URL, got: %s", capturedURL)
	}
}

// TestFREDEmptyKeyGuard verifies that calling GetTreasuryRate with an empty API key
// returns a non-nil error without making any HTTP request.
func TestFREDEmptyKeyGuard(t *testing.T) {
	t.Parallel()
	requestReceived := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := fred.NewClient("", srv.URL)
	_, err := client.GetTreasuryRate(context.Background(), time.Now(), time.Now())
	if err == nil {
		t.Fatal("expected error for empty API key, got nil")
	}
	if requestReceived {
		t.Error("expected no HTTP request when API key is empty")
	}
}

package fred

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/epeers/portfolio/internal/providers"
	log "github.com/sirupsen/logrus"
)

// FRED (Federal Reserve Economic Data) API base URL.
// https://fred.stlouisfed.org/docs/api/fred/
const defaultBaseURL = "https://api.stlouisfed.org/fred"

// Client is an HTTP client for the FRED API.
type Client struct {
	apiKey  string
	baseURL string
	http    *http.Client
}

// NewClient creates a new FRED client. An optional baseURL overrides the default
// (useful for injecting a mock server in tests).
func NewClient(apiKey string, baseURL ...string) *Client {
	base := defaultBaseURL
	if len(baseURL) > 0 {
		base = baseURL[0]
	}
	return &Client{
		apiKey:  apiKey,
		baseURL: base,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetTreasuryRate fetches US 10-year treasury rate (DGS10) from FRED for the given date range.
// Observations with value "." (holidays / missing data days) are skipped.
// Implements providers.TreasuryRateFetcher.
func (c *Client) GetTreasuryRate(ctx context.Context, startDate, endDate time.Time) ([]providers.ParsedPriceData, error) {
	if c.apiKey == "" {
		log.Errorf("FRED: GetTreasuryRate called but FRED_KEY is not configured")
		return nil, fmt.Errorf("fred: API key not configured")
	}

	reqURL := fmt.Sprintf("%s/series/observations?series_id=DGS10&api_key=%s&file_type=json&observation_start=%s&observation_end=%s",
		c.baseURL,
		c.apiKey,
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"),
	)

	log.Debugf("FRED GetTreasuryRate: %s to %s", startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create FRED request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("FRED request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read FRED response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("FRED API returned status %d: %s", resp.StatusCode, string(body[:min(len(body), 200)]))
	}

	var fredResp fredResponse
	if err := json.Unmarshal(body, &fredResp); err != nil {
		return nil, fmt.Errorf("failed to parse FRED response: %w", err)
	}

	var prices []providers.ParsedPriceData
	for _, obs := range fredResp.Observations {
		// Skip missing data (holidays, non-business days)
		if obs.Value == "." {
			continue
		}

		date, err := time.Parse("2006-01-02", obs.Date)
		if err != nil {
			continue
		}

		rate, err := strconv.ParseFloat(obs.Value, 64)
		if err != nil {
			continue
		}

		prices = append(prices, providers.ParsedPriceData{
			Date:             date,
			Open:             rate,
			High:             rate,
			Low:              rate,
			Close:            rate,
			Volume:           0,
			Dividend:         0,
			SplitCoefficient: 1.0,
		})
	}

	if len(prices) > 0 {
		log.Debugf("FRED DGS10: %d rows, first=%s last=%s", len(prices), prices[0].Date.Format("2006-01-02"), prices[len(prices)-1].Date.Format("2006-01-02"))
	}

	return prices, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

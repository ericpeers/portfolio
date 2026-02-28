package financialdata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	log "github.com/sirupsen/logrus"
)

// ErrRateLimited is returned when FinancialData.net responds with HTTP 429.
var ErrRateLimited = errors.New("financialdata: rate limited")

// FinancialData.net API base URL and page size constants.
const defaultBaseURL = "https://financialdata.net/api/v1"
const fdPageSize = 300

// Client is an HTTP client for the FinancialData.net API.
type Client struct {
	apiKey      string
	baseURL     string
	httpClient  *http.Client
	rateLimiter *tokenBucket
}

// tokenBucket enforces a maximum number of requests per interval.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   int
	max      int
	interval time.Duration
	last     time.Time
}

func newTokenBucket(max int, interval time.Duration) *tokenBucket {
	return &tokenBucket{
		tokens:   max,
		max:      max,
		interval: interval,
		last:     time.Now(),
	}
}

func (tb *tokenBucket) wait(ctx context.Context) error {
	for {
		tb.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(tb.last)
		if elapsed >= tb.interval {
			tb.tokens = tb.max
			tb.last = now
		}
		if tb.tokens > 0 {
			tb.tokens--
			tb.mu.Unlock()
			return nil
		}
		waitDur := tb.interval - elapsed
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDur):
		}
	}
}

// NewClient creates a new FinancialData.net client with a 30 req/sec rate limit.
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		rateLimiter: newTokenBucket(30, 1*time.Second),
	}
}

// NewClientWithBaseURL creates a new client with a custom base URL (for testing).
func NewClientWithBaseURL(apiKey, baseURL string) *Client {
	return &Client{
		apiKey:  apiKey,
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		rateLimiter: newTokenBucket(30, 1*time.Second),
	}
}

// routeEndpoint selects the correct FD API endpoint for a security.
// OTC securities use "otc-prices", US stocks use "stock-prices",
// and international stocks use "international-stock-prices".
func (c *Client) routeEndpoint(security *models.SecurityWithCountry) string {
	if strings.Contains(strings.ToUpper(security.ExchangeName), "OTC") {
		return "otc-prices"
	}
	if security.Country == "USA" {
		return "stock-prices"
	}
	return "international-stock-prices"
}

// GetDailyPrices fetches daily OHLCV price data for a security from FinancialData.net.
// Implements providers.StockPriceFetcher.
// FD prices are pre-adjusted; Dividend is always 0 and SplitCoefficient is always 1.0.
func (c *Client) GetDailyPrices(ctx context.Context, security *models.SecurityWithCountry, outputSize string) ([]providers.ParsedPriceData, error) {
	if c.apiKey == "" {
		log.Errorf("FinancialData.net: GetDailyPrices called but FD_KEY is not configured")
		return nil, fmt.Errorf("financialdata: API key not configured")
	}

	endpoint := c.routeEndpoint(security)
	var allPrices []providers.ParsedPriceData

	offset := 0
	for {
		if err := c.rateLimiter.wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limiter cancelled: %w", err)
		}

		reqURL := fmt.Sprintf("%s/%s?key=%s&ticker=%s&offset=%d",
			c.baseURL, endpoint, c.apiKey, security.Symbol, offset)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request failed: %w", err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			return nil, ErrRateLimited
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		var records []fdPriceRecord
		if err := json.Unmarshal(body, &records); err != nil {
			return nil, fmt.Errorf("failed to parse JSON response: %w", err)
		}

		for _, r := range records {
			date, err := time.Parse("2006-01-02", r.Date)
			if err != nil {
				continue
			}
			allPrices = append(allPrices, providers.ParsedPriceData{
				Date:             date,
				Open:             r.Open,
				High:             r.High,
				Low:              r.Low,
				Close:            r.Close,
				Volume:           int64(r.Volume),
				Dividend:         0,
				SplitCoefficient: 1.0,
			})
		}

		// Stop if last page (fewer records than page size) or compact mode
		if len(records) < fdPageSize || outputSize == "compact" {
			break
		}

		offset += fdPageSize
	}

	if len(allPrices) > 0 {
		log.Debugf("FD daily prices %s: %d rows, first=%s last=%s",
			security.Symbol, len(allPrices),
			allPrices[0].Date.Format("2006-01-02"),
			allPrices[len(allPrices)-1].Date.Format("2006-01-02"))
	}

	return allPrices, nil
}

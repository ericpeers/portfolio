package alphavantage

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// ErrRateLimited is returned when AlphaVantage responds with a rate limit message.
var ErrRateLimited = errors.New("alphavantage: rate limited")

// Alphavantage is a Stock and ETF API that fetches data including pricing data
// It is a subscription service, but provides free API access
// https://www.alphavantage.co/documentation/
const defaultBaseURL = "https://www.alphavantage.co/query"

// Client is an HTTP client for the AlphaVantage API
type Client struct {
	apiKey      string
	baseURL     string
	httpClient  *http.Client
	rateLimiter *dualRateLimiter
}

// tokenBucket enforces a maximum number of requests per interval using a token bucket.
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

// Alphavantage says not more than 5 rps, and no more than 75 rpm. But it has weird math. So we will
// limit to 5 rp(1200ms) and 74 rpm.
// Both must have an available token before a request proceeds.
type dualRateLimiter struct {
	burst  *tokenBucket
	minute *tokenBucket
}

func newDualRateLimiter() *dualRateLimiter {
	return &dualRateLimiter{
		burst:  newTokenBucket(5, 1200*time.Millisecond),
		minute: newTokenBucket(74, 60*time.Second),
	}
}

func (dl *dualRateLimiter) wait(ctx context.Context) error {
	if err := dl.minute.wait(ctx); err != nil {
		return err
	}
	return dl.burst.wait(ctx)
}

// NewClient creates a new AlphaVantage client
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		rateLimiter: newDualRateLimiter(),
	}
}

// NewClientWithBaseURL creates a new AlphaVantage client with a custom base URL (for testing)
func NewClientWithBaseURL(apiKey, baseURL string) *Client {
	return &Client{
		apiKey:  apiKey,
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		rateLimiter: newDualRateLimiter(),
	}
}

// GetDailyPrices fetches daily price data for a symbol
func (c *Client) GetDailyPrices(ctx context.Context, symbol string, outputSize string) ([]ParsedPriceData, error) {
	params := url.Values{}
	params.Set("function", "TIME_SERIES_DAILY_ADJUSTED")
	params.Set("symbol", symbol)
	params.Set("outputsize", outputSize) // "compact" or "full"
	params.Set("datatype", "csv")
	params.Set("apikey", c.apiKey)

	log.Debugf("AV request: TIME_SERIES_DAILY_ADJUSTED symbol=%s outputsize=%s", symbol, outputSize)
	body, err := c.doRequest(ctx, params)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(strings.NewReader(string(body)))
	records, err := reader.ReadAll()
	if err != nil {
		log.Errorf("Daily prices CSV parse error. Body (%d bytes): %s", len(body), string(body[:min(len(body), 500)]))
		return nil, fmt.Errorf("failed to parse CSV response: %w", err)
	}
	//log.Debugf("Body response: %s", body)

	if len(records) < 2 {
		return nil, fmt.Errorf("no daily price data returned")
	}

	var prices []ParsedPriceData
	// Skip header row (timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient)
	for _, record := range records[1:] {
		if len(record) < 9 {
			continue
		}

		date, err := time.Parse("2006-01-02", record[0])
		if err != nil {
			continue
		}

		open, _ := strconv.ParseFloat(record[1], 64)
		high, _ := strconv.ParseFloat(record[2], 64)
		low, _ := strconv.ParseFloat(record[3], 64)
		closePrice, _ := strconv.ParseFloat(record[4], 64)
		// skip record[5] (adjusted_close)
		volume, _ := strconv.ParseInt(record[6], 10, 64)
		dividend, _ := strconv.ParseFloat(record[7], 64)
		split, _ := strconv.ParseFloat(record[8], 64)

		prices = append(prices, ParsedPriceData{
			Date:             date,
			Open:             open,
			High:             high,
			Low:              low,
			Close:            closePrice,
			Volume:           volume,
			Dividend:         dividend,
			SplitCoefficient: split,
		})
	}

	if len(prices) > 0 {
		log.Debugf("AV daily prices %s: %d rows, first=%s last=%s", symbol, len(prices), prices[0].Date.Format("2006-01-02"), prices[len(prices)-1].Date.Format("2006-01-02"))
	}

	return prices, nil
}

// GetETFHoldings fetches the holdings of an ETF
func (c *Client) GetETFHoldings(ctx context.Context, symbol string) ([]ParsedETFHolding, error) {
	params := url.Values{}
	params.Set("function", "ETF_PROFILE")
	params.Set("symbol", symbol)
	params.Set("apikey", c.apiKey)

	log.Debugf("AV request: ETF_PROFILE symbol=%s", symbol)
	body, err := c.doRequest(ctx, params)
	if err != nil {
		return nil, err
	}

	var etfResp ETFProfileResponse
	if err := json.Unmarshal(body, &etfResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	var holdings []ParsedETFHolding
	for _, h := range etfResp.Holdings {
		weight, _ := strconv.ParseFloat(h.Weight, 64)
		holdings = append(holdings, ParsedETFHolding{
			Symbol:     h.Symbol,
			Name:       h.Name,
			Percentage: weight,
		})
	}

	return holdings, nil
}

// GetTreasuryRate fetches the US 10-year treasury rate
func (c *Client) GetTreasuryRate(ctx context.Context) ([]ParsedPriceData, error) {
	params := url.Values{}
	params.Set("function", "TREASURY_YIELD")
	params.Set("interval", "daily")
	params.Set("maturity", "10year")
	params.Set("datatype", "csv")
	params.Set("apikey", c.apiKey)

	log.Debug("GetTreasuryRate called")
	body, err := c.doRequest(ctx, params)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(strings.NewReader(string(body)))
	records, err := reader.ReadAll()
	if err != nil {
		log.Errorf("Treasury CSV parse error. Body (%d bytes): %s", len(body), string(body[:min(len(body), 500)]))
		return nil, fmt.Errorf("failed to parse CSV response: %w", err)
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("no treasury rate data returned")
	}

	var prices []ParsedPriceData
	// Skip header row (timestamp,value)
	for _, record := range records[1:] {
		if len(record) < 2 {
			continue
		}

		date, err := time.Parse("2006-01-02", record[0])
		if err != nil {
			continue
		}

		//we sometimes have days with no rate data. Holidays especially. Those will represent as a "." in the CSV
		rate, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			continue
		}

		prices = append(prices, ParsedPriceData{
			Date:   date,
			Open:   0,
			High:   0,
			Low:    0,
			Close:  rate,
			Volume: 0,
		})
	}

	if len(prices) > 0 {
		log.Debugf("AV treasury rate: %d rows, first=%s last=%s", len(prices), prices[0].Date.Format("2006-01-02"), prices[len(prices)-1].Date.Format("2006-01-02"))
	}

	return prices, nil
}

func (c *Client) doRequest(ctx context.Context, params url.Values) ([]byte, error) {
	reqURL := c.baseURL + "?" + params.Encode()

	//if we are running at 5 rps, with a max of 75 rpm, in 15s, we can saturate the rpm bucket.
	//so that means we have 45 seconds remaining. 2^7 => 12800ms == 12.8s. (plus the previous) makes it close to 24s.
	//so we could theoretically set our max retries to 9 and ensure we will get into the next-minute-bucket.
	//but having exponentially growing delays in the 12, 24, 48 second range is too much. we will miss our next
	//minute bucket. So I use a backoff of 10s instead, and add an extra one (possibly two?) in case the math
	//on the server end is not as generous.
	const maxRetries = 7 + 6
	backoff := 200 * time.Millisecond

	for attempt := range maxRetries {
		if err := c.rateLimiter.wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limiter cancelled: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request failed: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
		}

		if bytes.Contains(body, []byte("Burst pattern detected")) {
			log.Warnf("AV rate limited (attempt %d/%d): %s", attempt+1, maxRetries, string(body[:min(len(body), 200)]))
			if attempt+1 >= maxRetries {
				return nil, ErrRateLimited
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				if backoff > (10000 * time.Millisecond) {
					//don't go exponential after 10s. Start adding 10s chunks instead.
					backoff += 10000 * time.Millisecond
				} else {
					backoff *= 2
				}
			}
			continue
		}

		return body, nil
	}

	return nil, ErrRateLimited
}

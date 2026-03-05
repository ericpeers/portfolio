package eodhd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	log "github.com/sirupsen/logrus"
)

const defaultBaseURL = "https://eodhd.com/api"

// exchangeNameToCode maps dim_exchanges.name values to EODHD exchange codes.
// All US securities are handled by the Country=="USA" check in eohdExchangeCode
// and do not need an entry here.
var exchangeNameToCode = map[string]string{
	"London Exchange":                "LSE",
	"Toronto Exchange":               "TO",
	"NEO Exchange":                   "NEO",
	"TSX Venture Exchange":           "V",
	"Berlin Exchange":                "BE",
	"Hamburg Exchange":               "HM",
	"XETRA Stock Exchange":           "XETRA",
	"Dusseldorf Exchange":            "DU",
	"Frankfurt Exchange":             "F",
	"Munich Exchange":                "MU",
	"Stuttgart Exchange":             "STU",
	"Hanover Exchange":               "HA",
	"Luxembourg Stock Exchange":      "LU",
	"Vienna Exchange":                "VI",
	"Euronext Paris":                 "PA",
	"Euronext Brussels":              "BR",
	"Euronext Lisbon":                "LS",
	"Madrid Exchange":                "MC",
	"Euronext Amsterdam":             "AS",
	"SIX Swiss Exchange":             "SW",
	"Stockholm Exchange":             "ST",
	"Oslo Stock Exchange":            "OL",
	"Helsinki Exchange":              "HE",
	"Copenhagen Exchange":            "CO",
	"Iceland Exchange":               "IC",
	"Irish Exchange":                 "IR",
	"Prague Stock Exchange":          "PR",
	"Warsaw Stock Exchange":          "WAR",
	"Budapest Stock Exchange":        "BUD",
	"Athens Exchange":                "AT",
	"Tel Aviv Stock Exchange":        "TA",
	"Australian Securities Exchange": "AU",
	"Korea Stock Exchange":           "KO",
	"KOSDAQ":                         "KQ",
	"Philippine Stock Exchange":      "PSE",
	"Jakarta Exchange":               "JK",
	"Shanghai Stock Exchange":        "SHG",
	"Shenzhen Stock Exchange":        "SHE",
	"Chilean Stock Exchange":         "SN",
	"Egyptian Exchange":              "EGX",
	"Ghana Stock Exchange":           "GSE",
	"Nairobi Securities Exchange":    "XNAI",
	"Nigerian Stock Exchange":        "XNSA",
	"Botswana Stock Exchange":        "XBOT",
}

// Client is an HTTP client for the EODHD API.
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

// NewClient creates a new EODHD client with a 10 req/sec rate limit.
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		rateLimiter: newTokenBucket(10, 1*time.Second),
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
		rateLimiter: newTokenBucket(10, 1*time.Second),
	}
}

// eohdExchangeCode returns the EODHD exchange code for a security.
// USA securities always use "US". For others, looks up ExchangeName in the
// static map; falls back to the raw ExchangeName if not found.
func eohdExchangeCode(security *models.SecurityWithCountry) string {
	if security.Country == "USA" {
		return "US"
	}
	if code, ok := exchangeNameToCode[security.ExchangeName]; ok {
		return code
	}
	return security.ExchangeName
}

// doGet performs a rate-limited GET request and returns the response body.
func (c *Client) doGet(ctx context.Context, url string) ([]byte, error) {
	if err := c.rateLimiter.wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter cancelled: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("EODHD request to %s failed with status %d", url, resp.StatusCode)
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	return body, nil
}

// GetDailyPrices fetches daily OHLCV price data for a security from EODHD.
// Implements providers.StockPriceFetcher.
// Uses AdjustedClose for the Close field; Dividend=0 and SplitCoefficient=1.0
// (events are fetched separately via GetStockEvents).
func (c *Client) GetDailyPrices(ctx context.Context, security *models.SecurityWithCountry, startDT time.Time, endDT time.Time) ([]providers.ParsedPriceData, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("eodhd: API key not configured")
	}

	exchangeCode := eohdExchangeCode(security)
	reqURL := fmt.Sprintf("%s/eod/%s.%s?api_token=%s&fmt=json",
		c.baseURL, security.Ticker, exchangeCode, c.apiKey)

	reqURL += "&from=" + startDT.Format("2006-01-02")
	reqURL += "&to=" + endDT.Format("2006-01-02")

	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, err
	}

	var records []eohdEODRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	if len(records) == 0 {
		log.Warnf("EODHD: no daily price data found for %s.%s", security.Ticker, exchangeCode)
		return nil, fmt.Errorf("no daily price data found for %s", security.Ticker)
	}

	prices := make([]providers.ParsedPriceData, 0, len(records))
	for _, r := range records {
		date, err := time.Parse("2006-01-02", r.Date)
		if err != nil {
			continue
		}
		prices = append(prices, providers.ParsedPriceData{
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

	log.Debugf("Request [%s:%s], Got EODHD daily prices %s.%s: %d rows, first=%s last=%s",
		startDT.Format("2006-01-02"), endDT.Format("2006-01-02"),
		security.Ticker, exchangeCode, len(prices),
		prices[0].Date.Format("2006-01-02"),
		prices[len(prices)-1].Date.Format("2006-01-02"))

	return prices, nil
}

// getDividends fetches all historical dividend records for a security from EODHD.
func (c *Client) getDividends(ctx context.Context, ticker, exchangeCode string) ([]providers.ParsedEventData, error) {
	reqURL := fmt.Sprintf("%s/div/%s.%s?api_token=%s&fmt=json",
		c.baseURL, ticker, exchangeCode, c.apiKey)

	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, err
	}

	var records []eohdDividendRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, fmt.Errorf("failed to parse dividend JSON: %w", err)
	}

	events := make([]providers.ParsedEventData, 0, len(records))
	for _, r := range records {
		date, err := time.Parse("2006-01-02", r.Date)
		if err != nil {
			continue
		}
		events = append(events, providers.ParsedEventData{
			Date:             date,
			Dividend:         r.Value,
			SplitCoefficient: 1.0,
		})
	}
	return events, nil
}

// parseSplitRatio parses an EODHD split string like "4.0000/1.0000" into a coefficient.
// Returns 1.0 on any parse error.
func parseSplitRatio(s string) (float64, error) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return 1.0, fmt.Errorf("Could not parse %s, looking for '/'", s)
	}
	num, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil || num == 0 {
		return 1.0, err
	}
	den, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil || den == 0 {
		return 1.0, err
	}
	return num / den, nil
}

// getSplits fetches all historical split records for a security from EODHD.
func (c *Client) getSplits(ctx context.Context, ticker, exchangeCode string) ([]providers.ParsedEventData, error) {
	reqURL := fmt.Sprintf("%s/splits/%s.%s?api_token=%s&fmt=json",
		c.baseURL, ticker, exchangeCode, c.apiKey)

	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, err
	}

	var records []eohdSplitRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, fmt.Errorf("failed to parse splits JSON: %w", err)
	}

	events := make([]providers.ParsedEventData, 0, len(records))
	for _, r := range records {
		date, err := time.Parse("2006-01-02", r.Date)
		if err != nil {
			log.Errorf("Could not parse date for ticker: %s, %s", ticker, r.Date)
			continue
		}
		coeff, err := parseSplitRatio(r.Split)
		if err != nil {
			log.Errorf("Could not parse split for %s, Split: %s. Error: %s", ticker, r.Split, err)
		} else {
			events = append(events, providers.ParsedEventData{
				Date:             date,
				SplitCoefficient: coeff,
				Dividend:         0,
			})
		}
	}
	return events, nil
}

// GetStockEvents fetches corporate action events (splits and dividends) for a security.
// Implements providers.StockEventFetcher.
func (c *Client) GetStockEvents(ctx context.Context, security *models.SecurityWithCountry) ([]providers.ParsedEventData, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("eodhd: API key not configured")
	}

	exchangeCode := eohdExchangeCode(security)

	var splits, dividends []providers.ParsedEventData
	var splitErr, divErr error

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		splits, splitErr = c.getSplits(ctx, security.Ticker, exchangeCode)
	}()
	go func() {
		defer wg.Done()
		dividends, divErr = c.getDividends(ctx, security.Ticker, exchangeCode)
	}()
	wg.Wait()

	if splitErr != nil {
		return nil, fmt.Errorf("splits fetch failed for %s: %w", security.Ticker, splitErr)
	}
	if divErr != nil {
		return nil, fmt.Errorf("dividends fetch failed for %s: %w", security.Ticker, divErr)
	}

	return providers.MergeEventsByDate(splits, dividends), nil
}

// GetBulkEOD fetches end-of-day prices for all securities on an exchange for a given date.
// Implements providers.BulkPriceFetcher.
func (c *Client) GetBulkEOD(ctx context.Context, exchange string, date time.Time) ([]providers.BulkEODRecord, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("eodhd: API key not configured")
	}

	reqURL := fmt.Sprintf("%s/eod-bulk-last-day/%s?api_token=%s&fmt=json&date=%s",
		c.baseURL, exchange, c.apiKey, date.Format("2006-01-02"))

	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, err
	}

	var raw []eohdBulkEODRecord
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse bulk EOD JSON: %w", err)
	}

	records := make([]providers.BulkEODRecord, 0, len(raw))
	for _, r := range raw {
		date, err := time.Parse("2006-01-02", r.Date)
		if err != nil {
			continue
		}
		// Code from EODHD is "SYMBOL.EXCHANGE" — extract just the ticker.
		ticker := strings.SplitN(r.Code, ".", 2)[0]
		records = append(records, providers.BulkEODRecord{
			Code:     ticker,
			Date:     date,
			Open:     r.Open,
			High:     r.High,
			Low:      r.Low,
			Close:    r.Close,
			AdjClose: r.AdjustedClose,
			Volume:   int64(r.Volume),
		})
	}
	return records, nil
}

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
		// Continuous proportional replenishment: add tokens proportional to elapsed
		// time rather than refilling all at once. This allows an initial burst up to
		// max capacity while preventing a long dead-stall after the burst drains it.
		added := int(float64(tb.max) * elapsed.Seconds() / tb.interval.Seconds())
		if added > 0 {
			tb.tokens += added
			if tb.tokens > tb.max {
				tb.tokens = tb.max
			}
			tb.last = now
		}
		if tb.tokens > 0 {
			tb.tokens--
			tb.mu.Unlock()
			return nil
		}
		// Compute wait until at least 1 token is available.
		tokenDur := tb.interval / time.Duration(tb.max)
		//log.Debugf("[RATE LIMIT STALL] bucket empty, waiting %v for next token", tokenDur)
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tokenDur):
		}
	}
}

// NewClient creates a new EODHD client with a 16 req/sec rate limit (960/min, evenly distributed).
// An optional baseURL overrides the default (useful for injecting a mock server in tests).
func NewClient(apiKey string, baseURL ...string) *Client {
	base := defaultBaseURL
	if len(baseURL) > 0 {
		base = baseURL[0]
	}
	return &Client{
		apiKey:  apiKey,
		baseURL: base,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		rateLimiter: newTokenBucket(16, time.Second),
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
// On HTTP 429 it backs off exponentially (10s, 20s, 40s, 80s) and retries up to 4 times
// before returning an error. All other non-200 responses are returned immediately.
func (c *Client) doGet(ctx context.Context, url string) ([]byte, error) {
	if err := c.rateLimiter.wait(ctx); err != nil {
		return nil, fmt.Errorf("request cancelled (by parent) while waiting for rate limit slot: %w", err)
	}

	const maxRetries = 4
	backoff := 10 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request failed: %w", err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close() // #nosec G104 -- discarding body on error path (idiomatic Go)
			if attempt == maxRetries {
				log.Errorf("[EODHD THROTTLED 429] giving up after %d retries: url=%s", maxRetries, url)
				return nil, fmt.Errorf("EODHD rate limit exceeded after %d retries: %s", maxRetries, url)
			}
			log.Warnf("[EODHD 429] attempt %d/%d, backing off %v before retry: %s",
				attempt+1, maxRetries, backoff, url)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close() // #nosec G104 -- discarding body on error path (idiomatic Go)
			log.Errorf("EODHD request to %s failed with status %d", url, resp.StatusCode)
			return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close() // #nosec G104 -- error from Close after ReadAll is intentionally discarded (idiomatic Go)
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}
		return body, nil
	}

	// unreachable: the attempt==maxRetries check inside the loop always returns first
	return nil, fmt.Errorf("EODHD: exhausted retries for %s", url)
}

// GetDailyPrices fetches daily OHLCV price data for a security from EODHD.
// Implements providers.StockPriceFetcher.
// Uses unadjusted Close for the Close field; Dividend=0 and SplitCoefficient=1.0
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

	fetchStart := time.Now()
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, err
	}
	fetchEnd := time.Now()

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

	log.Debugf("EODHD Request [%s:%s] %s.%s: %d rows, first=%s last=%s req: %.2fms, parse: %.2fms",
		startDT.Format("2006-01-02"), endDT.Format("2006-01-02"),
		security.Ticker, exchangeCode, len(prices),
		prices[0].Date.Format("2006-01-02"),
		prices[len(prices)-1].Date.Format("2006-01-02"),
		float64(fetchEnd.Sub(fetchStart))/float64(time.Millisecond),
		float64(time.Since(fetchEnd))/float64(time.Millisecond),
	)

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
		return 1.0, fmt.Errorf("could not parse %s, looking for '/'", s)
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

	fetchStart := time.Now()
	body, err := c.doGet(ctx, reqURL)
	fetchEnd := time.Now()
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
	log.Debugf("EODHD BulkEOD [%s:%s]: %d records, req: %.2fms, parse: %.2fms",
		exchange, date.Format("2006-01-02"), len(records),
		float64(fetchEnd.Sub(fetchStart))/float64(time.Millisecond),
		float64(time.Since(fetchEnd))/float64(time.Millisecond),
	)
	return records, nil
}

// GetBulkSplits fetches split events for all securities on an exchange for a given date.
// Implements providers.BulkEventFetcher.
func (c *Client) GetBulkSplits(ctx context.Context, exchange string, date time.Time) ([]providers.BulkEventRecord, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("eodhd: API key not configured")
	}

	reqURL := fmt.Sprintf("%s/eod-bulk-last-day/%s?api_token=%s&fmt=json&date=%s&type=splits",
		c.baseURL, exchange, c.apiKey, date.Format("2006-01-02"))

	fetchStart := time.Now()
	body, err := c.doGet(ctx, reqURL)
	fetchEnd := time.Now()
	if err != nil {
		return nil, err
	}

	var raw []eohdBulkSplitRecord
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse bulk splits JSON: %w", err)
	}

	records := make([]providers.BulkEventRecord, 0, len(raw))
	for _, r := range raw {
		d, err := time.Parse("2006-01-02", r.Date)
		if err != nil {
			continue
		}
		coeff, err := parseSplitRatio(r.Split)
		if err != nil {
			log.Errorf("bulk splits: could not parse split ratio %q for %s: %v", r.Split, r.Code, err)
			continue
		}
		records = append(records, providers.BulkEventRecord{
			Code:             r.Code,
			Date:             d,
			SplitCoefficient: coeff,
		})
	}
	log.Debugf("EODHD BulkSplits [%s:%s]: %d records, req: %.2fms, parse: %.2fms",
		exchange, date.Format("2006-01-02"), len(records),
		float64(fetchEnd.Sub(fetchStart))/float64(time.Millisecond),
		float64(time.Since(fetchEnd))/float64(time.Millisecond),
	)
	return records, nil
}

// GetBulkDividends fetches dividend events for all securities on an exchange for a given date.
// Implements providers.BulkEventFetcher.
func (c *Client) GetBulkDividends(ctx context.Context, exchange string, date time.Time) ([]providers.BulkEventRecord, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("eodhd: API key not configured")
	}

	reqURL := fmt.Sprintf("%s/eod-bulk-last-day/%s?api_token=%s&fmt=json&date=%s&type=dividends",
		c.baseURL, exchange, c.apiKey, date.Format("2006-01-02"))

	fetchStart := time.Now()
	body, err := c.doGet(ctx, reqURL)
	fetchEnd := time.Now()
	if err != nil {
		return nil, err
	}

	var raw []eohdBulkDividendRecord
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse bulk dividends JSON: %w", err)
	}

	records := make([]providers.BulkEventRecord, 0, len(raw))
	for _, r := range raw {
		d, err := time.Parse("2006-01-02", r.Date)
		if err != nil {
			continue
		}
		dividend, err := strconv.ParseFloat(r.Dividend, 64)
		if err != nil {
			log.Errorf("bulk dividends: could not parse dividend %q for %s: %v", r.Dividend, r.Code, err)
			continue
		}
		records = append(records, providers.BulkEventRecord{
			Code:             r.Code,
			Date:             d,
			Dividend:         dividend,
			SplitCoefficient: 1.0,
		})
	}
	log.Debugf("EODHD BulkDividends [%s:%s]: %d records, req: %.2fms, parse: %.2fms",
		exchange, date.Format("2006-01-02"), len(records),
		float64(fetchEnd.Sub(fetchStart))/float64(time.Millisecond),
		float64(time.Since(fetchEnd))/float64(time.Millisecond),
	)
	return records, nil
}

// GetExchangeList fetches the full list of exchanges available in EODHD.
// Implements providers.SecurityListFetcher.
func (c *Client) GetExchangeList(ctx context.Context) ([]providers.ExchangeInfo, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("eodhd: API key not configured")
	}

	reqURL := fmt.Sprintf("%s/exchanges-list/?api_token=%s&fmt=json", c.baseURL, c.apiKey)
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch exchange list: %w", err)
	}

	var raw []eohdExchangeRecord
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse exchange list JSON: %w", err)
	}

	exchanges := make([]providers.ExchangeInfo, 0, len(raw))
	for _, r := range raw {
		exchanges = append(exchanges, providers.ExchangeInfo{
			Code:        r.Code,
			Name:        r.Name,
			Country:     r.Country,
			Currency:    r.Currency,
			CountryISO2: r.CountryISO2,
			CountryISO3: r.CountryISO3,
		})
	}
	log.Debugf("EODHD GetExchangeList: %d exchanges", len(exchanges))
	return exchanges, nil
}

// GetExchangeSymbolList fetches all active securities listed on a given exchange from EODHD.
// Implements providers.SecurityListFetcher.
func (c *Client) GetExchangeSymbolList(ctx context.Context, exchangeCode string) ([]providers.SymbolRecord, error) {
	return c.getSymbolList(ctx, exchangeCode, false)
}

// GetExchangeSymbolListDelisted fetches all delisted securities for a given exchange from EODHD.
// Implements providers.SecurityListFetcher.
func (c *Client) GetExchangeSymbolListDelisted(ctx context.Context, exchangeCode string) ([]providers.SymbolRecord, error) {
	return c.getSymbolList(ctx, exchangeCode, true)
}

func (c *Client) getSymbolList(ctx context.Context, exchangeCode string, fetchDelisted bool) ([]providers.SymbolRecord, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("eodhd: API key not configured")
	}

	reqURL := fmt.Sprintf("%s/exchange-symbol-list/%s?api_token=%s&fmt=json", c.baseURL, exchangeCode, c.apiKey)
	if fetchDelisted {
		reqURL += "&delisted=1"
	}
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch symbol list for %s: %w", exchangeCode, err)
	}

	var raw []eohdSymbolRecord
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse symbol list JSON for %s: %w", exchangeCode, err)
	}

	symbols := make([]providers.SymbolRecord, 0, len(raw))
	for _, r := range raw {
		symbols = append(symbols, providers.SymbolRecord{
			Ticker:   r.Code,
			Name:     r.Name,
			Country:  r.Country,
			Exchange: r.Exchange,
			Currency: r.Currency,
			Type:     r.Type,
			Isin:     r.Isin,
		})
	}
	log.Debugf("EODHD getSymbolList [%s] delisted=%v: %d symbols", exchangeCode, fetchDelisted, len(symbols))
	return symbols, nil
}

// GetFundamentals fetches fundamental data for a single security from EODHD.
// Implements providers.FundamentalsFetcher.
// ticker and exchangeCode follow EODHD conventions (e.g. "AAPL", "US").
func (c *Client) GetFundamentals(ctx context.Context, cand models.BackfillCandidate) (*providers.ParsedFundamentals, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("eodhd: API key not configured")
	}

	reqURL := fmt.Sprintf("%s/fundamentals/%s.%s?api_token=%s&fmt=json",
		c.baseURL, cand.Ticker, cand.ExchangeCode, c.apiKey)

	fetchStart := time.Now()
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("fundamentals fetch failed for %s.%s: %w", cand.Ticker, cand.ExchangeCode, err)
	}
	fetchEnd := time.Now()

	var raw eohdFundamentalsResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("fundamentals parse failed for %s.%s: %w", cand.Ticker, cand.ExchangeCode, err)
	}

	pf := parseFundamentals(&raw)
	log.Debugf("EODHD GetFundamentals [%s.%s] (%d): %d history rows, %d listings, req: %.2fms, parse: %.2fms",
		cand.Ticker, cand.ExchangeCode, cand.SecurityID, len(pf.History), len(pf.Listings),
		float64(fetchEnd.Sub(fetchStart))/float64(time.Millisecond),
		float64(time.Since(fetchEnd))/float64(time.Millisecond),
	)
	return pf, nil
}

// ParseFundamentalsJSON parses raw EODHD fundamentals JSON without making an HTTP request.
// Useful for importing cached or test JSON files; does not require a configured Client.
func ParseFundamentalsJSON(data []byte) (*providers.ParsedFundamentals, error) {
	var raw eohdFundamentalsResponse
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("fundamentals parse failed: %w", err)
	}
	return parseFundamentals(&raw), nil
}

// ParseEarningsCalendarJSON parses raw EODHD earnings calendar JSON without making an HTTP
// request. Useful for importing cached or test JSON files; does not require a configured Client.
func ParseEarningsCalendarJSON(data []byte) ([]providers.EarningsAnnouncement, error) {
	var raw eohdEarningsCalendarResponse
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("earnings calendar parse failed: %w", err)
	}
	return parseEarningsCalendar(raw.Earnings), nil
}

// parseEarningsCalendar converts raw entries into EarningsAnnouncement slices.
// Entries with unparseable dates or empty codes are skipped with a warning.
func parseEarningsCalendar(entries []eohdEarningsCalendarEntry) []providers.EarningsAnnouncement {
	results := make([]providers.EarningsAnnouncement, 0, len(entries))
	for _, e := range entries {
		if e.Code == "" || e.ReportDate == "" {
			continue
		}
		reportDate, err := time.Parse("2006-01-02", e.ReportDate)
		if err != nil {
			log.Warnf("[EODHD earnings calendar] skipping unparseable date %q for %s", e.ReportDate, e.Code)
			continue
		}
		parts := strings.SplitN(e.Code, ".", 2)
		ticker := parts[0]
		exchange := ""
		if len(parts) == 2 {
			exchange = parts[1]
		}
		results = append(results, providers.EarningsAnnouncement{
			Ticker:       ticker,
			ExchangeCode: exchange,
			ReportDate:   reportDate,
		})
	}
	return results
}

// stripExchangeSuffix removes the trailing exchange segment from an EODHD PrimaryTicker.
// "NVDA.US" → "NVDA", "BRK.B.US" → "BRK.B", "NVDA" → "NVDA".
func stripExchangeSuffix(primaryTicker string) string {
	if i := strings.LastIndex(primaryTicker, "."); i > 0 {
		return primaryTicker[:i]
	}
	return primaryTicker
}

func coalesceStr(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// parseFundamentals converts the raw EODHD response into the provider-level ParsedFundamentals.
func parseFundamentals(raw *eohdFundamentalsResponse) *providers.ParsedFundamentals {
	// Derive the bare ticker to match dim_security.ticker convention used everywhere else.
	// Common stocks carry PrimaryTicker ("NVDA.US") — strip the exchange suffix.
	// ETF JSON omits PrimaryTicker entirely; fall back to Code ("SPY"), which needs no stripping.
	ticker := stripExchangeSuffix(raw.General.PrimaryTicker)
	if ticker == "" {
		ticker = raw.General.Code
	}
	// ETFs place ISIN in ETF_Data.ISIN rather than General.ISIN.
	// Mutual funds include ISIN in General directly.
	isin := raw.General.ISIN
	if isin == "" {
		isin = raw.ETFData.ISIN
	}

	// Mutual funds use General.Fund_Summary instead of General.Description.
	description := raw.General.Description
	if description == "" {
		description = raw.General.FundSummary
	}

	pf := &providers.ParsedFundamentals{
		Ticker:         ticker,
		ExchangeName:   raw.General.Exchange,
		Code:           raw.General.Code,
		ISIN:           isin,
		CIK:            raw.General.CIK,
		CUSIP:          raw.General.CUSIP,
		LEI:            raw.General.LEI,
		Description:    description,
		Employees:      raw.General.Employees,
		FiscalYearEnd:  coalesceStr(raw.General.FiscalYearEnd, raw.General.FiscalYearEndMF),
		GicSector:      raw.General.GicSector,
		GicGroup:       raw.General.GicGroup,
		GicIndustry:    raw.General.GicIndustry,
		GicSubIndustry: raw.General.GicSubIndustry,
		// General.Sector and General.Industry are intentionally excluded.
		// They are EODHD's own flat taxonomy (Yahoo Finance-derived) with no
		// standardized definition. The four GIC fields above are the MSCI/S&P
		// standard and cover everything these would provide, at higher granularity.
		// Storing EODHD-specific labels would couple the schema to one provider's
		// taxonomy and add nothing GIC doesn't already give us.
	}

	// Trim the ISO code to 2 chars — EODHD returns "US" but sometimes longer strings.
	iso := strings.TrimSpace(raw.General.CountryISO)
	if len(iso) > 2 {
		iso = iso[:2]
	}
	pf.CountryISO = iso

	// Stocks: General.IPODate. ETFs: ETF_Data.Inception_Date. Mutual funds: MutualFund_Data.Inception_Date.
	ipoStr := raw.General.IPODate
	if ipoStr == "" {
		ipoStr = raw.ETFData.InceptionDate
	}
	if ipoStr == "" {
		ipoStr = raw.MutualFundData.InceptionDate
	}
	if ipoStr != "" {
		if t, err := time.Parse("2006-01-02", ipoStr); err == nil {
			pf.IPODate = &t
		}
	}

	// Stocks use General.WebURL; ETFs use ETF_Data.Company_URL; mutual funds have no URL field.
	pf.URL = raw.General.WebURL
	if pf.URL == "" {
		pf.URL = raw.ETFData.CompanyURL
	}
	pf.ETFURL = raw.ETFData.ETFURL

	// Expense ratio: ETF_Data.NetExpenseRatio, else MutualFund_Data.Expense_Ratio.
	expenseStr := raw.ETFData.NetExpenseRatio
	if expenseStr == "" {
		expenseStr = raw.MutualFundData.ExpenseRatio
	}
	if expenseStr != "" {
		if v, err := strconv.ParseFloat(expenseStr, 64); err == nil {
			pf.NetExpenseRatio = &v
		}
	}

	// Total assets: ETF_Data.TotalAssets, else MutualFund_Data.Share_Class_Net_Assets.
	assetsStr := raw.ETFData.TotalAssets
	if assetsStr == "" {
		assetsStr = raw.MutualFundData.ShareClassNetAssets
	}
	if assetsStr != "" {
		if v, err := strconv.ParseFloat(assetsStr, 64); err == nil {
			n := int64(v)
			pf.TotalAssets = &n
		}
	}

	// Yield: ETF_Data.Yield, else MutualFund_Data.Yield.
	yieldStr := raw.ETFData.Yield
	if yieldStr == "" {
		yieldStr = raw.MutualFundData.Yield
	}
	if yieldStr != "" {
		if v, err := strconv.ParseFloat(yieldStr, 64); err == nil {
			pf.ETFYield = &v
		}
	}

	// NAV: MutualFund_Data only; ETFs don't expose NAV via EODHD.
	if raw.MutualFundData.Nav != "" {
		if v, err := strconv.ParseFloat(raw.MutualFundData.Nav, 64); err == nil {
			pf.NAV = &v
		}
	}

	for _, l := range raw.General.Listings {
		if l.Code == "" || l.Exchange == "" {
			continue
		}
		pf.Listings = append(pf.Listings, providers.ParsedSecurityListing{
			ExchangeCode: l.Exchange,
			TickerCode:   l.Code,
			Name:         l.Name,
		})
	}

	pf.Snapshot = providers.ParsedFundamentalsSnapshot{
		// Price-derived fields (market_cap, pe_ratio, forward_pe, price_sales_ttm,
		// beta, week_52_high/low, ma_50/ma_200, dividend_yield) are not stored —
		// computed on demand from fact_price instead.
		PEGRatio:                raw.Highlights.PEGRatio,
		EpsTTM:                  raw.Highlights.EarningsShare,
		RevenueTTM:              raw.Highlights.RevenueTTM,
		EBITDA:                  raw.Highlights.EBITDA,
		ProfitMargin:            raw.Highlights.ProfitMargin,
		OperatingMarginTTM:      raw.Highlights.OperatingMarginTTM,
		ReturnOnAssetsTTM:       raw.Highlights.ReturnOnAssetsTTM,
		ReturnOnEquityTTM:       raw.Highlights.ReturnOnEquityTTM,
		RevenuePerShareTTM:      raw.Highlights.RevenuePerShareTTM,
		DividendPerShare:        raw.Highlights.DividendShare,
		QuarterlyEarningsGrowth: raw.Highlights.QuarterlyEarningsGrowthYOY,
		QuarterlyRevenueGrowth:  raw.Highlights.QuarterlyRevenueGrowthYOY,
		EpsEstimateCurrentYear:  raw.Highlights.EpsEstimateCurrentYear,
		EpsEstimateNextYear:     raw.Highlights.EpsEstimateNextYear,
		EnterpriseValue:         raw.Valuation.EnterpriseValue,
		PriceBookMRQ:            raw.Valuation.PriceBookMRQ,
		EvEBITDA:                raw.Valuation.EnterpriseValueEbitda,
		EvRevenue:               raw.Valuation.EnterpriseValueRevenue,
		SharesShort:             raw.Technicals.SharesShort,
		ShortPercent:            raw.Technicals.ShortPercent,
		ShortRatio:              raw.Technicals.ShortRatio,
		SharesOutstanding:       raw.SharesStats.SharesOutstanding,
		SharesFloat:             raw.SharesStats.SharesFloat,
		PercentInsiders:         raw.SharesStats.PercentInsiders,
		PercentInstitutions:     raw.SharesStats.PercentInstitutions,
		// AnalystRatings.Rating is intentionally excluded. EODHD computes it as a
		// weighted mean with StrongBuy=5 and StrongSell=1 (higher = more bullish),
		// which is the opposite of the industry-standard scale (StrongBuy=1, lower
		// = more bullish). Storing it would silently mislead any caller that assumes
		// the standard convention. Callers can derive a standard-scale rating from
		// the raw vote counts at query time.
		AnalystTargetPrice: raw.AnalystRatings.TargetPrice,
		AnalystStrongBuy:   raw.AnalystRatings.StrongBuy,
		AnalystBuy:         raw.AnalystRatings.Buy,
		AnalystHold:        raw.AnalystRatings.Hold,
		AnalystSell:        raw.AnalystRatings.Sell,
		AnalystStrongSell:  raw.AnalystRatings.StrongSell,
	}

	if raw.Highlights.MostRecentQuarter != "" {
		if t, err := time.Parse("2006-01-02", raw.Highlights.MostRecentQuarter); err == nil {
			pf.Snapshot.MostRecentQuarter = &t
		}
	}

	pf.History = append(pf.History, parseEarningsHistory(raw.Earnings.HistoryRaw)...)
	pf.History = append(pf.History, parseEarningsAnnual(raw.Earnings.AnnualRaw)...)

	return pf
}

// parseEarningsHistory converts the Earnings.History map into ParsedFinancialsRow slices.
// Returns nil if the raw JSON is not a JSON object (e.g. when EODHD sends []).
func parseEarningsHistory(raw json.RawMessage) []providers.ParsedFinancialsRow {
	if len(raw) == 0 || raw[0] != '{' {
		return nil
	}
	var m map[string]eohdEarningsHistoryEntry
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	rows := make([]providers.ParsedFinancialsRow, 0, len(m))
	for _, e := range m {
		t, err := time.Parse("2006-01-02", e.Date)
		if err != nil {
			continue
		}
		row := providers.ParsedFinancialsRow{
			PeriodEnd:       t,
			PeriodType:      "Q",
			EpsActual:       e.EpsActual,
			EpsEstimate:     e.EpsEstimate,
			EpsDifference:   e.EpsDifference,
			SurprisePercent: e.SurprisePercent,
		}
		if e.ReportDate != "" {
			if rd, err := time.Parse("2006-01-02", e.ReportDate); err == nil {
				row.ReportDate = &rd
			}
		}
		if e.BeforeAfterMarket != "" {
			s := e.BeforeAfterMarket
			row.BeforeAfterMarket = &s
		}
		rows = append(rows, row)
	}
	return rows
}

// parseEarningsAnnual converts the Earnings.Annual map into ParsedFinancialsRow slices.
func parseEarningsAnnual(raw json.RawMessage) []providers.ParsedFinancialsRow {
	if len(raw) == 0 || raw[0] != '{' {
		return nil
	}
	var m map[string]eohdEarningsAnnualEntry
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	rows := make([]providers.ParsedFinancialsRow, 0, len(m))
	for _, e := range m {
		t, err := time.Parse("2006-01-02", e.Date)
		if err != nil {
			continue
		}
		rows = append(rows, providers.ParsedFinancialsRow{
			PeriodEnd:  t,
			PeriodType: "A",
			EpsActual:  e.EpsActual,
		})
	}
	return rows
}


// GetUpcomingEarnings fetches upcoming earnings announcement dates from EODHD for the
// given date range. Implements providers.EarningsCalendarFetcher.
// The EODHD endpoint returns all securities with scheduled earnings in the window;
// securities outside the window (or with no scheduled date) are simply absent.
func (c *Client) GetUpcomingEarnings(ctx context.Context, from, to time.Time) ([]providers.EarningsAnnouncement, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("eodhd: API key not configured")
	}

	reqURL := fmt.Sprintf("%s/calendar/earnings?api_token=%s&fmt=json&from=%s&to=%s",
		c.baseURL, c.apiKey, from.Format("2006-01-02"), to.Format("2006-01-02"))

	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf("earnings calendar fetch failed: %w", err)
	}

	var raw eohdEarningsCalendarResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("earnings calendar parse failed: %w", err)
	}

	results := parseEarningsCalendar(raw.Earnings)
	log.Debugf("EODHD GetUpcomingEarnings [%s → %s]: %d announcements",
		from.Format("2006-01-02"), to.Format("2006-01-02"), len(results))
	return results, nil
}

// GetBulkEvents fetches splits and dividends for all securities on an exchange for a given date,
// merging records that share the same ticker and date.
// Implements providers.BulkEventFetcher.
func (c *Client) GetBulkEvents(ctx context.Context, exchange string, date time.Time) ([]providers.BulkEventRecord, error) {
	var splits, dividends []providers.BulkEventRecord
	var splitErr, divErr error

	fetchStart := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		splits, splitErr = c.GetBulkSplits(ctx, exchange, date)
	}()
	go func() {
		defer wg.Done()
		dividends, divErr = c.GetBulkDividends(ctx, exchange, date)
	}()
	wg.Wait()
	fetchEnd := time.Now()

	if splitErr != nil {
		return nil, fmt.Errorf("bulk splits fetch failed: %w", splitErr)
	}
	if divErr != nil {
		return nil, fmt.Errorf("bulk dividends fetch failed: %w", divErr)
	}

	type eventKey struct {
		code string
		date time.Time
	}
	merged := make(map[eventKey]providers.BulkEventRecord, len(splits)+len(dividends))
	for _, s := range splits {
		key := eventKey{s.Code, s.Date}
		e := merged[key]
		e.Code = s.Code
		e.Date = s.Date
		e.SplitCoefficient = s.SplitCoefficient
		merged[key] = e
	}
	for _, d := range dividends {
		key := eventKey{d.Code, d.Date}
		e := merged[key]
		e.Code = d.Code
		e.Date = d.Date
		if e.SplitCoefficient == 0 {
			e.SplitCoefficient = 1.0
		}
		e.Dividend = d.Dividend
		merged[key] = e
	}

	result := make([]providers.BulkEventRecord, 0, len(merged))
	for _, e := range merged {
		result = append(result, e)
	}
	log.Debugf("EODHD BulkEvents [%s:%s]: %d splits, %d dividends, %d merged, fetch: %.2fms, merge: %.2fms",
		exchange, date.Format("2006-01-02"), len(splits), len(dividends), len(result),
		float64(fetchEnd.Sub(fetchStart))/float64(time.Millisecond),
		float64(time.Since(fetchEnd))/float64(time.Millisecond),
	)
	return result, nil
}

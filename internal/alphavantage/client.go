package alphavantage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const baseURL = "https://www.alphavantage.co/query"

// Client is an HTTP client for the AlphaVantage API
type Client struct {
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new AlphaVantage client
func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetDailyPrices fetches daily price data for a symbol
func (c *Client) GetDailyPrices(ctx context.Context, symbol string, outputSize string) ([]ParsedPriceData, error) {
	params := url.Values{}
	params.Set("function", "TIME_SERIES_DAILY")
	params.Set("symbol", symbol)
	params.Set("outputsize", outputSize) // "compact" or "full"
	params.Set("apikey", c.apiKey)

	resp, err := c.doRequest(ctx, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var tsResp TimeSeriesDailyResponse
	if err := json.Unmarshal(body, &tsResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	var prices []ParsedPriceData
	for dateStr, ohlcv := range tsResp.TimeSeries {
		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue
		}

		open, _ := strconv.ParseFloat(ohlcv.Open, 64)
		high, _ := strconv.ParseFloat(ohlcv.High, 64)
		low, _ := strconv.ParseFloat(ohlcv.Low, 64)
		closePrice, _ := strconv.ParseFloat(ohlcv.Close, 64)
		volume, _ := strconv.ParseInt(ohlcv.Volume, 10, 64)

		prices = append(prices, ParsedPriceData{
			Date:   date,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  closePrice,
			Volume: volume,
		})
	}

	return prices, nil
}

// GetQuote fetches a real-time quote for a symbol
func (c *Client) GetQuote(ctx context.Context, symbol string) (*ParsedQuote, error) {
	params := url.Values{}
	params.Set("function", "GLOBAL_QUOTE")
	params.Set("symbol", symbol)
	params.Set("apikey", c.apiKey)

	resp, err := c.doRequest(ctx, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var quoteResp GlobalQuoteResponse
	if err := json.Unmarshal(body, &quoteResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	price, err := strconv.ParseFloat(quoteResp.GlobalQuote.Price, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse price: %w", err)
	}

	return &ParsedQuote{
		Symbol: symbol,
		Price:  price,
	}, nil
}

// GetETFHoldings fetches the holdings of an ETF
func (c *Client) GetETFHoldings(ctx context.Context, symbol string) ([]ParsedETFHolding, error) {
	params := url.Values{}
	params.Set("function", "ETF_PROFILE")
	params.Set("symbol", symbol)
	params.Set("apikey", c.apiKey)

	resp, err := c.doRequest(ctx, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
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
			Percentage: weight * 100, // Convert to percentage
		})
	}

	return holdings, nil
}

// GetTreasuryRate fetches the US 10-year treasury rate
func (c *Client) GetTreasuryRate(ctx context.Context) ([]ParsedTreasuryRate, error) {
	params := url.Values{}
	params.Set("function", "TREASURY_YIELD")
	params.Set("interval", "daily")
	params.Set("maturity", "10year")
	params.Set("apikey", c.apiKey)

	resp, err := c.doRequest(ctx, params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var treasuryResp TreasuryYieldResponse
	if err := json.Unmarshal(body, &treasuryResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	var rates []ParsedTreasuryRate
	for _, dp := range treasuryResp.Data {
		date, err := time.Parse("2006-01-02", dp.Date)
		if err != nil {
			continue
		}

		rate, err := strconv.ParseFloat(dp.Value, 64)
		if err != nil {
			continue
		}

		rates = append(rates, ParsedTreasuryRate{
			Date: date,
			Rate: rate,
		})
	}

	return rates, nil
}

func (c *Client) doRequest(ctx context.Context, params url.Values) (*http.Response, error) {
	reqURL := baseURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	return resp, nil
}

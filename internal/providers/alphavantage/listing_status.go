package alphavantage

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/epeers/portfolio/internal/providers"
	log "github.com/sirupsen/logrus"
)

// GetListingStatus fetches and parses the LISTING_STATUS CSV from AlphaVantage.
// Implements providers.ListingStatusFetcher.
func (c *Client) GetListingStatus(ctx context.Context, state string) ([]providers.ListingStatusEntry, error) {
	if c.apiKey == "" {
		log.Errorf("AlphaVantage: GetListingStatus called but AV_KEY is not configured")
		return nil, fmt.Errorf("alphavantage: API key not configured")
	}

	log.Debugf("GetListingStatus begins (from Alphavantage)")
	params := url.Values{}
	params.Set("function", "LISTING_STATUS")
	params.Set("apikey", c.apiKey)
	params.Set("state", state)

	body, err := c.doRequest(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch listing status: %w", err)
	}

	log.Debug("GetListingStatus ends (from AV)")
	return parseListingStatusCSV(bytes.NewReader(body))
}

// parseListingStatusCSV parses the CSV response from LISTING_STATUS endpoint
// Expected columns: symbol,name,exchange,assetType,ipoDate,delistingDate,status
func parseListingStatusCSV(r io.Reader) ([]providers.ListingStatusEntry, error) {
	reader := csv.NewReader(r)

	// Read header row
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Build column index map
	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[col] = i
	}

	// Verify required columns exist
	requiredCols := []string{"symbol", "name", "exchange", "assetType", "ipoDate", "delistingDate", "status"}
	for _, col := range requiredCols {
		if _, ok := colIdx[col]; !ok {
			return nil, fmt.Errorf("missing required column: %s", col)
		}
	}

	var entries []providers.ListingStatusEntry
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV record: %w", err)
		}

		entry := providers.ListingStatusEntry{
			Symbol:    record[colIdx["symbol"]],
			Name:      record[colIdx["name"]],
			Exchange:  record[colIdx["exchange"]],
			AssetType: record[colIdx["assetType"]],
			Status:    record[colIdx["status"]],
		}

		// Parse IPO date
		if ipoStr := record[colIdx["ipoDate"]]; ipoStr != "" && ipoStr != "null" {
			if t, err := time.Parse("2006-01-02", ipoStr); err == nil {
				entry.IPODate = &t
			}
		}

		// Parse delisting date
		if delistStr := record[colIdx["delistingDate"]]; delistStr != "" && delistStr != "null" {
			if t, err := time.Parse("2006-01-02", delistStr); err == nil {
				entry.DelistingDate = &t
			}
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

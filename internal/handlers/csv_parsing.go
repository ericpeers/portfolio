package handlers

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
)

// ParsedSecurityRow represents one row from a securities import CSV.
// Country is used only for auto-creating missing exchanges.
type ParsedSecurityRow struct {
	Ticker   string
	Name     string
	Exchange string // raw exchange name from CSV
	Type     string // raw type from CSV
	Currency string
	ISIN     string
	Country  string // used only for exchange auto-creation
}

// ParseSecuritiesCSV parses a securities import CSV into a slice of ParsedSecurityRow.
// Required columns: ticker, name, exchange, type
// Optional columns: currency, isin, country (missing columns default to "")
// Rows with an empty ticker are skipped.
func ParseSecuritiesCSV(r io.Reader) ([]ParsedSecurityRow, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[strings.ToLower(strings.TrimSpace(col))] = i
	}

	for _, col := range []string{"ticker", "name", "exchange", "type"} {
		if _, ok := colIdx[col]; !ok {
			return nil, fmt.Errorf("missing required column: %s", col)
		}
	}

	optionalCol := func(record []string, col string) string {
		idx, ok := colIdx[col]
		if !ok || idx >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[idx])
	}

	var rows []ParsedSecurityRow
	rowNum := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("row %d: failed to read CSV record: %w", rowNum+1, err)
		}
		rowNum++

		ticker := strings.TrimSpace(record[colIdx["ticker"]])
		if ticker == "" {
			continue
		}

		rows = append(rows, ParsedSecurityRow{
			Ticker:   ticker,
			Name:     strings.TrimSpace(record[colIdx["name"]]),
			Exchange: strings.TrimSpace(record[colIdx["exchange"]]),
			Type:     strings.TrimSpace(record[colIdx["type"]]),
			Currency: optionalCol(record, "currency"),
			ISIN:     optionalCol(record, "isin"),
			Country:  optionalCol(record, "country"),
		})
	}

	return rows, nil
}

// ParseMembershipCSV parses a CSV file with ticker and percentage_or_shares columns
// into a slice of MembershipRequest. Only the Ticker and PercentageOrShares fields
// are populated; security resolution happens downstream in the service layer.
func ParseMembershipCSV(r io.Reader) ([]models.MembershipRequest, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	// Read header row
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Build column index map (case-insensitive, trimmed)
	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[strings.ToLower(strings.TrimSpace(col))] = i
	}

	// Resolve ticker column (first alias found wins)
	tickerCol := ""
	for _, alias := range []string{"ticker", "symbol", "security"} {
		if _, ok := colIdx[alias]; ok {
			tickerCol = alias
			break
		}
	}
	if tickerCol == "" {
		return nil, fmt.Errorf("missing required column: expected one of ticker, symbol, security")
	}

	// Resolve quantity column (first alias found wins)
	quantityCol := ""
	for _, alias := range []string{"percentage_or_shares", "quantity"} {
		if _, ok := colIdx[alias]; ok {
			quantityCol = alias
			break
		}
	}
	if quantityCol == "" {
		return nil, fmt.Errorf("missing required column: expected one of percentage_or_shares, quantity")
	}

	var memberships []models.MembershipRequest
	rowNum := 1 // header is row 1, data starts at row 2
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("row %d: failed to read CSV record: %w", rowNum+1, err)
		}
		rowNum++

		ticker := strings.TrimSpace(record[colIdx[tickerCol]])
		if ticker == "" {
			return nil, fmt.Errorf("row %d: ticker is empty", rowNum)
		}

		pctStr := strings.TrimSpace(record[colIdx[quantityCol]])
		pct, err := strconv.ParseFloat(pctStr, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: ticker %q has invalid %s %q", rowNum, ticker, quantityCol, pctStr)
		}

		memberships = append(memberships, models.MembershipRequest{
			Ticker:             ticker,
			PercentageOrShares: pct,
		})
	}

	return memberships, nil
}

// ScanPriceCSVTickers does a lightweight first-pass scan of a price CSV, returning only
// the set of unique ticker strings. All numeric and date columns are skipped, making
// this significantly faster and cheaper than a full parse.
// The caller must seek the reader back to the start before performing a second pass.
func ScanPriceCSVTickers(r io.Reader) (map[string]struct{}, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[strings.ToLower(strings.TrimSpace(col))] = i
	}
	tickerIdx, ok := colIdx["ticker"]
	if !ok {
		return nil, fmt.Errorf("missing required column: ticker")
	}

	tickers := make(map[string]struct{})
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to scan CSV row: %w", err)
		}
		if tickerIdx < len(record) {
			if t := strings.TrimSpace(record[tickerIdx]); t != "" {
				tickers[t] = struct{}{}
			}
		}
	}
	return tickers, nil
}

// ParsePriceCSV parses a price export CSV into a slice of models.PriceExportRow.
// Required columns: ticker, exchange, date, open, high, low, close, volume (case-insensitive).
// Optional columns: dividend (default 0), split_coefficient (default 1.0).
// date must be in YYYY-MM-DD format. volume is parsed as float64 and rounded to int64.
func ParsePriceCSV(r io.Reader) ([]models.PriceExportRow, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[strings.ToLower(strings.TrimSpace(col))] = i
	}

	for _, col := range []string{"ticker", "exchange", "date", "open", "high", "low", "close", "volume"} {
		if _, ok := colIdx[col]; !ok {
			return nil, fmt.Errorf("missing required column: %s", col)
		}
	}

	parseFloat := func(record []string, col string, rowNum int) (float64, error) {
		s := strings.TrimSpace(record[colIdx[col]])
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("row %d: invalid %s %q", rowNum, col, s)
		}
		return v, nil
	}

	parseOptionalFloat := func(record []string, col string, defaultVal float64, rowNum int) (float64, error) {
		idx, ok := colIdx[col]
		if !ok || idx >= len(record) {
			return defaultVal, nil
		}
		s := strings.TrimSpace(record[idx])
		if s == "" {
			return defaultVal, nil
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("row %d: invalid %s %q", rowNum, col, s)
		}
		return v, nil
	}

	var rows []models.PriceExportRow
	rowNum := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("row %d: failed to read CSV record: %w", rowNum+1, err)
		}
		rowNum++

		ticker := strings.TrimSpace(record[colIdx["ticker"]])
		if ticker == "" {
			continue
		}
		exchange := strings.TrimSpace(record[colIdx["exchange"]])

		dateStr := strings.TrimSpace(record[colIdx["date"]])
		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid date %q: %w", rowNum, dateStr, err)
		}

		open, err := parseFloat(record, "open", rowNum)
		if err != nil {
			return nil, err
		}
		high, err := parseFloat(record, "high", rowNum)
		if err != nil {
			return nil, err
		}
		low, err := parseFloat(record, "low", rowNum)
		if err != nil {
			return nil, err
		}
		close, err := parseFloat(record, "close", rowNum)
		if err != nil {
			return nil, err
		}
		volumeF, err := parseFloat(record, "volume", rowNum)
		if err != nil {
			return nil, err
		}
		dividend, err := parseOptionalFloat(record, "dividend", 0, rowNum)
		if err != nil {
			return nil, err
		}
		splitCoeff, err := parseOptionalFloat(record, "split_coefficient", 1.0, rowNum)
		if err != nil {
			return nil, err
		}

		rows = append(rows, models.PriceExportRow{
			Ticker:           ticker,
			Exchange:         exchange,
			Date:             date,
			Open:             open,
			High:             high,
			Low:              low,
			Close:            close,
			Volume:           int64(math.Round(volumeF)),
			Dividend:         dividend,
			SplitCoefficient: splitCoeff,
		})
	}

	return rows, nil
}

// ParseETFHoldingsCSV parses an ETF holdings CSV file into ParsedETFHoldings.
// Expected columns: Symbol, Company, Weight
// Weight values are percentages (e.g. 7.83 = 7.83%) and are divided by 100
// to produce decimal form (0.0783) matching ParsedETFHolding.Percentage.
func ParseETFHoldingsCSV(r io.Reader) ([]providers.ParsedETFHolding, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	colIdx := make(map[string]int)
	for i, col := range header {
		colIdx[strings.ToLower(strings.TrimSpace(col))] = i
	}

	for _, col := range []string{"symbol", "company", "weight"} {
		if _, ok := colIdx[col]; !ok {
			return nil, fmt.Errorf("missing required column: %s", col)
		}
	}

	var holdings []providers.ParsedETFHolding
	rowNum := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("row %d: failed to read CSV record: %w", rowNum+1, err)
		}
		rowNum++

		ticker := strings.TrimSpace(record[colIdx["symbol"]])
		name := strings.TrimSpace(record[colIdx["company"]])

		weightStr := strings.TrimSpace(record[colIdx["weight"]])
		weight, err := strconv.ParseFloat(weightStr, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid weight %q: %w", rowNum, weightStr, err)
		}

		holdings = append(holdings, providers.ParsedETFHolding{
			Ticker:     ticker,
			Name:       name,
			Percentage: weight / 100.0,
		})
	}

	return holdings, nil
}

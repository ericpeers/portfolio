package handlers

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
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

	// Verify required columns exist
	requiredCols := []string{"ticker", "percentage_or_shares"}
	for _, col := range requiredCols {
		if _, ok := colIdx[col]; !ok {
			return nil, fmt.Errorf("missing required column: %s", col)
		}
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

		ticker := strings.TrimSpace(record[colIdx["ticker"]])
		if ticker == "" {
			return nil, fmt.Errorf("row %d: ticker is empty", rowNum)
		}

		pctStr := strings.TrimSpace(record[colIdx["percentage_or_shares"]])
		pct, err := strconv.ParseFloat(pctStr, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid percentage_or_shares %q", rowNum, pctStr)
		}

		memberships = append(memberships, models.MembershipRequest{
			Ticker:             ticker,
			PercentageOrShares: pct,
		})
	}

	return memberships, nil
}

// ParseETFHoldingsCSV parses an ETF holdings CSV file into ParsedETFHoldings.
// Expected columns: Symbol, Company, Weight
// Weight values are percentages (e.g. 7.83 = 7.83%) and are divided by 100
// to produce decimal form (0.0783) matching ParsedETFHolding.Percentage.
func ParseETFHoldingsCSV(r io.Reader) ([]alphavantage.ParsedETFHolding, error) {
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

	var holdings []alphavantage.ParsedETFHolding
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

		symbol := strings.TrimSpace(record[colIdx["symbol"]])
		name := strings.TrimSpace(record[colIdx["company"]])

		weightStr := strings.TrimSpace(record[colIdx["weight"]])
		weight, err := strconv.ParseFloat(weightStr, 64)
		if err != nil {
			return nil, fmt.Errorf("row %d: invalid weight %q: %w", rowNum, weightStr, err)
		}

		holdings = append(holdings, alphavantage.ParsedETFHolding{
			Symbol:     symbol,
			Name:       name,
			Percentage: weight / 100.0,
		})
	}

	return holdings, nil
}

package handlers

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/epeers/portfolio/internal/models"
)

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

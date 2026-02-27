package models

import (
	"time"
)

// SecurityType represents the type of security
type SecurityType string

const (
	SecurityTypeStock          SecurityType = "COMMON STOCK"
	SecurityTypePreferredStock SecurityType = "PREFERRED STOCK"
	SecurityTypeBond           SecurityType = "BOND"
	SecurityTypeETC            SecurityType = "ETC"
	SecurityTypeETF            SecurityType = "ETF"
	SecurityTypeFund           SecurityType = "FUND"
	SecurityTypeIndex          SecurityType = "INDEX"
	SecurityTypeMutualFund     SecurityType = "MUTUAL FUND"
	SecurityTypeNotes          SecurityType = "NOTES"
	SecurityTypeUnit           SecurityType = "UNIT"
	SecurityTypeWarrant        SecurityType = "WARRANT"
	SecurityTypeCurrency       SecurityType = "CURRENCY"
	SecurityTypeCommodity      SecurityType = "COMMODITY"
	SecurityTypeOption         SecurityType = "OPTION"
)

// Security represents a tradeable security
type Security struct {
	ID        int64      `json:"id"`
	Symbol    string     `json:"symbol"` // maps to ticker column
	Name      string     `json:"name"`
	Exchange  int        `json:"exchange"`  // FK to dim_exchanges
	Inception *time.Time `json:"inception"` // nullable DATE
	URL       *string    `json:"url"`       // nullable VARCHAR
	Type      string     `json:"type"`      // ds_type enum value
}

// SecurityWithCountry extends Security with exchange metadata for multi-exchange resolution.
// Internal use only; not exposed in API responses.
type SecurityWithCountry struct {
	Security
	Country  string // from dim_exchanges.country
	Currency string // from dim_security.currency
}

// ETFMembership represents a security's percentage within an ETF
// Maps to dim_etf_membership table: (dim_security_id, dim_composite_id, percentage)
type ETFMembership struct {
	SecurityID int64   `json:"security_id"` // dim_security_id - the member (e.g., NVDA)
	ETFID      int64   `json:"etf_id"`      // dim_composite_id - the ETF (e.g., SPY)
	Percentage float64 `json:"percentage"`
}

// LoadSecuritiesResponse is returned by POST /admin/load_securities.
type LoadSecuritiesResponse struct {
	Inserted          int      `json:"inserted"`
	SkippedExisting   int      `json:"skipped_existing"`
	SkippedDupInFile  int      `json:"skipped_dup_in_file"`
	SkippedBadType    int      `json:"skipped_bad_type"`
	SkippedLongTicker int      `json:"skipped_long_ticker"`
	UpdatedIsin       int      `json:"updated_isin"`
	NewExchanges      []string `json:"new_exchanges,omitempty"`
	Warnings          []string `json:"warnings,omitempty"`
	DryRun            bool     `json:"dry_run,omitempty"`
}

// PriceData represents historical price data for a security
type PriceData struct {
	SecurityID int64     `json:"security_id"`
	Date       time.Time `json:"date"`
	Open       float64   `json:"open"`
	High       float64   `json:"high"`
	Low        float64   `json:"low"`
	Close      float64   `json:"close"`
	Volume     int64     `json:"volume"`
}

// EventData is used for dividends and splits. It sits in parallel to fact_price
// and fact_price_range describes how much data we have for it.
type EventData struct {
	SecurityID       int64     `json:"security_id"`
	Date             time.Time `json:"date"`
	Dividend         float64   `json:"dividend"`
	SplitCoefficient float64   `json:"split_coefficient"`
}


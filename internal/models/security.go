package models

import (
	"strings"
	"time"
)

// ExchangeAliases maps raw exchange codes to their canonical dim_exchanges name.
var ExchangeAliases = map[string]string{
	"GBOND": "BONDS/CASH/TREASURIES",
}

// validSecurityTypeSet is the set of accepted ds_type values after normalization.
// "MUTUAL FUND" is intentionally absent — it is normalised to "FUND" before lookup.
var validSecurityTypeSet = map[string]bool{
	string(SecurityTypeStock):          true,
	string(SecurityTypePreferredStock): true,
	string(SecurityTypeBond):           true,
	string(SecurityTypeETC):            true,
	string(SecurityTypeETF):            true,
	string(SecurityTypeFund):           true,
	string(SecurityTypeIndex):          true,
	string(SecurityTypeNotes):          true,
	string(SecurityTypeUnit):           true,
	string(SecurityTypeWarrant):        true,
	string(SecurityTypeCurrency):       true,
	string(SecurityTypeCommodity):      true,
	string(SecurityTypeOption):         true,
}

// NormalizeSecurityType uppercases and trims rawType, maps "MUTUAL FUND" → "FUND",
// and returns (normalizedType, true) if the result is a known ds_type enum value.
// Returns ("", false) for unrecognised types.
func NormalizeSecurityType(rawType string) (string, bool) {
	t := strings.ToUpper(strings.TrimSpace(rawType))
	if t == string(SecurityTypeMutualFund) {
		t = string(SecurityTypeFund)
	}
	if validSecurityTypeSet[t] {
		return t, true
	}
	return "", false
}

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
	Ticker    string     `json:"ticker"`
	Name      string     `json:"name"`
	Exchange  int        `json:"exchange"`  // FK to dim_exchanges
	Inception *time.Time `json:"inception"` // nullable DATE
	URL       *string    `json:"url"`       // nullable VARCHAR
	Type      string     `json:"type"`      // ds_type enum value
	Delisted  bool       `json:"delisted"`
}

// SecurityWithCountry extends Security with exchange metadata for multi-exchange resolution.
// Internal use only; not exposed in API responses.
type SecurityWithCountry struct {
	Security
	Country      string // from dim_exchanges.country
	Currency     string // from dim_security.currency
	ExchangeName string // from dim_exchanges.name (used for OTC routing in FD client)
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
	TruncatedName     int      `json:"truncated_name"`
	UpdatedIsin       int      `json:"updated_isin"`
	NewExchanges      []string `json:"new_exchanges,omitempty"`
	Warnings          []string `json:"warnings,omitempty"`
	DryRun            bool     `json:"dry_run,omitempty"`
}

// MismatchedIPODate describes a ticker where the CSV IPO date differs from the DB.
type MismatchedIPODate struct {
	Ticker  string `json:"ticker"`
	Name    string `json:"name"`
	CSVDate string `json:"csv_date"`
	DBDate  string `json:"db_date"`
}

// LoadIPODatesResponse is returned by POST /admin/load_securities/ipo.
type LoadIPODatesResponse struct {
	Inserted       int                 `json:"inserted"`
	Skipped        int                 `json:"skipped"`
	FileDuplicates int                 `json:"file_duplicates"`
	Mismatches     []MismatchedIPODate `json:"mismatches"`
	NoMatch        int                 `json:"no_match"`
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

// PriceRangeData is the input record for BatchUpsertPriceRange.
type PriceRangeData struct {
	SecurityID int64
	StartDate  time.Time
	EndDate    time.Time
	NextUpdate time.Time
}

// PriceExportRow is used for bulk export/import of price data.
// Uses ticker + exchange name instead of security_id (which is DB-local and non-portable).
// Dividend defaults to 0; SplitCoefficient defaults to 1.0 (no event).
type PriceExportRow struct {
	Ticker           string
	Exchange         string // dim_exchanges.name — used as match key on import
	Date             time.Time
	Open             float64
	High             float64
	Low              float64
	Close            float64
	Volume           int64
	Dividend         float64
	SplitCoefficient float64
}


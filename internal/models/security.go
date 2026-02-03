package models

import (
	"time"
)

// SecurityType represents the type of security
type SecurityType string

const (
	SecurityTypeStock      SecurityType = "Stock"
	SecurityTypeETF        SecurityType = "ETF"
	SecurityTypeMutualFund SecurityType = "MutualFund"
	SecurityTypeBond       SecurityType = "Bond"
)

// Security represents a tradeable security
type Security struct {
	ID        int64      `json:"id"`
	Symbol    string     `json:"symbol"`      // maps to ticker column
	Name      string     `json:"name"`
	Exchange  int        `json:"exchange"`    // FK to dim_exchanges
	Inception *time.Time `json:"inception"`   // nullable DATE
	URL       *string    `json:"url"`         // nullable VARCHAR
	TypeID    int        `json:"type_id"`     // FK to dim_security_types
}

// ETFMembership represents a security's percentage within an ETF
// Maps to dim_etf_membership table: (dim_security_id, dim_composite_id, percentage)
type ETFMembership struct {
	SecurityID int64   `json:"security_id"` // dim_security_id - the member (e.g., NVDA)
	ETFID      int64   `json:"etf_id"`      // dim_composite_id - the ETF (e.g., SPY)
	Percentage float64 `json:"percentage"`
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

// Quote represents a real-time quote for a security
type Quote struct {
	SecurityID int64     `json:"security_id"`
	Symbol     string    `json:"symbol"`
	Price      float64   `json:"price"`
	FetchedAt  time.Time `json:"fetched_at"`
}

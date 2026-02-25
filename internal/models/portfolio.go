package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// PortfolioType represents whether a portfolio is Ideal or Active
type PortfolioType string

const (
	PortfolioTypeIdeal    PortfolioType = "Ideal"
	PortfolioTypeActive   PortfolioType = "Active"
	PortfolioTypeHistoric PortfolioType = "Historic"
)

// Objective represents the investment objective of a portfolio
type Objective string

const (
	ObjectiveAggressiveGrowth    Objective = "Aggressive Growth"
	ObjectiveGrowth              Objective = "Growth"
	ObjectiveIncomeGeneration    Objective = "Income Generation"
	ObjectiveCapitalPreservation Objective = "Capital Preservation"
	ObjectiveMixedGrowthIncome   Objective = "Mixed Growth/Income"
)

// Portfolio represents a user's portfolio
type Portfolio struct {
	ID            int64         `json:"id"`
	PortfolioType PortfolioType `json:"portfolio_type"`
	Objective     Objective     `json:"objective"`
	Name          string        `json:"name"`
	Comment       *string       `json:"comment,omitempty"`
	OwnerID       int64         `json:"owner_id"`
	CreatedAt     time.Time     `json:"created_at"`
	EndedAt       *time.Time    `json:"ended_at,omitempty"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

// PortfolioMembership represents a security holding within a portfolio
// Uses composite primary key (portfolio_id, security_id) - no separate ID field
type PortfolioMembership struct {
	PortfolioID        int64   `json:"portfolio_id"`
	Ticker             string  `json:"ticker"`
	SecurityID         int64   `json:"security_id"`
	PercentageOrShares float64 `json:"percentage_or_shares"`
}

// PortfolioWithMemberships combines a portfolio with its memberships
type PortfolioWithMemberships struct {
	Portfolio   Portfolio             `json:"portfolio"`
	Memberships []PortfolioMembership `json:"memberships"`
}

// MembershipSource represents a source contributing to a security's allocation.
// For direct holdings, the source is the security itself.
// For ETF-expanded holdings, the source is the ETF.
// Source allocations within an ExpandedMembership sum to 1.0.
type MembershipSource struct {
	SecurityID int64   `json:"security_id"`
	Symbol     string  `json:"symbol"`
	Allocation float64 `json:"allocation"` // Proportion of this security's allocation from this source (sums to 1.0)
}

// ExpandedMembership represents a security's allocation after ETF expansion
type ExpandedMembership struct {
	SecurityID int64              `json:"security_id"`
	Symbol     string             `json:"symbol"`
	Allocation float64            `json:"allocation"` // Decimal allocation (0.60 = 60%)
	Sources    []MembershipSource `json:"sources,omitempty"`
}

func (e MembershipSource) MarshalJSON() ([]byte, error) {
	type plain struct {
		SecurityID int64           `json:"security_id"`
		Symbol     string          `json:"symbol"`
		Allocation json.RawMessage `json:"allocation"`
	}
	return json.Marshal(plain{
		SecurityID: e.SecurityID,
		Symbol:     e.Symbol,
		Allocation: json.RawMessage(fmt.Sprintf("%.6f", e.Allocation)),
	})
}

func (e ExpandedMembership) MarshalJSON() ([]byte, error) {
	type plain struct {
		SecurityID int64              `json:"security_id"`
		Symbol     string             `json:"symbol"`
		Allocation json.RawMessage    `json:"allocation"`
		Sources    []MembershipSource `json:"sources,omitempty"`
	}
	return json.Marshal(plain{
		SecurityID: e.SecurityID,
		Symbol:     e.Symbol,
		Allocation: json.RawMessage(fmt.Sprintf("%.6f", e.Allocation)),
		Sources:    e.Sources,
	})
}

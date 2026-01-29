package models

import (
	"time"
)

// PortfolioType represents whether a portfolio is Ideal or Active
type PortfolioType string

const (
	PortfolioTypeIdeal    PortfolioType = "Ideal"
	PortfolioTypeActive   PortfolioType = "Active"
	PortfolioTypeHistoric PortfolioType = "Historic"
)

// Portfolio represents a user's portfolio
type Portfolio struct {
	ID            int64         `json:"id"`
	PortfolioType PortfolioType `json:"portfolio_type"`
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
	SecurityID         int64   `json:"security_id"`
	PercentageOrShares float64 `json:"percentage_or_shares"`
}

// PortfolioWithMemberships combines a portfolio with its memberships
type PortfolioWithMemberships struct {
	Portfolio   Portfolio             `json:"portfolio"`
	Memberships []PortfolioMembership `json:"memberships"`
}

// ExpandedMembership represents a security's allocation after ETF expansion
type ExpandedMembership struct {
	SecurityID int64   `json:"security_id"`
	Symbol     string  `json:"symbol"`
	Allocation float64 `json:"allocation"` // Percentage of total portfolio
}

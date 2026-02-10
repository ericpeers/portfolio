package models

// WarningCode categorizes warnings by subsystem.
// W1xxx = ETF/membership, W2xxx = pricing, W3xxx = validation, W4xxx = comparison.
type WarningCode string

const (
	WarnUnresolvedETFHolding WarningCode = "W1001" // individual unresolved holding
	WarnPartialETFExpansion  WarningCode = "W1002" // ETF only partially expanded, normalized
)

// Warning represents a non-fatal issue encountered during processing.
type Warning struct {
	Code    WarningCode `json:"code"`
	Message string      `json:"message"`
}

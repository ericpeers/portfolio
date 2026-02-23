package models

// WarningCode categorizes warnings by subsystem.
// W1xxx = ETF/membership, W2xxx = pricing, W3xxx = validation, W4xxx = comparison.
type WarningCode string

const (
	WarnUnresolvedETFHolding WarningCode = "W1001" // individual unresolved holding (dropped from results)
	WarnPartialETFExpansion  WarningCode = "W1002" // holdings scaled to 100% because resolved weights didn't sum to 1.0
	WarnETFSourceIncomplete  WarningCode = "W1003" // source ETF data does not add up to 100%
	WarnStartDateAdjusted    WarningCode = "W4001" // start date adjusted to security inception date
)

// Warning represents a non-fatal issue encountered during processing.
type Warning struct {
	Code    WarningCode `json:"code"`
	Message string      `json:"message"`
}

package models

// WarningCode categorizes warnings by subsystem.
// W1xxx = ETF/membership, W2xxx = portfolio import, W3xxx = validation, W4xxx = comparison.
type WarningCode string

const (
	WarnUnresolvedETFHolding    WarningCode = "W1001" // individual unresolved holding (dropped from results)
	WarnPartialETFExpansion     WarningCode = "W1002" // holdings scaled to 100% because resolved weights didn't sum to 1.0
	WarnETFSourceIncomplete     WarningCode = "W1003" // source ETF data does not add up to 100%
	WarnFuzzyMatchSubstituted   WarningCode = "W2001" // dash-inserted ticker used in place of original (e.g. BRKB → BRK-B)
	WarnStartDateAdjusted       WarningCode = "W4001" // start date adjusted to security inception date
)

// Warning represents a non-fatal issue encountered during processing.
type Warning struct {
	Code    WarningCode `json:"code"`
	Message string      `json:"message"`
}

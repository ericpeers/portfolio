package models

// WarningCode categorizes warnings by subsystem.
// W1xxx = ETF/membership, W2xxx = portfolio import, W3xxx = pricing/data, W4xxx = comparison.
type WarningCode string

const (
	WarnUnresolvedETFHolding    WarningCode = "W1001" // individual unresolved holding (dropped from results)
	WarnPartialETFExpansion     WarningCode = "W1002" // holdings scaled to 100% because resolved weights didn't sum to 1.0
	WarnETFSourceIncomplete     WarningCode = "W1003" // source ETF data does not add up to 100%
	WarnFuzzyMatchSubstituted   WarningCode = "W2001" // dash-inserted ticker used in place of original (e.g. BRKB → BRK-B)
	WarnMissingPriceHistory     WarningCode = "W3001" // one or more securities have no price history; affected dates excluded
	WarnExcessiveForwardFill    WarningCode = "W3002" // too many securities needed forward-filling on some dates; those dates excluded
	WarnStartDateAdjusted            WarningCode = "W4001" // start date adjusted to security inception date
	WarnBenchmarkDataUnavailable     WarningCode = "W4002" // benchmark ticker missing or has no price data; Alpha/Beta set to zero
)

// Warning represents a non-fatal issue encountered during processing.
type Warning struct {
	Code    WarningCode `json:"code"`
	Message string      `json:"message"`
}

package services

import (
	"time"
)

// DiffType identifies the kind of modification a PortfolioDiff applies.
type DiffType string

const (
	// DiffPriceOverride injects caller-computed synthetic prices for dates before EffectiveBefore.
	// Used by the cash_flat and cash_appreciating strategies.
	DiffPriceOverride DiffType = "price_override"
	// DiffSubstitute replaces this security's price source with a proxy security for dates
	// before EffectiveBefore. Proxy prices are looked up from the DB.
	DiffSubstitute DiffType = "substitute"
	// DiffRemove excludes the security for dates before EffectiveBefore and redistributes
	// its weight proportionally. At EffectiveBefore the security re-enters and shares are
	// recomputed from the portfolio's last known value.
	DiffRemove DiffType = "remove"
)

// PortfolioDiff describes one modification that is active for dates strictly before EffectiveBefore.
// At EffectiveBefore the portfolio returns to its normal state for that member.
// All strategy-specific handling (cash, reallocate, substitute) flows through this type.
type PortfolioDiff struct {
	EffectiveBefore time.Time // diff active for date.Before(EffectiveBefore)
	Type            DiffType
	SecurityID      int64 // security being affected

	// DiffSubstitute: use this security's DB prices instead of SecurityID's prices.
	ProxySecID int64

	// DiffPriceOverride: caller-computed synthetic prices keyed by date.
	SyntheticPrices map[time.Time]float64
}

// GenerateReallocateDiffs produces a DiffRemove entry for every gapped member in coverage.
// Per-transition weight recomputation is done on-the-fly inside ComputeDailyValues using
// prices already loaded for the transition date, so no price fetching is needed here.
func GenerateReallocateDiffs(coverage *DataCoverageReport) []PortfolioDiff {
	var diffs []PortfolioDiff
	for _, m := range coverage.Members {
		if !m.HasFullCoverage {
			diffs = append(diffs, PortfolioDiff{
				EffectiveBefore: m.EffectiveStart,
				Type:            DiffRemove,
				SecurityID:      m.SecurityID,
			})
		}
	}
	return diffs
}

// DiffsToMembershipOverlay extracts DiffPriceOverride synthetic prices from diffs into the
// legacy map[secID]map[date]price format expected by MembershipService. Returns nil when no
// price overrides are present (e.g., for the reallocate strategy which has only DiffRemove).
func DiffsToMembershipOverlay(diffs []PortfolioDiff) map[int64]map[time.Time]float64 {
	var overlay map[int64]map[time.Time]float64
	for _, d := range diffs {
		if d.Type == DiffPriceOverride {
			if overlay == nil {
				overlay = make(map[int64]map[time.Time]float64)
			}
			overlay[d.SecurityID] = d.SyntheticPrices
		}
	}
	return overlay
}


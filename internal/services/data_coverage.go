package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/models"
	log "github.com/sirupsen/logrus"
)

// MemberCoverage describes the data availability for a single portfolio member.
type MemberCoverage struct {
	SecurityID      int64
	Ticker          string
	EffectiveStart  time.Time // earliest reliable date (from inception or first price row)
	HasFullCoverage bool      // true when EffectiveStart <= requestedStart
}

// DataCoverageReport summarises which portfolio members have price data available
// for the requested date range. Used by callers to choose a missing-data strategy
// before invoking ComputeDailyValues.
type DataCoverageReport struct {
	RequestedStart   time.Time
	ConstrainedStart time.Time // max(EffectiveStart) across all members; equals RequestedStart when no gaps
	Members          []MemberCoverage
	AnyGaps          bool // true if any member's EffectiveStart is after RequestedStart
}

// ComputeDataCoverage inspects price availability for each member of portfolio and
// returns a DataCoverageReport relative to requestedStart.
//
// For each member the effective start date is resolved in priority order:
//  1. dim_security.inception (if set)
//  2. MIN(date) from fact_price (first cached price row)
//  3. requestedStart (if no price data exists at all — treated as fully available
//     so that the existing reactive missing-data logic in ComputeDailyValues handles it)
//
// ConstrainedStart is the latest EffectiveStart across all members; callers using the
// ConstrainDateRange strategy should pass this as their startDate to ComputeDailyValues.
//
// Uses two bulk fetches to avoid N singleton DB round-trips:
//   - GetAllSecurities (snapshot-cached) for inception dates and tickers
//   - GetFirstPriceDates for null-inception securities only
func (s *PerformanceService) ComputeDataCoverage(
	ctx context.Context,
	portfolio *models.PortfolioWithMemberships,
	requestedStart time.Time,
) (*DataCoverageReport, error) {
	// Bulk fetch all securities from the snapshot cache — O(1) when warm.
	byID, _, err := s.secRepo.GetAllSecurities(ctx)
	if err != nil {
		return nil, fmt.Errorf("ComputeDataCoverage: failed to load securities: %w", err)
	}

	// First pass: resolve inception-date members and collect IDs that need first-price fallback.
	type memberState struct {
		secID          int64
		ticker         string
		effectiveStart time.Time
		resolved       bool // true when inception date was found
	}
	states := make([]memberState, 0, len(portfolio.Memberships))
	nullInceptionIDs := make([]int64, 0)

	for _, m := range portfolio.Memberships {
		sec, ok := byID[m.SecurityID]
		if !ok {
			return nil, fmt.Errorf("ComputeDataCoverage: security %d not found in snapshot", m.SecurityID)
		}
		if sec.Inception != nil {
			states = append(states, memberState{
				secID:          m.SecurityID,
				ticker:         sec.Ticker,
				effectiveStart: *sec.Inception,
				resolved:       true,
			})
		} else {
			states = append(states, memberState{
				secID:  m.SecurityID,
				ticker: sec.Ticker,
			})
			nullInceptionIDs = append(nullInceptionIDs, m.SecurityID)
		}
	}

	// Bulk fetch first price dates for null-inception securities in a single query.
	var firstPriceDates map[int64]*time.Time
	if len(nullInceptionIDs) > 0 {
		firstPriceDates, err = s.pricingSvc.GetFirstPriceDates(ctx, nullInceptionIDs)
		if err != nil {
			return nil, fmt.Errorf("ComputeDataCoverage: failed to batch-fetch first price dates: %w", err)
		}
	}

	// Second pass: resolve null-inception members using the bulk result.
	for i := range states {
		if states[i].resolved {
			continue
		}
		fp, hasPrices := firstPriceDates[states[i].secID]
		if hasPrices && fp != nil {
			states[i].effectiveStart = *fp
		} else {
			// No price data at all — fall back to requestedStart so the reactive logic in
			// ComputeDailyValues handles the hard-missing case with its usual warning.
			log.Warnf("ComputeDataCoverage: no inception date or price data for security %d (%s); treating as available from requested start", states[i].secID, states[i].ticker)
			states[i].effectiveStart = requestedStart
		}
		states[i].resolved = true
	}

	// Build the report.
	report := &DataCoverageReport{
		RequestedStart:   requestedStart,
		ConstrainedStart: requestedStart,
		Members:          make([]MemberCoverage, 0, len(states)),
	}
	for _, st := range states {
		hasFullCoverage := !st.effectiveStart.After(requestedStart)
		report.Members = append(report.Members, MemberCoverage{
			SecurityID:      st.secID,
			Ticker:          st.ticker,
			EffectiveStart:  st.effectiveStart,
			HasFullCoverage: hasFullCoverage,
		})
		if st.effectiveStart.After(report.ConstrainedStart) {
			report.ConstrainedStart = st.effectiveStart
		}
	}
	report.AnyGaps = report.ConstrainedStart.After(requestedStart)
	return report, nil
}

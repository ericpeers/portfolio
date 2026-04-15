package services

import (
	"context"
	"fmt"
	"sort"
	"strings"
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

// SynthesizeCashPrices builds a price overlay map for securities that have pre-IPO gaps.
// For each gapped member in coverage, it fetches the anchor price (first real close) and
// generates synthetic prices for every calendar day from RequestedStart up to (but not
// including) the member's EffectiveStart.
//
// Two strategies are supported:
//   - MissingDataStrategyCashFlat: every pre-IPO day gets the anchor price (value is flat).
//   - MissingDataStrategyCashAppreciating: pre-IPO prices are discounted backward from the
//     anchor using the DGS10 daily rate, so the portfolio shows risk-free appreciation
//     toward the IPO date.
//
// The returned map is keyed by security ID → date → synthetic close price.
// Non-trading days are included intentionally; ComputeDailyValues only consults the map for
// dates in its already-filtered trading-day list, so unused entries are harmless.
//
// Callers pass this map as priceOverrides to ComputeDailyValues to apply the strategy.
func (s *PerformanceService) SynthesizeCashPrices(
	ctx context.Context,
	coverage *DataCoverageReport,
	strategy models.MissingDataStrategy,
) (map[int64]map[time.Time]float64, error) {
	overlay := make(map[int64]map[time.Time]float64)

	// Collect members that have a pre-IPO gap.
	var gapped []MemberCoverage
	for _, m := range coverage.Members {
		if !m.HasFullCoverage {
			gapped = append(gapped, m)
		}
	}
	if len(gapped) == 0 {
		return overlay, nil
	}

	// For appreciating cash, fetch DGS10 rates once covering the full pre-IPO window.
	var riskFreeRates map[time.Time]float64
	var dailyAvgRate float64
	if strategy == models.MissingDataStrategyCashAppreciating {
		maxEffStart := coverage.RequestedStart
		for _, m := range gapped {
			if m.EffectiveStart.After(maxEffStart) {
				maxEffStart = m.EffectiveStart
			}
		}
		var err error
		riskFreeRates, dailyAvgRate, err = s.computeRiskFreeRates(ctx, coverage.RequestedStart, maxEffStart)
		if err != nil {
			return nil, fmt.Errorf("SynthesizeCashPrices: failed to fetch DGS10 rates: %w", err)
		}
	}

	for _, m := range gapped {
		// Anchor = first real closing price on or near EffectiveStart.
		anchorPrice, err := s.GetPriceAtDate(ctx, m.SecurityID, m.EffectiveStart)
		if err != nil || anchorPrice == 0 {
			log.Warnf("SynthesizeCashPrices: no anchor price for %s (id=%d) at %s — skipping cash synthesis",
				m.Ticker, m.SecurityID, m.EffectiveStart.Format("2006-01-02"))
			continue
		}

		secOverlay := make(map[time.Time]float64)

		switch strategy {
		case models.MissingDataStrategyCashFlat:
			// Constant price for every calendar day before the IPO.
			for d := coverage.RequestedStart; d.Before(m.EffectiveStart); d = d.AddDate(0, 0, 1) {
				secOverlay[d] = anchorPrice
			}

		case models.MissingDataStrategyCashAppreciating:
			// Iterate backward from EffectiveStart-1 to RequestedStart.
			// price[d] = price[d+1] / (1 + dailyRate[d])
			// This produces a smooth appreciation from RequestedStart up to anchorPrice at EffectiveStart.
			price := anchorPrice
			for d := m.EffectiveStart.AddDate(0, 0, -1); !d.Before(coverage.RequestedStart); d = d.AddDate(0, 0, -1) {
				rate := interpolateRiskFreeRate(riskFreeRates, d, dailyAvgRate)
				price /= (1.0 + rate)
				secOverlay[d] = price
			}
		}

		overlay[m.SecurityID] = secOverlay
	}

	return overlay, nil
}

// CashSubstitutionWarningMessage builds the W4003 warning message listing affected tickers.
func CashSubstitutionWarningMessage(coverages ...*DataCoverageReport) string {
	seen := make(map[string]struct{})
	var tickers []string
	for _, cov := range coverages {
		for _, m := range cov.Members {
			if !m.HasFullCoverage {
				if _, ok := seen[m.Ticker]; !ok {
					seen[m.Ticker] = struct{}{}
					tickers = append(tickers, m.Ticker)
				}
			}
		}
	}
	sort.Strings(tickers)
	return fmt.Sprintf("Pre-IPO period for %s covered with synthetic cash prices; start date unchanged.", strings.Join(tickers, ", "))
}

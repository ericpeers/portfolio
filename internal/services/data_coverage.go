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
// For each member the effective start date is resolved as max(inception, firstPriceDate):
//  1. dim_security.inception anchors the "official" availability date.
//  2. The actual first row in fact_price overrides inception when it is later — this
//     catches securities where inception is a company founding date (e.g. 1949) that
//     far predates when price data was actually available.
//  3. If neither is known, requestedStart is used (treated as fully available so that
//     the existing missing-data logic in ComputeDailyValues handles it).
//
// ConstrainedStart is the latest EffectiveStart across all members; callers using the
// ConstrainDateRange strategy should pass this as their startDate to ComputeDailyValues.
//
// Uses two bulk fetches to avoid N singleton DB round-trips:
//   - GetAllSecurities (snapshot-cached) for inception dates and tickers
//   - GetFirstPriceDates for ALL members (cross-checks inception against real data)
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

	type memberState struct {
		secID     int64
		ticker    string
		inception *time.Time
	}
	states := make([]memberState, 0, len(portfolio.Memberships))
	allMemberIDs := make([]int64, 0, len(portfolio.Memberships))

	for _, m := range portfolio.Memberships {
		sec, ok := byID[m.SecurityID]
		if !ok {
			return nil, fmt.Errorf("ComputeDataCoverage: security %d not found in snapshot", m.SecurityID)
		}
		states = append(states, memberState{
			secID:     m.SecurityID,
			ticker:    sec.Ticker,
			inception: sec.Inception,
		})
		allMemberIDs = append(allMemberIDs, m.SecurityID)
	}

	// Bulk fetch actual first price dates for all members. Cross-checking against
	// inception catches cases where dim_security.inception is a historical founding
	// date that far predates the first available price (e.g. PCRHY: inception=1949,
	// first price=2024).
	firstPriceDates, err := s.pricingSvc.GetFirstPriceDates(ctx, allMemberIDs)
	if err != nil {
		return nil, fmt.Errorf("ComputeDataCoverage: failed to batch-fetch first price dates: %w", err)
	}

	// Resolve effective start for each member: max(inception, firstPriceDate).
	// inception provides a lower bound; firstPriceDate can only push it later.
	report := &DataCoverageReport{
		RequestedStart:   requestedStart,
		ConstrainedStart: requestedStart,
		Members:          make([]MemberCoverage, 0, len(states)),
	}
	for _, st := range states {
		effectiveStart := requestedStart // fallback: no data known, assume fully available
		if st.inception != nil {
			effectiveStart = *st.inception
		}
		if fp, ok := firstPriceDates[st.secID]; ok && fp != nil && fp.After(effectiveStart) {
			effectiveStart = *fp
		}
		if st.inception == nil {
			if fp, ok := firstPriceDates[st.secID]; !ok || fp == nil {
				log.Warnf("ComputeDataCoverage: no inception date or price data for security %d (%s); treating as available from requested start", st.secID, st.ticker)
			}
		}

		hasFullCoverage := !effectiveStart.After(requestedStart)
		report.Members = append(report.Members, MemberCoverage{
			SecurityID:      st.secID,
			Ticker:          st.ticker,
			EffectiveStart:  effectiveStart,
			HasFullCoverage: hasFullCoverage,
		})
		if effectiveStart.After(report.ConstrainedStart) {
			report.ConstrainedStart = effectiveStart
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
// The returned diffs carry the synthetic prices and are passed to ComputeDailyValues as the
// diffs parameter. Non-trading days are included intentionally; ComputeDailyValues only
// consults the map for dates in its already-filtered trading-day list.
func (s *PerformanceService) SynthesizeCashPrices(
	ctx context.Context,
	coverage *DataCoverageReport,
	strategy models.MissingDataStrategy,
) ([]PortfolioDiff, error) {
	// Collect members that have a pre-IPO gap.
	var gapped []MemberCoverage
	for _, m := range coverage.Members {
		if !m.HasFullCoverage {
			gapped = append(gapped, m)
		}
	}
	if len(gapped) == 0 {
		return nil, nil
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

	// Bulk-fetch actual first price dates for gapped members.
	// dim_security.inception can differ from the first price row by one or more days
	// (e.g., IPO declared for June 1 but first EODHD row lands June 2). Using the
	// inception date as EffectiveBefore leaves a gap on the IPO date itself.
	// Using the actual first price date closes that gap precisely.
	gappedIDs := make([]int64, 0, len(gapped))
	for _, m := range gapped {
		gappedIDs = append(gappedIDs, m.SecurityID)
	}
	firstPriceDates, err := s.pricingSvc.GetFirstPriceDates(ctx, gappedIDs)
	if err != nil {
		return nil, fmt.Errorf("SynthesizeCashPrices: failed to fetch first price dates: %w", err)
	}

	var diffs []PortfolioDiff
	for _, m := range gapped {
		// anchorDate = actual first price date in the DB. Falls back to m.EffectiveStart
		// when no first-price row exists (synthesis will be skipped below via anchor lookup).
		anchorDate := m.EffectiveStart
		if fp, ok := firstPriceDates[m.SecurityID]; ok && fp != nil && fp.After(m.EffectiveStart) {
			anchorDate = *fp
		}

		// Anchor = closing price on the first real trading day.
		anchorPrice, err := s.GetPriceAtDate(ctx, m.SecurityID, anchorDate)
		if err != nil || anchorPrice == 0 {
			log.Warnf("SynthesizeCashPrices: no anchor price for %s (id=%d) at %s — skipping cash synthesis",
				m.Ticker, m.SecurityID, anchorDate.Format("2006-01-02"))
			continue
		}

		syntheticPrices := make(map[time.Time]float64)

		switch strategy {
		case models.MissingDataStrategyCashFlat:
			// Constant price for every calendar day before the first real price.
			for d := coverage.RequestedStart; d.Before(anchorDate); d = d.AddDate(0, 0, 1) {
				syntheticPrices[d] = anchorPrice
			}

		case models.MissingDataStrategyCashAppreciating:
			// Iterate backward from anchorDate-1 to RequestedStart.
			// price[d] = price[d+1] / (1 + dailyRate[d])
			price := anchorPrice
			for d := anchorDate.AddDate(0, 0, -1); !d.Before(coverage.RequestedStart); d = d.AddDate(0, 0, -1) {
				rate := interpolateRiskFreeRate(riskFreeRates, d, dailyAvgRate)
				price /= (1.0 + rate)
				syntheticPrices[d] = price
			}
		}

		diffs = append(diffs, PortfolioDiff{
			EffectiveBefore: anchorDate,
			Type:            DiffPriceOverride,
			SecurityID:      m.SecurityID,
			SyntheticPrices: syntheticPrices,
		})
	}

	return diffs, nil
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

// ReallocWarningMessage builds the W4004 warning message listing tickers affected by
// the proportional reallocation strategy.
func ReallocWarningMessage(coverages ...*DataCoverageReport) string {
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
	return fmt.Sprintf("Pre-IPO period for %s handled by redistributing weight among available members; start date unchanged.", strings.Join(tickers, ", "))
}

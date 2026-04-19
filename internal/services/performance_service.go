package services

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

const tradingDaysPerYear = 252

// PerformanceService handles portfolio performance calculations
type PerformanceService struct {
	pricingSvc       *PricingService
	portfolioRepo    *repository.PortfolioRepository
	secRepo          *repository.SecurityRepository
	priceConcurrency int
}

// NewPerformanceService creates a new PerformanceService.
// priceConcurrency controls how many security price fetches run in parallel inside
// ComputeDailyValues (10–100 is a reasonable range; 20 is a good default).
func NewPerformanceService(
	pricingSvc *PricingService,
	portfolioRepo *repository.PortfolioRepository,
	secRepo *repository.SecurityRepository,
	priceConcurrency int,
) *PerformanceService {
	return &PerformanceService{
		pricingSvc:       pricingSvc,
		portfolioRepo:    portfolioRepo,
		secRepo:          secRepo,
		priceConcurrency: priceConcurrency,
	}
}

// NormalizeIdealPortfolio converts an ideal portfolio's percentages to share-based holdings.
// Returns a new PortfolioWithMemberships where PercentageOrShares contains computed shares.
// For actual portfolios, use the original pointer directly (no normalization needed).
//
// NOTE: This code may be optimized by collapsing price fetches between NormalizeIdealPortfolio
// and ComputeDailyValues. Consider retaining an in-memory cache of date/price points for
// securities to minimize postgres fetches.
func (s *PerformanceService) NormalizeIdealPortfolio(ctx context.Context, portfolio *models.PortfolioWithMemberships, startDate time.Time, targetStartValue float64, overlay map[int64]map[time.Time]float64) (*models.PortfolioWithMemberships, error) {
	defer TrackTime("NormalizeIdealPortfolio", time.Now())
	if portfolio.Portfolio.PortfolioType != models.PortfolioTypeIdeal {
		// Actual portfolios don't need normalization - use original pointer
		return portfolio, nil
	}

	// Calculate total percentage
	var totalPct float64
	for _, m := range portfolio.Memberships {
		totalPct += m.PercentageOrShares
	}

	// Create new portfolio with computed shares
	normalized := &models.PortfolioWithMemberships{
		Portfolio: portfolio.Portfolio,
	}

	// Convert percentages to shares based on start prices
	// FIXME: This should be a bulk fetch. GetPriceAtDate finds the price at that date,
	// or the preceding business day. Consider bulk fetching all prices from postgres
	// for the start date, with fallback to GetPriceAtOrBeforeDate for missing data.
	// This will be slow for large portfolios (e.g., 2000 securities).
	for _, m := range portfolio.Memberships {
		var price float64
		var err error

		// Check overlay first for pre-IPO prices
		if secOverlay, ok := overlay[m.SecurityID]; ok {
			if p, ok := secOverlay[startDate]; ok {
				price = p
			}
		}

		// Fallback to DB/Provider if not in overlay
		if price == 0 {
			price, err = s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, startDate)
			if err != nil {
				return nil, fmt.Errorf("failed to get price for security %d: %w", m.SecurityID, err)
			}
		}
		if price == 0 {
			return nil, fmt.Errorf("zero price for security %d at %s — cannot normalize portfolio", m.SecurityID, startDate.Format("2006-01-02"))
		}

		allocationDollars := targetStartValue * (m.PercentageOrShares / totalPct)
		shares := allocationDollars / price

		normalized.Memberships = append(normalized.Memberships, models.PortfolioMembership{
			PortfolioID:        m.PortfolioID,
			Ticker:             m.Ticker,
			SecurityID:         m.SecurityID,
			PercentageOrShares: shares,
		})
	}

	return normalized, nil
}

// GainResult contains gain calculations
type GainResult struct {
	StartValue  float64
	EndValue    float64
	GainDollar  float64
	GainPercent float64
}

// ComputeGain derives dollar and percentage returns from pre-computed daily values.
// This is a pure function — no DB calls, no split logic. Daily values are already
// split-adjusted, so gain is consistent with the chart the user sees.
func ComputeGain(dailyValues []DailyValue) GainResult {
	if len(dailyValues) == 0 {
		return GainResult{}
	}

	startValue := dailyValues[0].Value
	endValue := dailyValues[len(dailyValues)-1].Value
	gainDollar := endValue - startValue
	gainPercent := 0.0
	if startValue > 0 {
		gainPercent = gainDollar / startValue
	}

	return GainResult{
		StartValue:  startValue,
		EndValue:    endValue,
		GainDollar:  gainDollar,
		GainPercent: gainPercent,
	}
}

// computeRiskFreeRates fetches US10Y treasury rates and returns:
//   - riskFree: normalized-UTC-midnight date → daily rate
//   - dailyAvgRate: fallback average daily rate for days with no bond data
func (s *PerformanceService) computeRiskFreeRates(ctx context.Context, startDate, endDate time.Time) (map[time.Time]float64, float64, error) {
	US10Y, err := s.secRepo.GetByTicker(ctx, "US10Y")
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get US10Y security: %w", err)
	}

	treasuryRates, _, err := s.pricingSvc.GetDailyPrices(ctx, US10Y.ID, startDate, endDate)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get treasury rates: %w", err)
	}

	riskFree := make(map[time.Time]float64)
	var avgRiskFreeRate float64
	if len(treasuryRates) > 0 {
		var sum float64
		for _, tr := range treasuryRates {
			sum += tr.Close
			dailyRate := math.Pow(1.0+(float64(tr.Close)/100.0), 1.0/float64(tradingDaysPerYear)) - 1.0
			normalizedDate := time.Date(tr.Date.Year(), tr.Date.Month(), tr.Date.Day(), 0, 0, 0, 0, time.UTC)
			riskFree[normalizedDate] = dailyRate
		}
		avgRiskFreeRate = sum / float64(len(treasuryRates))
	}

	dailyAvgRate := math.Pow(1.0+(avgRiskFreeRate/100.0), 1.0/float64(tradingDaysPerYear)) - 1.0
	return riskFree, dailyAvgRate, nil
}

// interpolateRiskFreeRate returns the risk-free daily rate for date.
// If the date is missing from riskFree (bond market closed, stock market open),
// it interpolates from neighbors within ±5 days, falling back to dailyAvg.
func interpolateRiskFreeRate(riskFree map[time.Time]float64, date time.Time, dailyAvg float64) float64 {
	if rf, found := riskFree[date]; found {
		return rf
	}
	var prev, next float64
	var foundPrev, foundNext bool
	for offset := 1; offset <= 5; offset++ {
		if !foundPrev {
			if v, ok := riskFree[date.AddDate(0, 0, -offset)]; ok {
				prev = v
				foundPrev = true
			}
		}
		if !foundNext {
			if v, ok := riskFree[date.AddDate(0, 0, offset)]; ok {
				next = v
				foundNext = true
			}
		}
		if foundPrev && foundNext {
			break
		}
	}
	switch {
	case foundPrev && foundNext:
		return (prev + next) / 2.0
	case foundPrev:
		return prev
	case foundNext:
		return next
	default:
		return dailyAvg
	}
}

// computeExcessReturns fetches US10Y treasury rates and computes the daily excess return
// (portfolio return minus risk-free rate) for each consecutive day in dailyValues.
// Used by both ComputeSharpe and ComputeSortino to avoid duplicating treasury fetch logic.
func (s *PerformanceService) computeExcessReturns(ctx context.Context, dailyValues []DailyValue, startDate, endDate time.Time) ([]float64, error) {
	riskFree, dailyAvgRiskFreeRate, err := s.computeRiskFreeRates(ctx, startDate, endDate)
	if err != nil {
		return nil, err
	}

	var excessReturns []float64
	for i := 1; i < len(dailyValues); i++ {
		dailyReturn := (dailyValues[i].Value - dailyValues[i-1].Value) / dailyValues[i-1].Value
		normalizedDate := time.Date(dailyValues[i].Date.Year(), dailyValues[i].Date.Month(), dailyValues[i].Date.Day(), 0, 0, 0, 0, time.UTC)
		dailyRF := interpolateRiskFreeRate(riskFree, normalizedDate, dailyAvgRiskFreeRate)
		if _, found := riskFree[normalizedDate]; !found {
			log.Infof("Missing daily Risk Free Rate on day: %s, interpolated from neighbors", dailyValues[i].Date)
		}
		excessReturns = append(excessReturns, dailyReturn-dailyRF)
	}
	return excessReturns, nil
}

// ComputeSharpe calculates Sharpe ratios from pre-computed daily values
// to convert a risk free value at an annual rate assuming n=interest rate, p=period
// daily_rate = (1+n)^(1/p)-1
// in this case, we would want p=252 for trading days in the year.
// n is divided by 100 in computeRiskFreeRates (FRED returns 4.52 meaning 4.52%, not 0.0452)
// Return: day (1×), month (√20×), 3m (√60×), year (√252×)
func (s *PerformanceService) ComputeSharpe(ctx context.Context, dailyValues []DailyValue, startDate, endDate time.Time) (models.SharpeRatios, error) {
	defer TrackTime("ComputeSharpe", time.Now())
	if len(dailyValues) < 2 {
		return models.SharpeRatios{}, nil
	}

	excessReturns, err := s.computeExcessReturns(ctx, dailyValues, startDate, endDate)
	if err != nil {
		return models.SharpeRatios{}, err
	}

	// Calculate mean excess return
	var sumExcess float64
	for _, er := range excessReturns {
		sumExcess += er
	}
	meanExcessReturn := sumExcess / float64(len(excessReturns))

	// Calculate standard deviation of excess returns
	var sumSquaredDiff float64
	for _, er := range excessReturns {
		diff := er - meanExcessReturn
		sumSquaredDiff += diff * diff
	}
	stdDevExcessReturn := math.Sqrt(sumSquaredDiff / float64(len(excessReturns)))

	// Calculate daily Sharpe ratio
	dailySharpe := 0.0
	if stdDevExcessReturn > 0 {
		dailySharpe = meanExcessReturn / stdDevExcessReturn
	}

	// Annualize Sharpe ratios for different periods
	// day (1×), month (√20×), 3m (√60×), year (√252×)
	return models.SharpeRatios{
		Daily:      dailySharpe,
		Monthly:    dailySharpe * math.Sqrt(20),
		ThreeMonth: dailySharpe * math.Sqrt(60),
		Yearly:     dailySharpe * math.Sqrt(tradingDaysPerYear),
	}, nil
}

// ComputeSortino calculates Sortino ratios from pre-computed daily values.
// Unlike Sharpe, only downside deviations (negative excess returns) contribute to
// the denominator. Downside deviation divides by N (total observations), not just negative ones.
// Return: day (1×), month (√20×), 3m (√60×), year (√252×)
func (s *PerformanceService) ComputeSortino(ctx context.Context, dailyValues []DailyValue, startDate, endDate time.Time) (models.SortinoRatios, error) {
	defer TrackTime("ComputeSortino", time.Now())
	if len(dailyValues) < 2 {
		return models.SortinoRatios{}, nil
	}

	excessReturns, err := s.computeExcessReturns(ctx, dailyValues, startDate, endDate)
	if err != nil {
		return models.SortinoRatios{}, err
	}

	// Calculate mean excess return
	var sumExcess float64
	for _, er := range excessReturns {
		sumExcess += er
	}
	meanExcessReturn := sumExcess / float64(len(excessReturns))

	// Downside deviation: use only negative excess returns in the numerator,
	// but divide by N (total observations) — standard Sortino formulation.
	var sumSquaredDownside float64
	for _, er := range excessReturns {
		if er < 0 {
			sumSquaredDownside += er * er
		}
	}
	downsideDev := math.Sqrt(sumSquaredDownside / float64(len(excessReturns)))

	dailySortino := 0.0
	if downsideDev > 0 {
		dailySortino = meanExcessReturn / downsideDev
	}

	return models.SortinoRatios{
		Daily:      dailySortino,
		Monthly:    dailySortino * math.Sqrt(20),
		ThreeMonth: dailySortino * math.Sqrt(60),
		Yearly:     dailySortino * math.Sqrt(tradingDaysPerYear),
	}, nil
}

// DailyValue represents portfolio value on a specific date
type DailyValue struct {
	Date  time.Time
	Value float64
}

// ToModelDailyValues converts internal DailyValue slice to model DailyValue slice
func ToModelDailyValues(values []DailyValue) []models.DailyValue {
	result := make([]models.DailyValue, len(values))
	for i, v := range values {
		result[i] = models.DailyValue{
			Date:  v.Date.Format("2006-01-02"),
			Value: v.Value,
		}
	}
	return result
}

// ComputeDailyValues calculates the portfolio value for each trading day in the period.
// PercentageOrShares is treated as shares (works for actual portfolios or normalized ideal portfolios).
// Only returns dates where all securities in the portfolio have price data.
//
// priceOverrides is an optional caller-supplied map (securityID → date → price) of synthetic
// prices to inject for dates that have no real data. Real prices are never overwritten.
// Pass nil to use only real prices (current behaviour). Build the map with SynthesizeCashPrices.
func (s *PerformanceService) ComputeDailyValues(ctx context.Context, portfolio *models.PortfolioWithMemberships, startDate, endDate time.Time, priceOverrides map[int64]map[time.Time]float64) ([]DailyValue, error) {
	defer TrackTime("ComputeDailyValues", time.Now())
	// Collect all security IDs.
	secIDs := make([]int64, len(portfolio.Memberships))
	for i, m := range portfolio.Memberships {
		secIDs[i] = m.SecurityID
	}

	// Derive a cancellable context for fail-fast error propagation.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Step 1: Bulk range check — one DB query for all securities.
	// Replaces N individual GetPriceRange + GetByIDWithCountry calls on the cache-hit path.
	secMeta, err := s.pricingSvc.GetPriceRangesWithInception(ctx, secIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to bulk-check price ranges: %w", err)
	}

	// Step 2: Classify — which securities have a stale or missing cache entry?
	type fetchItem struct {
		secID      int64
		priceRange *repository.PriceRange
	}
	now := time.Now()
	var toFetch []fetchItem
	for _, secID := range secIDs {
		meta := secMeta[secID]
		var pr *repository.PriceRange
		var inception *time.Time
		if meta != nil {
			pr = meta.PriceRange
			inception = meta.Inception
		}
		needsFetch, _, _ := DetermineFetch(pr, now, effectiveStartDate(startDate, inception), endDate)
		// ↑ adjStartDT/adjEndDT discarded — EnsurePricesCached snaps currentDT fresh
		//   and recomputes them, avoiding drift if this loop runs slowly
		if needsFetch {
			toFetch = append(toFetch, fetchItem{secID: secID, priceRange: pr})
		}
	}

	// Step 3: Provider fetch — only for securities with stale/missing cache.
	// Bulk-fetch routing metadata first so goroutines don't each call GetByIDWithCountry.
	// Singleflight inside EnsurePricesCached deduplicates concurrent provider calls when
	// two simultaneous requests both see a cache miss for the same security.
	if len(toFetch) > 0 {
		// Collect security metadata before spawning goroutines. GetByIDWithCountry reads
		// from the in-process snapshot (populated by PrefetchService at startup) so these
		// are in-memory lookups — no DB round-trips on the hot path.
		staleSecurities := make(map[int64]*models.SecurityWithCountry, len(toFetch))
		for _, item := range toFetch {
			sec, err := s.secRepo.GetByIDWithCountry(ctx, item.secID)
			if err != nil {
				return nil, fmt.Errorf("security %d not found: %w", item.secID, err)
			}
			staleSecurities[item.secID] = sec
		}

		type fetchErr struct {
			secID  int64
			ticker string
			err    error
		}
		errCh := make(chan fetchErr, len(toFetch))
		sem := make(chan struct{}, s.priceConcurrency)
		var wg sync.WaitGroup
		for _, item := range toFetch {
			sec := staleSecurities[item.secID]
			wg.Add(1)
			go func(sec *models.SecurityWithCountry, pr *repository.PriceRange) {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					errCh <- fetchErr{secID: sec.ID, ticker: sec.Ticker, err: ctx.Err()}
					return
				}
				if ferr := s.pricingSvc.EnsurePricesCached(ctx, sec, pr, startDate, endDate); ferr != nil {
					errCh <- fetchErr{secID: sec.ID, ticker: sec.Ticker, err: ferr}
					cancel()
				}
			}(sec, item.priceRange)
		}
		go func() { wg.Wait(); close(errCh) }()
		for fe := range errCh {
			return nil, fmt.Errorf("failed to cache prices for %s (id=%d): %w", fe.ticker, fe.secID, fe.err)
		}
	}

	// Step 4: Bulk DB reads — two queries cover all securities regardless of portfolio size.
	allPrices, err := s.pricingSvc.GetDailyPricesMulti(ctx, secIDs, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to bulk-fetch prices: %w", err)
	}
	allSplits, err := s.pricingSvc.GetDailySplitsMulti(ctx, secIDs, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to bulk-fetch splits: %w", err)
	}

	pricesBySecID := make(map[int64]map[time.Time]float64, len(secIDs))
	for secID, prices := range allPrices {
		pm := make(map[time.Time]float64, len(prices))
		for _, p := range prices {
			pm[p.Date] = p.Close
		}
		pricesBySecID[secID] = pm
	}
	splitsBySecID := make(map[int64]map[time.Time]float64, len(secIDs))
	for secID, events := range allSplits {
		sm := make(map[time.Time]float64, len(events))
		for _, e := range events {
			sm[e.Date] = e.SplitCoefficient
		}
		splitsBySecID[secID] = sm
	}

	// Remap split events that fall on non-trading days (weekends, NYSE holidays) to the
	// next trading day. When a company declares a split effective on a market-closure day
	// (e.g., MLK Day), shares are adjusted at the next market open. The forward loop
	// below uses exact-date lookup against the filtered trading-day list; without this
	// remapping a holiday-dated split would be silently skipped for the rest of the period.
	for secID, splits := range splitsBySecID {
		remapped := make(map[time.Time]float64, len(splits))
		for splitDate, coeff := range splits {
			effective := splitDate
			for effective.Weekday() == time.Saturday || effective.Weekday() == time.Sunday || IsUSMarketHoliday(effective) {
				effective = effective.AddDate(0, 0, 1)
			}
			if existing, ok := remapped[effective]; ok {
				remapped[effective] = existing * coeff
			} else {
				remapped[effective] = coeff
			}
		}
		splitsBySecID[secID] = remapped
	}

	// Merge caller-supplied synthetic prices for dates not already in pricesBySecID.
	// Real prices are never overwritten. This runs before dateSet is built so synthetic
	// dates flow naturally into the trading-day list and the main value loop.
	for secID, overrideMap := range priceOverrides {
		pm := pricesBySecID[secID]
		if pm == nil {
			pm = make(map[time.Time]float64)
			pricesBySecID[secID] = pm
		}
		for d, price := range overrideMap {
			if _, exists := pm[d]; !exists {
				pm[d] = price
			}
		}
	}

	// Find all dates where we have prices for all securities, and only return those.
	// This logic takes a while - look for //REJECT HERE to see where we kick out missing data.
	// It is possible we have overachievers reporting data on US holidays/weekends, especially with foreign data. SPAXX also
	// autofills into these ranges. So we do want to ignore dates for which we don't have all data, or dates for which we just
	// have one or two overachievers.
	dateSet := make(map[time.Time]bool)
	for _, priceMap := range pricesBySecID {
		for date := range priceMap {
			dateSet[date] = true
		}
	}

	// Sort dates
	var dates []time.Time
	for date := range dateSet {
		dates = append(dates, date)
	}
	sort.Slice(dates, func(i, j int) bool {
		return dates[i].Before(dates[j])
	})

	// Filter out weekends and US market holidays. Some foreign securities (e.g., Irish
	// stocks) trade on days the US market is closed (Thanksgiving, etc.). Including those
	// dates would cause all US holdings to be forward-filled on a day that shouldn't
	// appear in the portfolio's value series at all.
	{
		filtered := dates[:0]
		for _, d := range dates {
			if d.Weekday() != time.Saturday && d.Weekday() != time.Sunday && !IsUSMarketHoliday(d) {
				filtered = append(filtered, d)
			}
		}
		dates = filtered
	}

	// Build mutable shares map — split adjustments accumulate over time.
	// lastKnownPrice tracks the most recent observed close for forward-filling
	// days where a security has no data (e.g., thinly-traded ADR, holiday gap).
	sharesMap := make(map[int64]float64)
	lastKnownPrice := make(map[int64]float64)
	for _, m := range portfolio.Memberships {
		sharesMap[m.SecurityID] = m.PercentageOrShares
	}

	// Pre-seed lastKnownPrice for securities that have no price on the first trading day.
	// Without this, a thinly-traded security (e.g., an ADR that skips the portfolio start
	// date) has no lastKnownPrice when the loop first hits it, and the date is incorrectly
	// flagged as "hard missing" rather than forward-filled.
	//
	// Only fetches for securities actually missing on dates[0], so the common case (all
	// securities have prices on the first day) pays zero DB cost.
	//
	// Gap splits: splitsBySecID covers [startDate, endDate] only. If a split occurred
	// between the pre-seed date and startDate it won't fire in the forward loop, so the
	// seeded price must be divided by any such coefficients to reflect the post-split price.
	if len(dates) > 0 {
		firstDate := dates[0]
		var needSeed []int64
		for _, m := range portfolio.Memberships {
			if _, ok := pricesBySecID[m.SecurityID][firstDate]; !ok {
				needSeed = append(needSeed, m.SecurityID)
			}
		}

		if len(needSeed) > 0 {
			preSeed, err := s.pricingSvc.GetLastPricesBeforeMulti(ctx, needSeed, startDate)
			if err != nil {
				return nil, fmt.Errorf("failed to pre-seed last known prices: %w", err)
			}

			if len(preSeed) > 0 {
				// Find the earliest pre-seed date to bound the gap-split query.
				minPreSeedDate := startDate
				for _, pd := range preSeed {
					if pd.Date.Before(minPreSeedDate) {
						minPreSeedDate = pd.Date
					}
				}
				gapSplits, err := s.pricingSvc.GetDailySplitsMulti(ctx, needSeed, minPreSeedDate, startDate.AddDate(0, 0, -1))
				if err != nil {
					return nil, fmt.Errorf("failed to fetch gap splits for pre-seed: %w", err)
				}

				for secID, pd := range preSeed {
					price := pd.Close
					// Apply splits that occurred strictly after this security's pre-seed date.
					// Earlier splits are already reflected in pd.Close (the actual market close).
					for _, ev := range gapSplits[secID] {
						if ev.Date.After(pd.Date) && ev.SplitCoefficient != 0 {
							price /= ev.SplitCoefficient
						}
					}
					lastKnownPrice[secID] = price
				}
			}
		}
	}

	// Adjust sharesMap when snapshotted_at is set, to account for splits that occurred
	// between snapshotted_at and startDate (in either direction).
	// Only applies to Active portfolios — Ideal portfolios use normalized share counts.
	if portfolio.Portfolio.SnapshottedAt != nil &&
		portfolio.Portfolio.PortfolioType == models.PortfolioTypeActive {
		snapDate := time.Date(
			portfolio.Portfolio.SnapshottedAt.Year(),
			portfolio.Portfolio.SnapshottedAt.Month(),
			portfolio.Portfolio.SnapshottedAt.Day(),
			0, 0, 0, 0, time.UTC,
		)
		startNorm := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.UTC)

		if snapDate.After(startNorm) {
			// snapshotted_at is AFTER startDate: snapshot shares are already post-split
			// for splits that happened after startDate. Reverse those splits so the
			// forward loop can re-apply them correctly, giving accurate values from startDate.
			// splitsBySecID already covers [startDate, endDate] so all relevant splits are present.
			for _, m := range portfolio.Memberships {
				cumulativeCoeff := 1.0
				for splitDate, coeff := range splitsBySecID[m.SecurityID] {
					// Half-open [startDate, snapshotted): a split on the snapshot date is NOT
					// reversed — snapshot shares already reflect that day's post-split count.
					if !splitDate.Before(startNorm) && splitDate.Before(snapDate) {
						cumulativeCoeff *= coeff
					}
				}
				if cumulativeCoeff != 0 && cumulativeCoeff != 1.0 {
					sharesMap[m.SecurityID] /= cumulativeCoeff
				}
			}
		} else if snapDate.Before(startNorm) {
			// snapshotted_at is BEFORE startDate: splits in [snapshotted_at, startDate) occurred
			// after the snapshot but are outside the fetched price window, so the forward loop
			// will never apply them. Fetch and apply them forward now.
			gapCoeffs, err := s.pricingSvc.GetSplitAdjustmentsBatch(ctx, secIDs, snapDate, startNorm.AddDate(0, 0, -1))
			if err != nil {
				return nil, fmt.Errorf("failed to fetch split adjustments for snapshotted gap: %w", err)
			}
			for _, m := range portfolio.Memberships {
				if coeff, ok := gapCoeffs[m.SecurityID]; ok && coeff != 0 && coeff != 1.0 {
					sharesMap[m.SecurityID] *= coeff
				}
			}
		}
	}

	// Tracks securities with no price history at all (no forward-fill possible).
	// Keyed by SecurityID to deduplicate across dates; value is the ticker for reporting.
	missingPriceHistory := make(map[int64]string)
	// Tracks tickers that caused the tooManyMissing threshold to fire on at least one date.
	excessiveForwardFill := make(map[string]struct{})

	// Reject the day if more than this fraction of holdings needed forward-filling.
	// A single thinly-traded security missing one day is fine; a broad data outage is not.
	const maxMissingFraction = 0.10

	// Calculate portfolio value for each date, adjusting shares on split dates
	var dailyValues []DailyValue
	for _, date := range dates {
		// Apply split adjustments before computing value.
		// On a 2-for-1 split, coefficient is 2: price halves, shares double.
		// Also adjust lastKnownPrice so any forward-fill on this day uses the post-split price.
		for _, m := range portfolio.Memberships {
			if coeff, ok := splitsBySecID[m.SecurityID][date]; ok {
				sharesMap[m.SecurityID] *= coeff
				if lp, ok := lastKnownPrice[m.SecurityID]; ok {
					lastKnownPrice[m.SecurityID] = lp / coeff
				}
			}
		}

		var value float64
		missingCount := 0
		hardMissing := false // a security has no price history at all — cannot forward-fill
		var forwardFilledThisDate []string
		for _, m := range portfolio.Memberships {
			price, exists := pricesBySecID[m.SecurityID][date]
			if exists {
				lastKnownPrice[m.SecurityID] = price
			} else {
				lp, hasPrior := lastKnownPrice[m.SecurityID]
				if !hasPrior {
					hardMissing = true
					if _, seen := missingPriceHistory[m.SecurityID]; !seen {
						log.Warnf("ComputeDailyValues: security %q (id=%d) has no price history — %s excluded from results", m.Ticker, m.SecurityID, date.Format("2006-01-02"))
						missingPriceHistory[m.SecurityID] = m.Ticker
					}
					break
				}
				price = lp
				missingCount++
				forwardFilledThisDate = append(forwardFilledThisDate, m.Ticker)
				log.Debugf("ComputeDailyValues: forward-filling security %s (%d) on %s with %.4f",
					m.Ticker, m.SecurityID, date.Format("2006-01-02"), lp)
			}
			value += sharesMap[m.SecurityID] * price
		}

		// Apply the fraction threshold only when 2+ securities are missing.
		// A single thinly-traded security (e.g., an ADR that skips a day) is always
		// forward-filled regardless of portfolio size, so small portfolios aren't
		// penalized by a 1/2 = 50% ratio on what is really just one quiet stock.
		missingFraction := float64(missingCount) / float64(len(portfolio.Memberships))
		tooManyMissing := missingCount >= 2 && missingFraction > maxMissingFraction
		if hardMissing || tooManyMissing {
			if hardMissing {
				log.Warnf("ComputeDailyValues: skipping %s — a security has no price history yet",
					date.Format("2006-01-02"))
			} else {
				log.Errorf("ComputeDailyValues: skipping %s — %d/%d securities forward-filled (%.0f%% > %.0f%% threshold): %s",
					date.Format("2006-01-02"), missingCount, len(portfolio.Memberships),
					missingFraction*100, maxMissingFraction*100, strings.Join(forwardFilledThisDate, ", "))
				for _, ticker := range forwardFilledThisDate {
					excessiveForwardFill[ticker] = struct{}{}
				}
			}
			continue
		}

		dailyValues = append(dailyValues, DailyValue{
			Date:  date,
			Value: value,
		})
	}

	if len(missingPriceHistory) > 0 {
		tickers := make([]string, 0, len(missingPriceHistory))
		for _, ticker := range missingPriceHistory {
			tickers = append(tickers, ticker)
		}
		sort.Strings(tickers)
		AddWarning(ctx, models.Warning{
			Code:    models.WarnMissingPriceHistory,
			Message: fmt.Sprintf("securities with no price history (affected dates excluded): %s", strings.Join(tickers, ", ")),
		})
	}

	if len(excessiveForwardFill) > 0 {
		tickers := make([]string, 0, len(excessiveForwardFill))
		for ticker := range excessiveForwardFill {
			tickers = append(tickers, ticker)
		}
		sort.Strings(tickers)
		AddWarning(ctx, models.Warning{
			Code:    models.WarnExcessiveForwardFill,
			Message: fmt.Sprintf("securities with sparse data caused dates to be excluded (forward-fill threshold exceeded): %s", strings.Join(tickers, ", ")),
		})
	}

	return dailyValues, nil
}

// ComputeDividends calculates dividends received during the period. It assumes you have passed in an actual or normalized ideal portfolio to do the math.
func (s *PerformanceService) ComputeDividends(ctx context.Context, portfolio *models.PortfolioWithMemberships, startDate, endDate time.Time) (float64, error) {
	dividendEvents, err := s.pricingSvc.GetAggregatePortfolioDividends(ctx, portfolio.Portfolio.ID, startDate, endDate)
	if err != nil {
		log.Errorf("Couldn't fetch portfolio # %d dividends: %s", portfolio.Portfolio.ID, err)
		return 0, err
	}

	// build a quick map of my portfolio memberships -> security_id to share count.
	idToShareCount := make(map[int64]float64)
	for _, member := range portfolio.Memberships {
		idToShareCount[member.SecurityID] = member.PercentageOrShares
	}

	dividendSum := 0.0
	//now multiply each dividend against the membership amount.
	for _, divEvent := range dividendEvents {
		dividendSum += divEvent.Dividend * idToShareCount[divEvent.SecurityID]
	}

	return dividendSum, nil
}

// GetPriceAtDate returns the closing price of a security on or before the given date.
func (s *PerformanceService) GetPriceAtDate(ctx context.Context, securityID int64, date time.Time) (float64, error) {
	return s.pricingSvc.GetPriceAtDate(ctx, securityID, date)
}

// FetchBenchmarkPrices retrieves daily closing prices for a benchmark security.
// Events (splits/dividends) are discarded; benchmarks are price-only indices.
func (s *PerformanceService) FetchBenchmarkPrices(ctx context.Context, securityID int64, startDate, endDate time.Time) ([]models.PriceData, error) {
	prices, _, err := s.pricingSvc.GetDailyPrices(ctx, securityID, startDate, endDate)
	return prices, err
}

// ComputeAlphaBeta computes Jensen's Alpha (annualized) and Beta for a portfolio relative to
// a benchmark index. benchmarkPrices must be pre-fetched by the caller; an empty slice means
// no benchmark data is available, in which case AlphaBeta{} is returned.
//
// Forward-fill: if the benchmark has no price for a portfolio date, the most recently known
// benchmark price is carried forward (same convention as ComputeDailyValues for the portfolio).
//
// Formula:
//
//	beta  = Cov(Rp_excess, Rb_excess) / Var(Rb_excess)
//	alpha = (meanRp_excess - beta * meanRb_excess) * tradingDaysPerYear
func (s *PerformanceService) ComputeAlphaBeta(
	ctx context.Context,
	dailyValues []DailyValue,
	benchmarkPrices []models.PriceData,
	startDate, endDate time.Time,
) (models.AlphaBeta, error) {
	defer TrackTime("ComputeAlphaBeta", time.Now())
	if len(dailyValues) < 2 || len(benchmarkPrices) == 0 {
		return models.AlphaBeta{}, nil
	}

	// Build a forward-filled benchmark price for each portfolio date.
	// benchmarkPrices is already ordered chronologically from GetDailyPrices.
	forwardFilledBench := make(map[time.Time]float64, len(dailyValues))
	var lastBenchPrice float64
	bi := 0
	for _, dv := range dailyValues {
		normDate := time.Date(dv.Date.Year(), dv.Date.Month(), dv.Date.Day(), 0, 0, 0, 0, time.UTC)
		// Advance benchmark pointer while its date is <= the current portfolio date.
		for bi < len(benchmarkPrices) {
			bDate := time.Date(benchmarkPrices[bi].Date.Year(), benchmarkPrices[bi].Date.Month(), benchmarkPrices[bi].Date.Day(), 0, 0, 0, 0, time.UTC)
			if bDate.After(normDate) {
				break
			}
			lastBenchPrice = benchmarkPrices[bi].Close
			bi++
		}
		if lastBenchPrice > 0 {
			forwardFilledBench[normDate] = lastBenchPrice
		}
	}

	riskFree, dailyAvgRF, err := s.computeRiskFreeRates(ctx, startDate, endDate)
	if err != nil {
		return models.AlphaBeta{}, err
	}

	var portfolioExcess, benchExcess []float64
	for i := 1; i < len(dailyValues); i++ {
		normDate := time.Date(dailyValues[i].Date.Year(), dailyValues[i].Date.Month(), dailyValues[i].Date.Day(), 0, 0, 0, 0, time.UTC)
		normPrev := time.Date(dailyValues[i-1].Date.Year(), dailyValues[i-1].Date.Month(), dailyValues[i-1].Date.Day(), 0, 0, 0, 0, time.UTC)

		bPrev, hasPrev := forwardFilledBench[normPrev]
		bCurr, hasCurr := forwardFilledBench[normDate]
		if !hasPrev || !hasCurr || bPrev == 0 {
			continue // can't compute benchmark return for this pair
		}

		rfRate := interpolateRiskFreeRate(riskFree, normDate, dailyAvgRF)
		portfolioReturn := (dailyValues[i].Value - dailyValues[i-1].Value) / dailyValues[i-1].Value
		benchReturn := (bCurr - bPrev) / bPrev

		portfolioExcess = append(portfolioExcess, portfolioReturn-rfRate)
		benchExcess = append(benchExcess, benchReturn-rfRate)
	}

	if len(portfolioExcess) == 0 {
		return models.AlphaBeta{}, nil
	}

	n := float64(len(portfolioExcess))
	var sumRp, sumRb float64
	for i := range portfolioExcess {
		sumRp += portfolioExcess[i]
		sumRb += benchExcess[i]
	}
	meanRp := sumRp / n
	meanRb := sumRb / n

	var cov, varB float64
	for i := range portfolioExcess {
		dp := portfolioExcess[i] - meanRp
		db := benchExcess[i] - meanRb
		cov += dp * db
		varB += db * db
	}
	cov /= n
	varB /= n

	beta := 0.0
	if varB > 0 {
		beta = cov / varB
	}
	dailyAlpha := meanRp - beta*meanRb
	alpha := dailyAlpha * tradingDaysPerYear

	return models.AlphaBeta{Alpha: alpha, Beta: beta}, nil
}

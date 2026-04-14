package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// basketETFInfo holds precomputed constituent availability for one A-ETF,
// relative to portfolio B's expanded stock pool.
type basketETFInfo struct {
	availableConst  []models.ETFMembership
	availableWeight float64
	totalWeight     float64 // sum of all constituent Percentage values; used to normalize per-constituent caps
}

// ComparisonService orchestrates portfolio comparisons
type ComparisonService struct {
	portfolioSvc   *PortfolioService
	membershipSvc  *MembershipService
	performanceSvc *PerformanceService
	secRepo        *repository.SecurityRepository
}

// NewComparisonService creates a new ComparisonService
func NewComparisonService(
	portfolioSvc *PortfolioService,
	membershipSvc *MembershipService,
	performanceSvc *PerformanceService,
	secRepo *repository.SecurityRepository,
) *ComparisonService {
	return &ComparisonService{
		portfolioSvc:   portfolioSvc,
		membershipSvc:  membershipSvc,
		performanceSvc: performanceSvc,
		secRepo:        secRepo,
	}
}

// ComparePortfolios performs a full comparison between two portfolios
// Comparison supports: [actual,actual], [actual,ideal], [ideal,actual], [ideal,ideal]
func (s *ComparisonService) ComparePortfolios(ctx context.Context, req *models.CompareRequest) (*models.CompareResponse, error) {
	defer TrackTime("ComparePortfolios", time.Now())

	//FIXME: Adjust the start/end dates to be on Market-Open Days AND Days we have data for. If you are requesting data that won't be there until
	//8 hours from now, then don't use that end date. If you're asking for data on Saturday as an end date, just move back to Friday.

	// Get both portfolios
	portfolioA, err := s.portfolioSvc.GetPortfolio(ctx, req.PortfolioA)
	if err != nil {
		return nil, fmt.Errorf("failed to get portfolio A: %w", err)
	}

	portfolioB, err := s.portfolioSvc.GetPortfolio(ctx, req.PortfolioB)
	if err != nil {
		return nil, fmt.Errorf("failed to get portfolio B: %w", err)
	}

	// Pre-fetch ALL securities once; reused for inception date calculation,
	// ComputeMembership (by-ID and by-symbol), ComputeDirectMembership (by-ID),
	// and GetETFHoldings (by-ID) to eliminate per-ETF DB calls.
	allSecurities, allBySymbol, err := s.secRepo.GetAllSecurities(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to pre-fetch securities: %w", err)
	}

	// Compute latest inception date from portfolio members only (not all securities)
	var latestInception *time.Time
	var latestSecurity *models.Security
	for _, m := range portfolioA.Memberships {
		if sec := allSecurities[m.SecurityID]; sec != nil && sec.Inception != nil {
			if latestInception == nil || sec.Inception.After(*latestInception) {
				latestInception = sec.Inception
				latestSecurity = sec
			}
		}
	}
	for _, m := range portfolioB.Memberships {
		if sec := allSecurities[m.SecurityID]; sec != nil && sec.Inception != nil {
			if latestInception == nil || sec.Inception.After(*latestInception) {
				latestInception = sec.Inception
				latestSecurity = sec
			}
		}
	}
	if latestInception != nil && req.StartPeriod.Time.Before(*latestInception) {
		req.StartPeriod.Time = *latestInception
		log.Warnf("ComparePortfolios: ipo/inception date of security %d (%s) moves the comparison date range", latestSecurity.ID, latestSecurity.Ticker)
		AddWarning(ctx, models.Warning{
			Code:    models.WarnStartDateAdjusted,
			Message: fmt.Sprintf("The start date was adjusted to %s to reflect the inception date of one or more securities in the comparison.", latestInception.Format("2006-01-02")),
		})
	}

	// Pre-warm any stale ETFs from either portfolio serially before parallel computation.
	// This prevents concurrent AV fetches and duplicate warnings when both portfolios
	// share the same stale ETF (including self-compares using the same portfolio).
	etfIDSet := make(map[int64]struct{})
	for _, pm := range append(portfolioA.Memberships, portfolioB.Memberships...) {
		sec := allSecurities[pm.SecurityID]
		if sec != nil && (sec.Type == string(models.SecurityTypeETF) || sec.Type == string(models.SecurityTypeMutualFund)) {
			etfIDSet[pm.SecurityID] = struct{}{}
		}
	}
	allPortfolioETFIDs := make([]int64, 0, len(etfIDSet))
	for id := range etfIDSet {
		allPortfolioETFIDs = append(allPortfolioETFIDs, id)
	}
	prewarmRanges, err := s.secRepo.GetETFPullRanges(ctx, allPortfolioETFIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to check ETF staleness: %w", err)
	}
	now := time.Now()
	for _, etfID := range allPortfolioETFIDs {
		sec := allSecurities[etfID]
		if sec == nil {
			continue
		}
		pr := prewarmRanges[etfID]
		if pr == nil || now.After(pr.NextUpdate) {
			if _, _, fetchErr := s.membershipSvc.FetchOrRefreshETFHoldings(ctx, etfID, sec.Ticker, allSecurities, allBySymbol); fetchErr != nil {
				log.Warnf("ComparePortfolios: failed to pre-warm ETF %s: %v", sec.Ticker, fetchErr)
			}
		}
	}

	// Compute expanded and direct memberships for both portfolios in parallel.
	// All ETFs are now fresh; allSecurities/allBySymbol are read-only; WarningCollector is mutex-protected.
	type portfolioResult struct {
		expanded []models.ExpandedMembership
		direct   []models.ExpandedMembership
		err      error
	}
	var resA, resB portfolioResult
	var membershipWg sync.WaitGroup
	membershipWg.Add(2)
	go func() {
		defer membershipWg.Done()
		resA.expanded, resA.err = s.membershipSvc.ComputeMembership(ctx, portfolioA.Portfolio.ID, portfolioA.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities, allBySymbol)
		if resA.err != nil {
			return
		}
		resA.direct, resA.err = s.membershipSvc.ComputeDirectMembership(ctx, portfolioA.Portfolio.ID, portfolioA.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities)
	}()
	go func() {
		defer membershipWg.Done()
		resB.expanded, resB.err = s.membershipSvc.ComputeMembership(ctx, portfolioB.Portfolio.ID, portfolioB.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities, allBySymbol)
		if resB.err != nil {
			return
		}
		resB.direct, resB.err = s.membershipSvc.ComputeDirectMembership(ctx, portfolioB.Portfolio.ID, portfolioB.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities)
	}()
	membershipWg.Wait()
	if resA.err != nil {
		return nil, fmt.Errorf("failed to compute membership for portfolio A: %w", resA.err)
	}
	if resB.err != nil {
		return nil, fmt.Errorf("failed to compute membership for portfolio B: %w", resB.err)
	}
	expandedA := resA.expanded
	expandedB := resB.expanded
	directA := resA.direct
	directB := resB.direct

	// Compute similarity score
	similarityScore := s.ComputeSimilarity(expandedA, expandedB)

	aIsIdeal := portfolioA.Portfolio.PortfolioType == models.PortfolioTypeIdeal
	bIsIdeal := portfolioB.Portfolio.PortfolioType == models.PortfolioTypeIdeal

	// Compute daily values and normalize portfolios
	// For actual portfolios: use original pointer, get startValue from dailyValues[0]
	// For ideal portfolios: normalize to actual's start value (or $100 if both ideal)

	var pA, pB *models.PortfolioWithMemberships
	var dailyValuesA, dailyValuesB []DailyValue
	var startValueA, startValueB float64

	// Compute daily values for actual portfolios.
	// actual+actual: both are independent — run in parallel.
	// mixed (actual+ideal): the actual must run first so its start value can seed the ideal's
	// normalization; keep those sequential.
	if !aIsIdeal && !bIsIdeal {
		pA, pB = portfolioA, portfolioB
		var errA, errB error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			dailyValuesA, errA = s.performanceSvc.ComputeDailyValues(ctx, pA, req.StartPeriod.Time, req.EndPeriod.Time)
		}()
		go func() {
			defer wg.Done()
			dailyValuesB, errB = s.performanceSvc.ComputeDailyValues(ctx, pB, req.StartPeriod.Time, req.EndPeriod.Time)
		}()
		wg.Wait()
		if errA != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio A: %w", errA)
		}
		if len(dailyValuesA) == 0 {
			return nil, fmt.Errorf("no daily values for portfolio A")
		}
		startValueA = dailyValuesA[0].Value
		if errB != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio B: %w", errB)
		}
		if len(dailyValuesB) == 0 {
			return nil, fmt.Errorf("no daily values for portfolio B")
		}
		startValueB = dailyValuesB[0].Value
	} else if !aIsIdeal {
		// A is actual, B is ideal: need A's start value before normalizing B.
		pA = portfolioA
		dailyValuesA, err = s.performanceSvc.ComputeDailyValues(ctx, pA, req.StartPeriod.Time, req.EndPeriod.Time)
		if err != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio A: %w", err)
		}
		if len(dailyValuesA) == 0 {
			return nil, fmt.Errorf("no daily values for portfolio A")
		}
		startValueA = dailyValuesA[0].Value
	} else if !bIsIdeal {
		// B is actual, A is ideal: need B's start value before normalizing A.
		pB = portfolioB
		dailyValuesB, err = s.performanceSvc.ComputeDailyValues(ctx, pB, req.StartPeriod.Time, req.EndPeriod.Time)
		if err != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio B: %w", err)
		}
		if len(dailyValuesB) == 0 {
			return nil, fmt.Errorf("no daily values for portfolio B")
		}
		startValueB = dailyValuesB[0].Value
	}

	// Determine start value for ideal portfolios: use actual's value if mixed, else $100
	idealStartValue := 100.0
	if !aIsIdeal && bIsIdeal {
		idealStartValue = startValueA
	} else if aIsIdeal && !bIsIdeal {
		idealStartValue = startValueB
	}

	// Normalize and compute daily values for ideal portfolios.
	// both ideal: idealStartValue is known ($100) and neither depends on the other — parallel.
	// mixed: the actual was already computed above; the ideal depends on no further data — sequential.
	if aIsIdeal && bIsIdeal {
		var errA, errB error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			var nerr error
			pA, nerr = s.performanceSvc.NormalizeIdealPortfolio(ctx, portfolioA, req.StartPeriod.Time, idealStartValue)
			if nerr != nil {
				errA = nerr
				return
			}
			dailyValuesA, errA = s.performanceSvc.ComputeDailyValues(ctx, pA, req.StartPeriod.Time, req.EndPeriod.Time)
		}()
		go func() {
			defer wg.Done()
			var nerr error
			pB, nerr = s.performanceSvc.NormalizeIdealPortfolio(ctx, portfolioB, req.StartPeriod.Time, idealStartValue)
			if nerr != nil {
				errB = nerr
				return
			}
			dailyValuesB, errB = s.performanceSvc.ComputeDailyValues(ctx, pB, req.StartPeriod.Time, req.EndPeriod.Time)
		}()
		wg.Wait()
		if errA != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio A: %w", errA)
		}
		if errB != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio B: %w", errB)
		}
	} else if aIsIdeal {
		pA, err = s.performanceSvc.NormalizeIdealPortfolio(ctx, portfolioA, req.StartPeriod.Time, idealStartValue)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize portfolio A: %w", err)
		}
		dailyValuesA, err = s.performanceSvc.ComputeDailyValues(ctx, pA, req.StartPeriod.Time, req.EndPeriod.Time)
		if err != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio A: %w", err)
		}
	} else if bIsIdeal {
		pB, err = s.performanceSvc.NormalizeIdealPortfolio(ctx, portfolioB, req.StartPeriod.Time, idealStartValue)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize portfolio B: %w", err)
		}
		dailyValuesB, err = s.performanceSvc.ComputeDailyValues(ctx, pB, req.StartPeriod.Time, req.EndPeriod.Time)
		if err != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio B: %w", err)
		}
	}

	// Compute basket analysis (only when A is ideal). Runs after daily values so that
	// B's end portfolio value and security end prices (already cached) are available.
	var bEndValue float64
	if len(dailyValuesB) > 0 {
		bEndValue = dailyValuesB[len(dailyValuesB)-1].Value
	}
	endPrices := make(map[int64]float64, len(directA))
	for _, m := range directA {
		price, priceErr := s.performanceSvc.GetPriceAtDate(ctx, m.SecurityID, req.EndPeriod.Time)
		if priceErr == nil && price > 0 {
			endPrices[m.SecurityID] = price
		}
	}

	var baskets *models.BasketResult
	if aIsIdeal {
		var basketErr error
		baskets, basketErr = s.ComputeBaskets(ctx, directA, directB, allSecurities, bEndValue, endPrices)
		if basketErr != nil {
			return nil, fmt.Errorf("failed to compute baskets: %w", basketErr)
		}
	}

	// Sharpe, Sortino, Dividends, and Alpha/Beta for A and B are all independent — run all in parallel.
	gainA := ComputeGain(dailyValuesA)
	gainB := ComputeGain(dailyValuesB)

	// Fetch benchmark price series once (serially) before launching goroutines.
	// Fetching here prevents duplicate warnings and cache races if each goroutine fetched independently.
	const gspcTicker = "^GSPC"
	const diaTicker = "^DJI"
	var gspcPrices, diaPrices []models.PriceData
	for _, spec := range []struct {
		ticker string
		dest   *[]models.PriceData
	}{{gspcTicker, &gspcPrices}, {diaTicker, &diaPrices}} {
		sec, lookupErr := s.secRepo.GetByTicker(ctx, spec.ticker)
		if lookupErr != nil {
			AddWarning(ctx, models.Warning{
				Code:    models.WarnBenchmarkDataUnavailable,
				Message: fmt.Sprintf("benchmark %s not found in securities database; Alpha/Beta vs. this benchmark will be zero", spec.ticker),
			})
			continue
		}
		ps, fetchErr := s.performanceSvc.FetchBenchmarkPrices(ctx, sec.ID, req.StartPeriod.Time, req.EndPeriod.Time)
		if fetchErr != nil || len(ps) == 0 {
			AddWarning(ctx, models.Warning{
				Code:    models.WarnBenchmarkDataUnavailable,
				Message: fmt.Sprintf("no price data available for benchmark %s; Alpha/Beta vs. this benchmark will be zero", spec.ticker),
			})
			continue
		}
		*spec.dest = ps
	}

	var (
		sharpeA, sharpeB              models.SharpeRatios
		sortinoA, sortinoB            models.SortinoRatios
		dividendsA, dividendsB        float64
		alphaBetaAGSPC, alphaBetaADIA models.AlphaBeta
		alphaBetaBGSPC, alphaBetaBDIA models.AlphaBeta
		errSharpeA, errSharpeB        error
		errSortinoA, errSortinoB      error
		errDividendsA, errDividendsB  error
		errABaGSPC, errABaDIA         error
		errABbGSPC, errABbDIA         error
	)
	var wg sync.WaitGroup
	wg.Add(10)
	go func() {
		defer wg.Done()
		sharpeA, errSharpeA = s.performanceSvc.ComputeSharpe(ctx, dailyValuesA, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	go func() {
		defer wg.Done()
		sortinoA, errSortinoA = s.performanceSvc.ComputeSortino(ctx, dailyValuesA, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	go func() {
		defer wg.Done()
		dividendsA, errDividendsA = s.performanceSvc.ComputeDividends(ctx, pA, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	go func() {
		defer wg.Done()
		sharpeB, errSharpeB = s.performanceSvc.ComputeSharpe(ctx, dailyValuesB, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	go func() {
		defer wg.Done()
		sortinoB, errSortinoB = s.performanceSvc.ComputeSortino(ctx, dailyValuesB, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	go func() {
		defer wg.Done()
		dividendsB, errDividendsB = s.performanceSvc.ComputeDividends(ctx, pB, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	go func() {
		defer wg.Done()
		alphaBetaAGSPC, errABaGSPC = s.performanceSvc.ComputeAlphaBeta(ctx, dailyValuesA, gspcPrices, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	go func() {
		defer wg.Done()
		alphaBetaADIA, errABaDIA = s.performanceSvc.ComputeAlphaBeta(ctx, dailyValuesA, diaPrices, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	go func() {
		defer wg.Done()
		alphaBetaBGSPC, errABbGSPC = s.performanceSvc.ComputeAlphaBeta(ctx, dailyValuesB, gspcPrices, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	go func() {
		defer wg.Done()
		alphaBetaBDIA, errABbDIA = s.performanceSvc.ComputeAlphaBeta(ctx, dailyValuesB, diaPrices, req.StartPeriod.Time, req.EndPeriod.Time)
	}()
	wg.Wait()

	if errSharpeA != nil {
		return nil, fmt.Errorf("failed to compute Sharpe for portfolio A: %w", errSharpeA)
	}
	if errSortinoA != nil {
		return nil, fmt.Errorf("failed to compute Sortino for portfolio A: %w", errSortinoA)
	}
	if errDividendsA != nil {
		return nil, fmt.Errorf("failed to compute dividends for portfolio A: %w", errDividendsA)
	}
	if errSharpeB != nil {
		return nil, fmt.Errorf("failed to compute Sharpe for portfolio B: %w", errSharpeB)
	}
	if errSortinoB != nil {
		return nil, fmt.Errorf("failed to compute Sortino for portfolio B: %w", errSortinoB)
	}
	if errDividendsB != nil {
		return nil, fmt.Errorf("failed to compute dividends for portfolio B: %w", errDividendsB)
	}
	if errABaGSPC != nil {
		return nil, fmt.Errorf("failed to compute Alpha/Beta vs. %s for portfolio A: %w", gspcTicker, errABaGSPC)
	}
	if errABaDIA != nil {
		return nil, fmt.Errorf("failed to compute Alpha/Beta vs. %s for portfolio A: %w", diaTicker, errABaDIA)
	}
	if errABbGSPC != nil {
		return nil, fmt.Errorf("failed to compute Alpha/Beta vs. %s for portfolio B: %w", gspcTicker, errABbGSPC)
	}
	if errABbDIA != nil {
		return nil, fmt.Errorf("failed to compute Alpha/Beta vs. %s for portfolio B: %w", diaTicker, errABbDIA)
	}

	return &models.CompareResponse{
		PortfolioA: models.PortfolioSummary{
			ID:                  portfolioA.Portfolio.ID,
			Name:                portfolioA.Portfolio.Name,
			Type:                portfolioA.Portfolio.PortfolioType,
			DirectMembership:    directA,
			ExpandedMemberships: expandedA,
		},
		PortfolioB: models.PortfolioSummary{
			ID:                  portfolioB.Portfolio.ID,
			Name:                portfolioB.Portfolio.Name,
			Type:                portfolioB.Portfolio.PortfolioType,
			DirectMembership:    directB,
			ExpandedMemberships: expandedB,
		},

		AbsoluteSimilarityScore: similarityScore,

		PerformanceMetrics: models.PerformanceMetrics{
			PortfolioAMetrics: models.PortfolioPerformance{
				StartValue:    gainA.StartValue,
				EndValue:      gainA.EndValue,
				GainDollar:    gainA.GainDollar,
				GainPercent:   gainA.GainPercent,
				Dividends:     dividendsA,
				SharpeRatios:  sharpeA,
				SortinoRatios: sortinoA,
				BenchmarkMetrics: models.BenchmarkMetrics{
					SP500:    alphaBetaAGSPC,
					DowJones: alphaBetaADIA,
				},
				DailyValues: ToModelDailyValues(dailyValuesA),
			},
			PortfolioBMetrics: models.PortfolioPerformance{
				StartValue:    gainB.StartValue,
				EndValue:      gainB.EndValue,
				GainDollar:    gainB.GainDollar,
				GainPercent:   gainB.GainPercent,
				Dividends:     dividendsB,
				SharpeRatios:  sharpeB,
				SortinoRatios: sortinoB,
				BenchmarkMetrics: models.BenchmarkMetrics{
					SP500:    alphaBetaBGSPC,
					DowJones: alphaBetaBDIA,
				},
				DailyValues: ToModelDailyValues(dailyValuesB),
			},
		},
		Baskets: baskets,
	}, nil
}

// ComputeSimilarity calculates the overlap between two portfolios by summing
// the minimum allocation percentage for each security that exists in both.
func (s *ComparisonService) ComputeSimilarity(membershipA, membershipB []models.ExpandedMembership) float64 {
	// Create map from security ID to allocation for portfolio B
	mapB := make(map[int64]float64)
	for _, m := range membershipB {
		mapB[m.SecurityID] = m.Allocation
	}

	// Sum minimum allocations for matching securities
	var similarity float64
	for _, mA := range membershipA {
		if allocB, exists := mapB[mA.SecurityID]; exists {
			if mA.Allocation < allocB {
				similarity += mA.Allocation
			} else {
				similarity += allocB
			}
		}
	}

	// Clamp to 1.0 max to handle floating point rounding errors
	if similarity > 1.0 {
		similarity = 1.0
	}

	return similarity
}

// ComputeBaskets evaluates how well portfolio B fills each ideal basket from portfolio A
// at five coverage thresholds (20%, 40%, 60%, 80%, 100%). Only called when A is ideal.
//
// For each ETF in A, B can "redeem" constituent stocks it holds into the equivalent
// ETF position, provided the available constituent weight meets the threshold.
// Each threshold level uses a fresh copy of B's stock pool (no cross-level deduction).
// Within a threshold, stocks are consumed round-robin (first qualifying ETF claims them).
func (s *ComparisonService) ComputeBaskets(
	ctx context.Context,
	directA []models.ExpandedMembership,
	directB []models.ExpandedMembership,
	allSecurities map[int64]*models.Security,
	bEndValue float64,
	endPrices map[int64]float64,
) (*models.BasketResult, error) {
	// Build portfolio B allocation lookup
	bAlloc := make(map[int64]float64, len(directB))
	for _, m := range directB {
		bAlloc[m.SecurityID] = m.Allocation
	}

	// Identify ETFs in portfolio B by security type.
	bIsETF := make(map[int64]bool, len(directB))
	for _, m := range directB {
		sec := allSecurities[m.SecurityID]
		if sec == nil {
			return nil, fmt.Errorf("failed to lookup member holding: %d", m.SecurityID)
		}
		if sec.Type == string(models.SecurityTypeETF) || sec.Type == string(models.SecurityTypeMutualFund) {
			bIsETF[sec.ID] = true
		}
	}

	// Build set of A's direct security IDs to detect B's ETFs that are direct fills.
	directASecIDs := make(map[int64]bool, len(directA))
	for _, m := range directA {
		directASecIDs[m.SecurityID] = true
	}

	// expandedBPool: B's effective stock-level exposure.
	// - Direct stocks: included at their allocation.
	// - B ETFs that ARE in A (direct fill): excluded (captured by DirectFill, not pool).
	// - B ETFs that are NOT in A: expanded into constituent stocks, scaled by
	//   bAlloc[etfID] * (h.Percentage / totalETFWeight).
	expandedBPool := make(map[int64]float64, len(bAlloc))
	for _, m := range directB {
		if !bIsETF[m.SecurityID] {
			expandedBPool[m.SecurityID] += m.Allocation
			continue
		}
		if directASecIDs[m.SecurityID] {
			continue // direct fill ETF — not expanded
		}
		holdings, err := s.membershipSvc.GetCachedETFMembership(ctx, m.SecurityID)
		if err != nil {
			return nil, fmt.Errorf("failed to get ETF membership for B's ETF %d: %w", m.SecurityID, err)
		}
		if len(holdings) == 0 {
			continue
		}
		var totalWeight float64
		for _, h := range holdings {
			totalWeight += h.Percentage
		}
		if totalWeight == 0 {
			continue
		}
		for _, h := range holdings {
			expandedBPool[h.SecurityID] += m.Allocation * (h.Percentage / totalWeight)
		}
	}

	// Pre-compute ETF info for each ETF in portfolio A
	etfInfoMap := make(map[int64]*basketETFInfo, len(directA))

	for _, m := range directA {
		holdings, err := s.membershipSvc.GetCachedETFMembership(ctx, m.SecurityID)
		if err != nil {
			return nil, fmt.Errorf("failed to get ETF membership for security %d: %w", m.SecurityID, err)
		}
		if len(holdings) == 0 {
			continue // stock, not an ETF
		}

		var total float64
		for _, h := range holdings {
			total += h.Percentage
		}

		var available []models.ETFMembership
		var availW float64
		for _, h := range holdings {
			if expandedBPool[h.SecurityID] > 0 {
				available = append(available, h)
				availW += h.Percentage
			}
		}
		if total > 0 {
			availW = availW / total
		}

		etfInfoMap[m.SecurityID] = &basketETFInfo{
			availableConst:  available,
			availableWeight: availW,
			totalWeight:     total,
		}
	}

	// Evaluate each threshold independently
	type thresholdDef struct {
		val float64
	}
	thresholds := []thresholdDef{
		{0.20}, {0.40}, {0.60}, {0.80}, {1.00},
	}

	var result models.BasketResult
	for _, td := range thresholds {
		level := s.buildBasketLevel(td.val, bAlloc, expandedBPool, directA, allSecurities, etfInfoMap, bEndValue, endPrices)
		switch td.val {
		case 0.20:
			result.Basket20 = level
		case 0.40:
			result.Basket40 = level
		case 0.60:
			result.Basket60 = level
		case 0.80:
			result.Basket80 = level
		case 1.00:
			result.Basket100 = level
		}
	}

	return &result, nil
}

func (s *ComparisonService) buildBasketLevel(
	T float64,
	bAlloc map[int64]float64,
	expandedBPool map[int64]float64,
	directA []models.ExpandedMembership,
	allSecurities map[int64]*models.Security,
	etfInfoMap map[int64]*basketETFInfo,
	bEndValue float64,
	endPrices map[int64]float64,
) models.BasketLevel {
	// Fresh redemption pool per threshold, seeded from expandedBPool.
	pool := make(map[int64]float64, len(expandedBPool))
	for secID, alloc := range expandedBPool {
		pool[secID] = alloc
	}

	holdings := make([]models.BasketHolding, 0, len(directA))
	for _, m := range directA {
		sec := allSecurities[m.SecurityID]
		if sec == nil {
			continue
		}
		info := etfInfoMap[m.SecurityID]
		var h models.BasketHolding
		if info == nil {
			h = models.BasketHolding{
				SecurityID: m.SecurityID,
				Ticker:     sec.Ticker,
				IdealAlloc: m.Allocation,
				DirectFill: bAlloc[m.SecurityID],
			}
		} else {
			directFill := bAlloc[m.SecurityID]
			var redeemedFill float64
			if info.availableWeight >= T {
				for _, c := range info.availableConst {
					take := min(pool[c.SecurityID], c.Percentage/info.totalWeight)
					redeemedFill += take
					pool[c.SecurityID] -= take
				}
			}
			h = models.BasketHolding{
				SecurityID:     m.SecurityID,
				Ticker:         sec.Ticker,
				IdealAlloc:     m.Allocation,
				DirectFill:     directFill,
				RedeemedFill:   redeemedFill,
				CoverageWeight: info.availableWeight,
			}
		}
		gap := h.IdealAlloc - h.DirectFill - h.RedeemedFill
		dollars := gap * bEndValue
		var shares float64
		if price := endPrices[m.SecurityID]; price > 0 {
			shares = dollars / price
		}
		h.BuySell = models.BuySell{Dollars: dollars, Shares: shares}
		holdings = append(holdings, h)
	}

	var totalFill float64
	for _, h := range holdings {
		totalFill += h.DirectFill + h.RedeemedFill
	}
	return models.BasketLevel{
		Threshold: T,
		Holdings:  holdings,
		TotalFill: totalFill,
	}
}

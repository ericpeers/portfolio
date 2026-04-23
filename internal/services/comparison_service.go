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

	diffsA, diffsB, err := s.applyMissingDataStrategy(ctx, portfolioA, portfolioB, req)
	if err != nil {
		return nil, err
	}

	// Compute expanded and direct memberships for both portfolios in parallel.
	// allSecurities/allBySymbol are read-only; WarningCollector is mutex-protected.
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
		resA.expanded, resA.err = s.membershipSvc.ComputeMembership(ctx, portfolioA.Portfolio.ID, portfolioA.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities, allBySymbol, DiffsToMembershipOverlay(diffsA))
		if resA.err != nil {
			return
		}
		resA.direct, resA.err = s.membershipSvc.ComputeDirectMembership(ctx, portfolioA.Portfolio.ID, portfolioA.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities, DiffsToMembershipOverlay(diffsA))
	}()
	go func() {
		defer membershipWg.Done()
		resB.expanded, resB.err = s.membershipSvc.ComputeMembership(ctx, portfolioB.Portfolio.ID, portfolioB.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities, allBySymbol, DiffsToMembershipOverlay(diffsB))
		if resB.err != nil {
			return
		}
		resB.direct, resB.err = s.membershipSvc.ComputeDirectMembership(ctx, portfolioB.Portfolio.ID, portfolioB.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities, DiffsToMembershipOverlay(diffsB))
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

	dvr, err := s.computeDailyValuesForBoth(ctx, portfolioA, portfolioB, req, diffsA, diffsB, aIsIdeal, bIsIdeal)
	if err != nil {
		return nil, err
	}
	pA, pB := dvr.pA, dvr.pB
	dailyValuesA, dailyValuesB := dvr.dailyValuesA, dvr.dailyValuesB

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
	gspcPrices, diaPrices, err := s.fetchBenchmarkPrices(ctx, req.StartPeriod.Time, req.EndPeriod.Time)
	if err != nil {
		return nil, err
	}

	metrics, err := s.computeParallelMetrics(ctx, pA, pB, dailyValuesA, dailyValuesB, gspcPrices, diaPrices, req)
	if err != nil {
		return nil, err
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
				Dividends:     metrics.dividendsA,
				SharpeRatios:  metrics.sharpeA,
				SortinoRatios: metrics.sortinoA,
				BenchmarkMetrics: models.BenchmarkMetrics{
					SP500:    metrics.alphaBetaAGSPC,
					DowJones: metrics.alphaBetaADIA,
				},
				DailyValues: ToModelDailyValues(dailyValuesA),
			},
			PortfolioBMetrics: models.PortfolioPerformance{
				StartValue:    gainB.StartValue,
				EndValue:      gainB.EndValue,
				GainDollar:    gainB.GainDollar,
				GainPercent:   gainB.GainPercent,
				Dividends:     metrics.dividendsB,
				SharpeRatios:  metrics.sharpeB,
				SortinoRatios: metrics.sortinoB,
				BenchmarkMetrics: models.BenchmarkMetrics{
					SP500:    metrics.alphaBetaBGSPC,
					DowJones: metrics.alphaBetaBDIA,
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

// dailyValueResult carries the normalized portfolio pointers and daily value slices computed
// by computeDailyValuesForBoth for both portfolio A and B.
type dailyValueResult struct {
	pA, pB       *models.PortfolioWithMemberships
	dailyValuesA []DailyValue
	dailyValuesB []DailyValue
}

// computeDailyValuesForBoth computes daily portfolio values for both A and B, handling all
// combinations of Ideal/Active portfolio types. The sequencing rules are:
//   - both actual: run in parallel (independent)
//   - mixed (one actual, one ideal): compute the actual first to get its start value, which
//     seeds the ideal's normalization target; then compute the ideal sequentially
//   - both ideal: normalizeStart defaults to $100; run both normalize+compute in parallel
func (s *ComparisonService) computeDailyValuesForBoth(
	ctx context.Context,
	portfolioA, portfolioB *models.PortfolioWithMemberships,
	req *models.CompareRequest,
	diffsA, diffsB []PortfolioDiff,
	aIsIdeal, bIsIdeal bool,
) (dailyValueResult, error) {
	var res dailyValueResult
	res.pA, res.pB = portfolioA, portfolioB

	// Step 1: compute actual portfolios. Actual portfolios are independent of one another
	// so both-actual runs in parallel. A mixed pair runs the actual side sequentially here
	// so its start value is available to seed the ideal normalization in step 3.
	if !aIsIdeal && !bIsIdeal {
		var errA, errB error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			res.dailyValuesA, errA = s.performanceSvc.ComputeDailyValues(ctx, res.pA, req.StartPeriod.Time, req.EndPeriod.Time, diffsA, nil)
		}()
		go func() {
			defer wg.Done()
			res.dailyValuesB, errB = s.performanceSvc.ComputeDailyValues(ctx, res.pB, req.StartPeriod.Time, req.EndPeriod.Time, diffsB, nil)
		}()
		wg.Wait()
		if errA != nil {
			return res, fmt.Errorf("failed to compute daily values for portfolio A: %w", errA)
		}
		if len(res.dailyValuesA) == 0 {
			return res, fmt.Errorf("no daily values for portfolio A")
		}
		if errB != nil {
			return res, fmt.Errorf("failed to compute daily values for portfolio B: %w", errB)
		}
		if len(res.dailyValuesB) == 0 {
			return res, fmt.Errorf("no daily values for portfolio B")
		}
		return res, nil
	}

	var startValueA, startValueB float64
	if !aIsIdeal {
		// A is actual, B is ideal: compute A first for its start value.
		var err error
		res.dailyValuesA, err = s.performanceSvc.ComputeDailyValues(ctx, res.pA, req.StartPeriod.Time, req.EndPeriod.Time, diffsA, nil)
		if err != nil {
			return res, fmt.Errorf("failed to compute daily values for portfolio A: %w", err)
		}
		if len(res.dailyValuesA) == 0 {
			return res, fmt.Errorf("no daily values for portfolio A")
		}
		startValueA = res.dailyValuesA[0].Value
	} else if !bIsIdeal {
		// B is actual, A is ideal: compute B first for its start value.
		var err error
		res.dailyValuesB, err = s.performanceSvc.ComputeDailyValues(ctx, res.pB, req.StartPeriod.Time, req.EndPeriod.Time, diffsB, nil)
		if err != nil {
			return res, fmt.Errorf("failed to compute daily values for portfolio B: %w", err)
		}
		if len(res.dailyValuesB) == 0 {
			return res, fmt.Errorf("no daily values for portfolio B")
		}
		startValueB = res.dailyValuesB[0].Value
	}

	// Step 2: determine the normalization target for ideal portfolios.
	// Mixed: use the actual portfolio's start value. Both ideal: default to $100.
	idealStartValue := 100.0
	if !aIsIdeal && bIsIdeal {
		idealStartValue = startValueA
	} else if aIsIdeal && !bIsIdeal {
		idealStartValue = startValueB
	}

	// Step 3: normalize and compute daily values for ideal portfolios.
	// both ideal: neither depends on the other — run in parallel.
	// mixed: only one side is ideal; run it sequentially (the actual side is done above).
	if aIsIdeal && bIsIdeal {
		var errA, errB error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			var origA []models.PortfolioMembership
			var nerr error
			res.pA, origA, nerr = s.performanceSvc.NormalizeIdealPortfolio(ctx, portfolioA, req.StartPeriod.Time, idealStartValue, diffsA)
			if nerr != nil {
				errA = nerr
				return
			}
			res.dailyValuesA, errA = s.performanceSvc.ComputeDailyValues(ctx, res.pA, req.StartPeriod.Time, req.EndPeriod.Time, diffsA, origA)
		}()
		go func() {
			defer wg.Done()
			var origB []models.PortfolioMembership
			var nerr error
			res.pB, origB, nerr = s.performanceSvc.NormalizeIdealPortfolio(ctx, portfolioB, req.StartPeriod.Time, idealStartValue, diffsB)
			if nerr != nil {
				errB = nerr
				return
			}
			res.dailyValuesB, errB = s.performanceSvc.ComputeDailyValues(ctx, res.pB, req.StartPeriod.Time, req.EndPeriod.Time, diffsB, origB)
		}()
		wg.Wait()
		if errA != nil {
			return res, fmt.Errorf("failed to compute daily values for portfolio A: %w", errA)
		}
		if errB != nil {
			return res, fmt.Errorf("failed to compute daily values for portfolio B: %w", errB)
		}
	} else if aIsIdeal {
		var origA []models.PortfolioMembership
		var err error
		res.pA, origA, err = s.performanceSvc.NormalizeIdealPortfolio(ctx, portfolioA, req.StartPeriod.Time, idealStartValue, diffsA)
		if err != nil {
			return res, fmt.Errorf("failed to normalize portfolio A: %w", err)
		}
		res.dailyValuesA, err = s.performanceSvc.ComputeDailyValues(ctx, res.pA, req.StartPeriod.Time, req.EndPeriod.Time, diffsA, origA)
		if err != nil {
			return res, fmt.Errorf("failed to compute daily values for portfolio A: %w", err)
		}
	} else if bIsIdeal {
		var origB []models.PortfolioMembership
		var err error
		res.pB, origB, err = s.performanceSvc.NormalizeIdealPortfolio(ctx, portfolioB, req.StartPeriod.Time, idealStartValue, diffsB)
		if err != nil {
			return res, fmt.Errorf("failed to normalize portfolio B: %w", err)
		}
		res.dailyValuesB, err = s.performanceSvc.ComputeDailyValues(ctx, res.pB, req.StartPeriod.Time, req.EndPeriod.Time, diffsB, origB)
		if err != nil {
			return res, fmt.Errorf("failed to compute daily values for portfolio B: %w", err)
		}
	}

	return res, nil
}

// parallelMetrics holds all per-portfolio risk and benchmark metrics returned by computeParallelMetrics.
type parallelMetrics struct {
	sharpeA, sharpeB              models.SharpeRatios
	sortinoA, sortinoB            models.SortinoRatios
	dividendsA, dividendsB        float64
	alphaBetaAGSPC, alphaBetaADIA models.AlphaBeta
	alphaBetaBGSPC, alphaBetaBDIA models.AlphaBeta
}

// computeParallelMetrics runs Sharpe, Sortino, Dividends, and Alpha/Beta (vs. ^GSPC and ^DJI)
// for both portfolios concurrently (10 goroutines, one WaitGroup). Returns on the first error.
func (s *ComparisonService) computeParallelMetrics(
	ctx context.Context,
	pA, pB *models.PortfolioWithMemberships,
	dailyValuesA, dailyValuesB []DailyValue,
	gspcPrices, diaPrices []models.PriceData,
	req *models.CompareRequest,
) (parallelMetrics, error) {
	var m parallelMetrics
	var (
		errSharpeA, errSharpeB       error
		errSortinoA, errSortinoB     error
		errDividendsA, errDividendsB error
		errABaGSPC, errABaDIA        error
		errABbGSPC, errABbDIA        error
	)
	var wg sync.WaitGroup
	wg.Add(10)
	go func() { defer wg.Done(); m.sharpeA, errSharpeA = s.performanceSvc.ComputeSharpe(ctx, dailyValuesA, req.StartPeriod.Time, req.EndPeriod.Time) }()
	go func() { defer wg.Done(); m.sortinoA, errSortinoA = s.performanceSvc.ComputeSortino(ctx, dailyValuesA, req.StartPeriod.Time, req.EndPeriod.Time) }()
	go func() { defer wg.Done(); m.dividendsA, errDividendsA = s.performanceSvc.ComputeDividends(ctx, pA, req.StartPeriod.Time, req.EndPeriod.Time) }()
	go func() { defer wg.Done(); m.sharpeB, errSharpeB = s.performanceSvc.ComputeSharpe(ctx, dailyValuesB, req.StartPeriod.Time, req.EndPeriod.Time) }()
	go func() { defer wg.Done(); m.sortinoB, errSortinoB = s.performanceSvc.ComputeSortino(ctx, dailyValuesB, req.StartPeriod.Time, req.EndPeriod.Time) }()
	go func() { defer wg.Done(); m.dividendsB, errDividendsB = s.performanceSvc.ComputeDividends(ctx, pB, req.StartPeriod.Time, req.EndPeriod.Time) }()
	go func() { defer wg.Done(); m.alphaBetaAGSPC, errABaGSPC = s.performanceSvc.ComputeAlphaBeta(ctx, dailyValuesA, gspcPrices, req.StartPeriod.Time, req.EndPeriod.Time) }()
	go func() { defer wg.Done(); m.alphaBetaADIA, errABaDIA = s.performanceSvc.ComputeAlphaBeta(ctx, dailyValuesA, diaPrices, req.StartPeriod.Time, req.EndPeriod.Time) }()
	go func() { defer wg.Done(); m.alphaBetaBGSPC, errABbGSPC = s.performanceSvc.ComputeAlphaBeta(ctx, dailyValuesB, gspcPrices, req.StartPeriod.Time, req.EndPeriod.Time) }()
	go func() { defer wg.Done(); m.alphaBetaBDIA, errABbDIA = s.performanceSvc.ComputeAlphaBeta(ctx, dailyValuesB, diaPrices, req.StartPeriod.Time, req.EndPeriod.Time) }()
	wg.Wait()

	switch {
	case errSharpeA != nil:
		return m, fmt.Errorf("failed to compute Sharpe for portfolio A: %w", errSharpeA)
	case errSortinoA != nil:
		return m, fmt.Errorf("failed to compute Sortino for portfolio A: %w", errSortinoA)
	case errDividendsA != nil:
		return m, fmt.Errorf("failed to compute dividends for portfolio A: %w", errDividendsA)
	case errSharpeB != nil:
		return m, fmt.Errorf("failed to compute Sharpe for portfolio B: %w", errSharpeB)
	case errSortinoB != nil:
		return m, fmt.Errorf("failed to compute Sortino for portfolio B: %w", errSortinoB)
	case errDividendsB != nil:
		return m, fmt.Errorf("failed to compute dividends for portfolio B: %w", errDividendsB)
	case errABaGSPC != nil:
		return m, fmt.Errorf("failed to compute Alpha/Beta vs. ^GSPC for portfolio A: %w", errABaGSPC)
	case errABaDIA != nil:
		return m, fmt.Errorf("failed to compute Alpha/Beta vs. ^DJI for portfolio A: %w", errABaDIA)
	case errABbGSPC != nil:
		return m, fmt.Errorf("failed to compute Alpha/Beta vs. ^GSPC for portfolio B: %w", errABbGSPC)
	case errABbDIA != nil:
		return m, fmt.Errorf("failed to compute Alpha/Beta vs. ^DJI for portfolio B: %w", errABbDIA)
	}
	return m, nil
}

// fetchBenchmarkPrices fetches the ^GSPC and ^DJI price series for the given date range.
// Missing or unavailable benchmarks emit WarnBenchmarkDataUnavailable and return a nil slice
// (Alpha/Beta against that benchmark will be zero).
func (s *ComparisonService) fetchBenchmarkPrices(
	ctx context.Context,
	startDate, endDate time.Time,
) (gspcPrices []models.PriceData, diaPrices []models.PriceData, err error) {
	const gspcTicker = "^GSPC"
	const diaTicker = "^DJI"
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
		ps, fetchErr := s.performanceSvc.FetchBenchmarkPrices(ctx, sec.ID, startDate, endDate)
		if fetchErr != nil || len(ps) == 0 {
			AddWarning(ctx, models.Warning{
				Code:    models.WarnBenchmarkDataUnavailable,
				Message: fmt.Sprintf("no price data available for benchmark %s; Alpha/Beta vs. this benchmark will be zero", spec.ticker),
			})
			continue
		}
		*spec.dest = ps
	}
	return gspcPrices, diaPrices, nil
}

// applyMissingDataStrategy inspects pre-IPO coverage for both portfolios and produces
// PortfolioDiff slices based on the requested strategy. For the constrain-date-range
// default, it mutates req.StartPeriod.Time if coverage forces a later start date.
func (s *ComparisonService) applyMissingDataStrategy(
	ctx context.Context,
	portfolioA, portfolioB *models.PortfolioWithMemberships,
	req *models.CompareRequest,
) (diffsA []PortfolioDiff, diffsB []PortfolioDiff, err error) {
	coverageA, err := s.performanceSvc.ComputeDataCoverage(ctx, portfolioA, req.StartPeriod.Time)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compute data coverage for portfolio A: %w", err)
	}
	coverageB, err := s.performanceSvc.ComputeDataCoverage(ctx, portfolioB, req.StartPeriod.Time)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compute data coverage for portfolio B: %w", err)
	}

	switch req.MissingDataStrategy {
	case models.MissingDataStrategyCashFlat, models.MissingDataStrategyCashAppreciating:
		if coverageA.AnyGaps {
			diffsA, err = s.performanceSvc.SynthesizeCashPrices(ctx, coverageA, req.MissingDataStrategy)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to synthesize cash prices for portfolio A: %w", err)
			}
		}
		if coverageB.AnyGaps {
			diffsB, err = s.performanceSvc.SynthesizeCashPrices(ctx, coverageB, req.MissingDataStrategy)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to synthesize cash prices for portfolio B: %w", err)
			}
		}
		if coverageA.AnyGaps || coverageB.AnyGaps {
			AddWarning(ctx, models.Warning{
				Code:    models.WarnCashSubstituted,
				Message: CashSubstitutionWarningMessage(coverageA, coverageB),
			})
		}
	case models.MissingDataStrategyReallocate:
		if coverageA.AnyGaps {
			diffsA = GenerateReallocateDiffs(coverageA)
		}
		if coverageB.AnyGaps {
			diffsB = GenerateReallocateDiffs(coverageB)
		}
		if coverageA.AnyGaps || coverageB.AnyGaps {
			AddWarning(ctx, models.Warning{
				Code:    models.WarnProportionalReallocation,
				Message: ReallocWarningMessage(coverageA, coverageB),
			})
		}
	default: // MissingDataStrategyConstrainDateRange
		constrainedStart := coverageA.ConstrainedStart
		if coverageB.ConstrainedStart.After(constrainedStart) {
			constrainedStart = coverageB.ConstrainedStart
		}
		if constrainedStart.After(req.StartPeriod.Time) {
			log.Warnf("ComparePortfolios: data coverage constrains start date from %s to %s",
				req.StartPeriod.Time.Format("2006-01-02"), constrainedStart.Format("2006-01-02"))
			if constrainedStart.After(req.EndPeriod.Time) {
				return nil, nil, fmt.Errorf("%w: constrained start %s is after requested end %s — one or more securities have not yet IPO'd within the requested window. Try using a substitution strategy or change your date range",
					ErrInvalidDateRange, constrainedStart.Format("2006-01-02"), req.EndPeriod.Time.Format("2006-01-02"))
			}
			req.StartPeriod.Time = constrainedStart
			AddWarning(ctx, models.Warning{
				Code:    models.WarnStartDateAdjusted,
				Message: fmt.Sprintf("The start date was adjusted to %s to reflect the inception date of one or more securities in the comparison.", constrainedStart.Format("2006-01-02")),
			})
		}
	}
	return diffsA, diffsB, nil
}

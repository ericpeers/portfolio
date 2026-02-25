package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/models"
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
}

// NewComparisonService creates a new ComparisonService
func NewComparisonService(
	portfolioSvc *PortfolioService,
	membershipSvc *MembershipService,
	performanceSvc *PerformanceService,
) *ComparisonService {
	return &ComparisonService{
		portfolioSvc:   portfolioSvc,
		membershipSvc:  membershipSvc,
		performanceSvc: performanceSvc,
	}
}

// ComparePortfolios performs a full comparison between two portfolios
// Comparison supports: [actual,actual], [actual,ideal], [ideal,actual], [ideal,ideal]
func (s *ComparisonService) ComparePortfolios(ctx context.Context, req *models.CompareRequest) (*models.CompareResponse, error) {
	defer TrackTime("ComparePortfolios", time.Now())
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
	allSecurities, allBySymbol, err := s.membershipSvc.GetAllSecurities(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to pre-fetch securities: %w", err)
	}

	// Compute latest inception date from portfolio members only (not all securities)
	var latestInception *time.Time
	for _, m := range portfolioA.Memberships {
		if sec := allSecurities[m.SecurityID]; sec != nil && sec.Inception != nil {
			if latestInception == nil || sec.Inception.After(*latestInception) {
				latestInception = sec.Inception
			}
		}
	}
	for _, m := range portfolioB.Memberships {
		if sec := allSecurities[m.SecurityID]; sec != nil && sec.Inception != nil {
			if latestInception == nil || sec.Inception.After(*latestInception) {
				latestInception = sec.Inception
			}
		}
	}
	if latestInception != nil && req.StartPeriod.Time.Before(*latestInception) {
		req.StartPeriod.Time = *latestInception
		AddWarning(ctx, models.Warning{
			Code:    models.WarnStartDateAdjusted,
			Message: fmt.Sprintf("The start date was adjusted to %s to reflect the inception date of one or more securities in the comparison.", latestInception.Format("2006-01-02")),
		})
	}

	// Compute expanded memberships for both portfolios
	expandedA, err := s.membershipSvc.ComputeMembership(ctx, portfolioA.Portfolio.ID, portfolioA.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities, allBySymbol)
	if err != nil {
		return nil, fmt.Errorf("failed to compute membership for portfolio A: %w", err)
	}

	expandedB, err := s.membershipSvc.ComputeMembership(ctx, portfolioB.Portfolio.ID, portfolioB.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities, allBySymbol)
	if err != nil {
		return nil, fmt.Errorf("failed to compute membership for portfolio B: %w", err)
	}

	// Compute direct (unexpanded) memberships
	directA, err := s.membershipSvc.ComputeDirectMembership(ctx, portfolioA.Portfolio.ID, portfolioA.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities)
	if err != nil {
		return nil, fmt.Errorf("failed to compute direct membership for portfolio A: %w", err)
	}

	directB, err := s.membershipSvc.ComputeDirectMembership(ctx, portfolioB.Portfolio.ID, portfolioB.Portfolio.PortfolioType, req.StartPeriod.Time, req.EndPeriod.Time, allSecurities)
	if err != nil {
		return nil, fmt.Errorf("failed to compute direct membership for portfolio B: %w", err)
	}

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

	// Process actual portfolios first to get their start values
	if !aIsIdeal {
		pA = portfolioA // Use original pointer for actual portfolios
		dailyValuesA, err = s.performanceSvc.ComputeDailyValues(ctx, pA, req.StartPeriod.Time, req.EndPeriod.Time)
		if err != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio A: %w", err)
		}
		if len(dailyValuesA) == 0 {
			return nil, fmt.Errorf("no daily values for portfolio A")
		}
		startValueA = dailyValuesA[0].Value
	}
	if !bIsIdeal {
		pB = portfolioB // Use original pointer for actual portfolios
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

	// Process ideal portfolios with the determined start value
	if aIsIdeal {
		pA, err = s.performanceSvc.NormalizeIdealPortfolio(ctx, portfolioA, req.StartPeriod.Time, idealStartValue)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize portfolio A: %w", err)
		}
		dailyValuesA, err = s.performanceSvc.ComputeDailyValues(ctx, pA, req.StartPeriod.Time, req.EndPeriod.Time)
		if err != nil {
			return nil, fmt.Errorf("failed to compute daily values for portfolio A: %w", err)
		}
	}
	if bIsIdeal {
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

	// Compute performance metrics for portfolio A
	gainA := ComputeGain(dailyValuesA)

	sharpeA, err := s.performanceSvc.ComputeSharpe(ctx, dailyValuesA, req.StartPeriod.Time, req.EndPeriod.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to compute Sharpe for portfolio A: %w", err)
	}

	dividendsA, err := s.performanceSvc.ComputeDividends(ctx, pA, req.StartPeriod.Time, req.EndPeriod.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to compute dividends for portfolio A: %w", err)
	}

	// Compute performance metrics for portfolio B
	gainB := ComputeGain(dailyValuesB)

	sharpeB, err := s.performanceSvc.ComputeSharpe(ctx, dailyValuesB, req.StartPeriod.Time, req.EndPeriod.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to compute Sharpe for portfolio B: %w", err)
	}

	dividendsB, err := s.performanceSvc.ComputeDividends(ctx, pB, req.StartPeriod.Time, req.EndPeriod.Time)
	if err != nil {
		return nil, fmt.Errorf("failed to compute dividends for portfolio B: %w", err)
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
				StartValue:   gainA.StartValue,
				EndValue:     gainA.EndValue,
				GainDollar:   gainA.GainDollar,
				GainPercent:  gainA.GainPercent,
				Dividends:    dividendsA,
				SharpeRatios: *sharpeA,
				DailyValues:  ToModelDailyValues(dailyValuesA),
			},
			PortfolioBMetrics: models.PortfolioPerformance{
				StartValue:   gainB.StartValue,
				EndValue:     gainB.EndValue,
				GainDollar:   gainB.GainDollar,
				GainPercent:  gainB.GainPercent,
				Dividends:    dividendsB,
				SharpeRatios: *sharpeB,
				DailyValues:  ToModelDailyValues(dailyValuesB),
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

// ComparePortfoliosAtDate compares portfolios at a specific point in time
func (s *ComparisonService) ComparePortfoliosAtDate(ctx context.Context, portfolioAID, portfolioBID int64, date time.Time) (*models.CompareResponse, error) {
	// Create a comparison request with same start and end date for point-in-time comparison
	req := &models.CompareRequest{
		PortfolioA:  portfolioAID,
		PortfolioB:  portfolioBID,
		StartPeriod: models.FlexibleDate{Time: date.AddDate(0, 0, -1)}, // Day before
		EndPeriod:   models.FlexibleDate{Time: date},
	}
	return s.ComparePortfolios(ctx, req)
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
			return nil, fmt.Errorf("Failed to lookup member holding: %d", m.SecurityID)
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
				Symbol:     sec.Symbol,
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
				Symbol:         sec.Symbol,
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

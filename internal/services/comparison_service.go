package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/models"
)

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

	// Compute daily values and normalize portfolios
	// For actual portfolios: use original pointer, get startValue from dailyValues[0]
	// For ideal portfolios: normalize to actual's start value (or $100 if both ideal)
	aIsIdeal := portfolioA.Portfolio.PortfolioType == models.PortfolioTypeIdeal
	bIsIdeal := portfolioB.Portfolio.PortfolioType == models.PortfolioTypeIdeal

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

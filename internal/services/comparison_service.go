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
	// Get both portfolios
	portfolioA, err := s.portfolioSvc.GetPortfolio(ctx, req.PortfolioA)
	if err != nil {
		return nil, fmt.Errorf("failed to get portfolio A: %w", err)
	}

	portfolioB, err := s.portfolioSvc.GetPortfolio(ctx, req.PortfolioB)
	if err != nil {
		return nil, fmt.Errorf("failed to get portfolio B: %w", err)
	}

	// Compute expanded memberships for both portfolios
	expandedA, err := s.membershipSvc.ComputeMembership(ctx, portfolioA.Portfolio.ID, portfolioA.Portfolio.PortfolioType, req.EndPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to compute membership for portfolio A: %w", err)
	}

	expandedB, err := s.membershipSvc.ComputeMembership(ctx, portfolioB.Portfolio.ID, portfolioB.Portfolio.PortfolioType, req.EndPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to compute membership for portfolio B: %w", err)
	}

	// Compute membership diff
	membershipDiff := s.membershipSvc.DiffMembership(expandedA, expandedB)

	// Normalize portfolios for performance comparison
	normA, normB, err := s.performanceSvc.NormalizePortfolios(ctx, portfolioA, portfolioB, req.StartPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize portfolios: %w", err)
	}

	// Compute performance metrics for portfolio A
	gainA, err := s.performanceSvc.ComputeGain(ctx, normA, req.EndPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to compute gain for portfolio A: %w", err)
	}

	sharpeA, err := s.performanceSvc.ComputeSharpe(ctx, normA, req.StartPeriod, req.EndPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to compute Sharpe for portfolio A: %w", err)
	}

	dividendsA, err := s.performanceSvc.ComputeDividends(ctx, normA, req.StartPeriod, req.EndPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to compute dividends for portfolio A: %w", err)
	}

	// Compute performance metrics for portfolio B
	gainB, err := s.performanceSvc.ComputeGain(ctx, normB, req.EndPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to compute gain for portfolio B: %w", err)
	}

	sharpeB, err := s.performanceSvc.ComputeSharpe(ctx, normB, req.StartPeriod, req.EndPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to compute Sharpe for portfolio B: %w", err)
	}

	dividendsB, err := s.performanceSvc.ComputeDividends(ctx, normB, req.StartPeriod, req.EndPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to compute dividends for portfolio B: %w", err)
	}

	return &models.CompareResponse{
		PortfolioA: models.PortfolioSummary{
			ID:                  portfolioA.Portfolio.ID,
			Name:                portfolioA.Portfolio.Name,
			Type:                portfolioA.Portfolio.PortfolioType,
			ExpandedMemberships: expandedA,
		},
		PortfolioB: models.PortfolioSummary{
			ID:                  portfolioB.Portfolio.ID,
			Name:                portfolioB.Portfolio.Name,
			Type:                portfolioB.Portfolio.PortfolioType,
			ExpandedMemberships: expandedB,
		},
		MembershipDiff: membershipDiff,
		PerformanceMetrics: models.PerformanceMetrics{
			PortfolioAMetrics: models.PortfolioPerformance{
				StartValue:   gainA.StartValue,
				EndValue:     gainA.EndValue,
				GainDollar:   gainA.GainDollar,
				GainPercent:  gainA.GainPercent,
				Dividends:    dividendsA,
				SharpeRatios: *sharpeA,
			},
			PortfolioBMetrics: models.PortfolioPerformance{
				StartValue:   gainB.StartValue,
				EndValue:     gainB.EndValue,
				GainDollar:   gainB.GainDollar,
				GainPercent:  gainB.GainPercent,
				Dividends:    dividendsB,
				SharpeRatios: *sharpeB,
			},
		},
	}, nil
}

// ComparePortfoliosAtDate compares portfolios at a specific point in time
func (s *ComparisonService) ComparePortfoliosAtDate(ctx context.Context, portfolioAID, portfolioBID int64, date time.Time) (*models.CompareResponse, error) {
	// Create a comparison request with same start and end date for point-in-time comparison
	req := &models.CompareRequest{
		PortfolioA:  portfolioAID,
		PortfolioB:  portfolioBID,
		StartPeriod: date.AddDate(0, 0, -1), // Day before
		EndPeriod:   date,
	}
	return s.ComparePortfolios(ctx, req)
}

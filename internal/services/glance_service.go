package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// GlanceService manages the portfolio glance (home page pinned portfolios) feature.
type GlanceService struct {
	glanceRepo     *repository.GlanceRepository
	portfolioSvc   *PortfolioService
	performanceSvc *PerformanceService
}

// NewGlanceService creates a new GlanceService.
func NewGlanceService(
	glanceRepo *repository.GlanceRepository,
	portfolioSvc *PortfolioService,
	performanceSvc *PerformanceService,
) *GlanceService {
	return &GlanceService{
		glanceRepo:     glanceRepo,
		portfolioSvc:   portfolioSvc,
		performanceSvc: performanceSvc,
	}
}

// Add pins a portfolio to the user's glance list.
// Returns (true, nil) if newly added, (false, nil) if already pinned.
func (s *GlanceService) Add(ctx context.Context, userID, portfolioID int64) (bool, error) {
	if _, err := s.portfolioSvc.GetPortfolio(ctx, portfolioID); err != nil {
		return false, err
	}
	return s.glanceRepo.Add(ctx, userID, portfolioID)
}

// Remove unpins a portfolio from the user's glance list.
func (s *GlanceService) Remove(ctx context.Context, userID, portfolioID int64) error {
	return s.glanceRepo.Remove(ctx, userID, portfolioID)
}

// List returns all pinned portfolios for a user with their current key metrics.
// Errors for individual portfolios are surfaced as per-item warnings rather than
// aborting the whole list.
func (s *GlanceService) List(ctx context.Context, userID int64) ([]models.GlancePortfolio, error) {
	ids, err := s.glanceRepo.ListPortfolioIDs(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to list glance portfolio IDs: %w", err)
	}

	result := make([]models.GlancePortfolio, 0, len(ids))
	endDate := PreviousMarketDay(time.Now())

	for _, portfolioID := range ids {
		item, err := s.computeGlancePortfolio(ctx, portfolioID, endDate)
		if err != nil {
			log.Warnf("GlanceService.List: failed to compute metrics for portfolio %d: %v", portfolioID, err)
			result = append(result, models.GlancePortfolio{
				PortfolioID: portfolioID,
				Warnings: []models.Warning{{
					Code:    models.WarnMissingPriceHistory,
					Message: fmt.Sprintf("failed to compute metrics: %v", err),
				}},
			})
			continue
		}
		result = append(result, *item)
	}

	return result, nil
}

// computeGlancePortfolio computes all display metrics for a single pinned portfolio.
func (s *GlanceService) computeGlancePortfolio(ctx context.Context, portfolioID int64, endDate time.Time) (*models.GlancePortfolio, error) {
	portfolio, err := s.portfolioSvc.GetPortfolio(ctx, portfolioID)
	if err != nil {
		return nil, fmt.Errorf("failed to get portfolio: %w", err)
	}

	// Truncate creation date to midnight UTC as the start of the life window.
	startDate := time.Date(
		portfolio.Portfolio.CreatedAt.Year(),
		portfolio.Portfolio.CreatedAt.Month(),
		portfolio.Portfolio.CreatedAt.Day(),
		0, 0, 0, 0, time.UTC,
	)

	// Create a per-portfolio warning context so ComputeDailyValues warnings are captured.
	warnCtx, wc := NewWarningContext(ctx)

	// Normalize ideal portfolios to a $10,000 basis before computing daily values.
	const idealStartValue = 10_000.0
	if portfolio.Portfolio.PortfolioType == models.PortfolioTypeIdeal {
		portfolio, err = s.performanceSvc.NormalizeIdealPortfolio(warnCtx, portfolio, startDate, idealStartValue)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize ideal portfolio: %w", err)
		}
	}

	dailyValues, err := s.performanceSvc.ComputeDailyValues(warnCtx, portfolio, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to compute daily values: %w", err)
	}

	var currentValue float64
	var valuationDate string
	if len(dailyValues) > 0 {
		last := dailyValues[len(dailyValues)-1]
		currentValue = last.Value
		valuationDate = last.Date.Format("2006-01-02")
	}

	lifeValues := dailyValues
	yearValues := filterFrom(dailyValues, endDate.AddDate(-1, 0, 0))
	monthValues := filterFrom(dailyValues, endDate.AddDate(0, -1, 0))
	dayValues := lastNDailyValues(dailyValues, 2)

	return &models.GlancePortfolio{
		PortfolioID:           portfolioID,
		Name:                  portfolio.Portfolio.Name,
		CurrentValue:          currentValue,
		ValuationDate:         valuationDate,
		DailyReturn:           toReturnMetric(ComputeGain(dayValues), dayValues),
		OneMonthReturn:        toReturnMetric(ComputeGain(monthValues), monthValues),
		OneYearReturn:         toReturnMetric(ComputeGain(yearValues), yearValues),
		LifeOfPortfolioReturn: toReturnMetric(ComputeGain(lifeValues), lifeValues),
		Warnings:              wc.GetWarnings(),
	}, nil
}

// filterFrom returns the subslice of values where Date >= cutoff.
// If no values meet the cutoff, returns the full slice (all available history).
func filterFrom(values []DailyValue, cutoff time.Time) []DailyValue {
	cutoff = time.Date(cutoff.Year(), cutoff.Month(), cutoff.Day(), 0, 0, 0, 0, time.UTC)
	for i, v := range values {
		if !v.Date.Before(cutoff) {
			return values[i:]
		}
	}
	return values
}

// lastNDailyValues returns the last n elements, or all elements if len(values) <= n.
func lastNDailyValues(values []DailyValue, n int) []DailyValue {
	if len(values) <= n {
		return values
	}
	return values[len(values)-n:]
}

// toReturnMetric converts a GainResult to a ReturnMetric, populating StartDate
// from the first value in the slice.
func toReturnMetric(g GainResult, values []DailyValue) models.ReturnMetric {
	m := models.ReturnMetric{
		Dollar:     g.GainDollar,
		Percentage: g.GainPercent,
	}
	if len(values) > 0 {
		m.StartDate = values[0].Date.Format("2006-01-02")
	}
	return m
}

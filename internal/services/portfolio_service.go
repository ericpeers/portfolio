package services

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

var (
	ErrPortfolioNotFound      = errors.New("portfolio not found")
	ErrConflict               = errors.New("portfolio with same name and type already exists")
	ErrUnauthorized           = errors.New("not authorized to modify this portfolio")
	ErrInvalidMembership      = errors.New("invalid membership")
	ErrInvalidIdealPercentage = errors.New("ideal portfolio percentages must be in decimal form (0 < value <= 1.0)")
	ErrIdealTotalExceedsOne   = errors.New("ideal portfolio total allocation exceeds 100%")
	ErrInvalidObjective       = errors.New("invalid objective")

	ValidObjectives = map[models.Objective]struct{}{
		models.ObjectiveAggressiveGrowth:    {},
		models.ObjectiveGrowth:              {},
		models.ObjectiveIncomeGeneration:    {},
		models.ObjectiveCapitalPreservation: {},
		models.ObjectiveMixedGrowthIncome:   {},
	}
)

// PortfolioService handles portfolio business logic
type PortfolioService struct {
	portfolioRepo *repository.PortfolioRepository
	securityRepo  *repository.SecurityRepository
}

// NewPortfolioService creates a new PortfolioService
func NewPortfolioService(portfolioRepo *repository.PortfolioRepository, securityRepo *repository.SecurityRepository) *PortfolioService {
	return &PortfolioService{
		portfolioRepo: portfolioRepo,
		securityRepo:  securityRepo,
	}
}

// ResolveMembershipTickers validates membership entries and resolves tickers to security IDs.
// Each membership must have exactly one of SecurityID or Ticker set.
// Ticker-based entries are resolved in a single bulk lookup.
func (s *PortfolioService) ResolveMembershipTickers(ctx context.Context, memberships []models.MembershipRequest) error {
	var tickersToResolve []string
	var validationErrors []string

	// Pass 1: validate and collect tickers
	for i, m := range memberships {
		hasSID := m.SecurityID != 0
		hasTicker := m.Ticker != ""
		if hasSID && hasTicker {
			validationErrors = append(validationErrors, fmt.Sprintf("membership[%d]: cannot specify both security_id and ticker", i))
		} else if !hasSID && !hasTicker {
			validationErrors = append(validationErrors, fmt.Sprintf("membership[%d]: must specify either security_id or ticker", i))
		} else if hasTicker {
			tickersToResolve = append(tickersToResolve, m.Ticker)
		}
	}
	if len(validationErrors) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalidMembership, strings.Join(validationErrors, "; "))
	}

	if len(tickersToResolve) == 0 {
		return nil
	}

	// Pass 2: bulk resolve tickers
	// Deduplicate for the query
	uniqueTickers := make(map[string]struct{})
	for _, t := range tickersToResolve {
		uniqueTickers[t] = struct{}{}
	}
	deduped := make([]string, 0, len(uniqueTickers))
	for t := range uniqueTickers {
		deduped = append(deduped, t)
	}

	resolved, err := s.securityRepo.GetMultipleBySymbols(ctx, deduped)
	if err != nil {
		return fmt.Errorf("failed to resolve tickers: %w", err)
	}

	// Check for unresolvable tickers (must have at least one US listing)
	var notFound []string
	for t := range uniqueTickers {
		usOnly := repository.OnlyUSListings(resolved[t])
		if len(usOnly) == 0 {
			notFound = append(notFound, t)
		}
	}
	if len(notFound) > 0 {
		return fmt.Errorf("%w: unknown tickers: %s", ErrInvalidMembership, strings.Join(notFound, ", "))
	}

	// Pass 3: write resolved IDs back, picking the first US listing
	for i := range memberships {
		if memberships[i].Ticker != "" {
			usOnly := repository.OnlyUSListings(resolved[memberships[i].Ticker])
			memberships[i].SecurityID = usOnly[0].ID
		}
	}

	return nil
}

// CreatePortfolio creates a new portfolio with memberships
func (s *PortfolioService) CreatePortfolio(ctx context.Context, req *models.CreatePortfolioRequest) (*models.PortfolioWithMemberships, error) {
	// Resolve any ticker-based memberships to security IDs
	if len(req.Memberships) > 0 {
		if err := s.ResolveMembershipTickers(ctx, req.Memberships); err != nil {
			return nil, err
		}
	}

	// Validate ideal portfolio percentages are in decimal form
	if req.PortfolioType == models.PortfolioTypeIdeal {
		if err := validateIdealMemberships(req.Memberships); err != nil {
			return nil, err
		}
	}

	// Check for conflict - same name and type for the same user
	existing, err := s.portfolioRepo.GetByNameAndType(ctx, req.OwnerID, req.Name, req.PortfolioType)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing portfolio: %w", err)
	}
	if existing != nil {
		return nil, ErrConflict
	}

	// Start transaction
	tx, err := s.portfolioRepo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create portfolio
	portfolio := &models.Portfolio{
		PortfolioType: req.PortfolioType,
		Objective:     req.Objective,
		Name:          req.Name,
		OwnerID:       req.OwnerID,
	}
	if err := s.portfolioRepo.Create(ctx, tx, portfolio); err != nil {
		return nil, fmt.Errorf("failed to create portfolio: %w", err)
	}

	// Create memberships
	memberships, err := s.portfolioRepo.CreateMemberships(ctx, tx, portfolio.ID, req.Memberships)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberships: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &models.PortfolioWithMemberships{
		Portfolio:   *portfolio,
		Memberships: memberships,
	}, nil
}

// GetPortfolio retrieves a portfolio with its memberships
func (s *PortfolioService) GetPortfolio(ctx context.Context, id int64) (*models.PortfolioWithMemberships, error) {
	defer TrackTime("GetPortfolio", time.Now())

	portfolio, err := s.portfolioRepo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrPortfolioNotFound) {
			return nil, ErrPortfolioNotFound
		}
		return nil, fmt.Errorf("failed to get portfolio: %w", err)
	}

	memberships, err := s.portfolioRepo.GetMemberships(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get memberships: %w", err)
	}

	return &models.PortfolioWithMemberships{
		Portfolio:   *portfolio,
		Memberships: memberships,
	}, nil
}

// UpdatePortfolio updates a portfolio and replaces its memberships
func (s *PortfolioService) UpdatePortfolio(ctx context.Context, id int64, userID int64, req *models.UpdatePortfolioRequest) (*models.PortfolioWithMemberships, error) {
	// Get existing portfolio
	portfolio, err := s.portfolioRepo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrPortfolioNotFound) {
			return nil, ErrPortfolioNotFound
		}
		return nil, fmt.Errorf("failed to get portfolio: %w", err)
	}

	// Validate owner
	if portfolio.OwnerID != userID {
		return nil, ErrUnauthorized
	}

	// Start transaction
	tx, err := s.portfolioRepo.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update portfolio metadata if provided
	needsUpdate := false
	if req.PortfolioType != nil {
		portfolio.PortfolioType = *req.PortfolioType
		needsUpdate = true
	}
	if req.Name != "" {
		portfolio.Name = req.Name
		needsUpdate = true
	}
	if req.Objective != nil {
		portfolio.Objective = *req.Objective
		needsUpdate = true
	}
	if needsUpdate {
		if err := s.portfolioRepo.Update(ctx, tx, portfolio); err != nil {
			return nil, fmt.Errorf("failed to update portfolio: %w", err)
		}
	}

	// Resolve any ticker-based memberships to security IDs
	if req.Memberships != nil && len(req.Memberships) > 0 {
		if err := s.ResolveMembershipTickers(ctx, req.Memberships); err != nil {
			return nil, err
		}
	}

	// Validate ideal portfolio percentages are in decimal form
	// portfolio.PortfolioType has already been updated above if req.PortfolioType was set
	if portfolio.PortfolioType == models.PortfolioTypeIdeal && req.Memberships != nil {
		if err := validateIdealMemberships(req.Memberships); err != nil {
			return nil, err
		}
	}

	// Replace memberships if provided
	var memberships []models.PortfolioMembership
	if req.Memberships != nil {
		// Delete existing memberships
		if err := s.portfolioRepo.DeleteMemberships(ctx, tx, id); err != nil {
			return nil, fmt.Errorf("failed to delete memberships: %w", err)
		}

		// Create new memberships
		memberships, err = s.portfolioRepo.CreateMemberships(ctx, tx, id, req.Memberships)
		if err != nil {
			return nil, fmt.Errorf("failed to create memberships: %w", err)
		}
	} else {
		// Keep existing memberships
		memberships, err = s.portfolioRepo.GetMemberships(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("failed to get memberships: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &models.PortfolioWithMemberships{
		Portfolio:   *portfolio,
		Memberships: memberships,
	}, nil
}

// DeletePortfolio deletes a portfolio and its memberships
func (s *PortfolioService) DeletePortfolio(ctx context.Context, id int64, userID int64) error {
	// Get existing portfolio
	portfolio, err := s.portfolioRepo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrPortfolioNotFound) {
			return ErrPortfolioNotFound
		}
		return fmt.Errorf("failed to get portfolio: %w", err)
	}

	// Validate owner
	if portfolio.OwnerID != userID {
		return ErrUnauthorized
	}

	// Start transaction
	tx, err := s.portfolioRepo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Delete memberships first (foreign key constraint)
	if err := s.portfolioRepo.DeleteMemberships(ctx, tx, id); err != nil {
		return fmt.Errorf("failed to delete memberships: %w", err)
	}

	// Delete portfolio
	if err := s.portfolioRepo.Delete(ctx, tx, id); err != nil {
		return fmt.Errorf("failed to delete portfolio: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// validateIdealMemberships checks that ideal portfolio memberships use decimal
// form (0 < value <= 1.0) and that the total does not exceed 1.0.
func validateIdealMemberships(memberships []models.MembershipRequest) error {
	var total float64
	for i, m := range memberships {
		if m.PercentageOrShares <= 0 || m.PercentageOrShares > 1.0 {
			return fmt.Errorf("%w: membership[%d] has value %.4f", ErrInvalidIdealPercentage, i, m.PercentageOrShares)
		}
		total += m.PercentageOrShares
	}
	if total > 1.0 && math.Abs(total-1.0) > 0.0001 {
		return fmt.Errorf("%w: total is %.4f", ErrIdealTotalExceedsOne, total)
	}
	return nil
}

// ValidateObjective checks that the given objective is a valid enum value.
func ValidateObjective(obj models.Objective) error {
	if _, ok := ValidObjectives[obj]; !ok {
		return fmt.Errorf("%w: %q is not a valid objective", ErrInvalidObjective, obj)
	}
	return nil
}

// GetUserPortfolios retrieves all portfolios for a user (metadata only)
func (s *PortfolioService) GetUserPortfolios(ctx context.Context, userID int64) ([]models.PortfolioListItem, error) {
	portfolios, err := s.portfolioRepo.GetByUserID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user portfolios: %w", err)
	}
	return portfolios, nil
}

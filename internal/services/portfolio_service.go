package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

var (
	ErrPortfolioNotFound = errors.New("portfolio not found")
	ErrConflict          = errors.New("portfolio with same name and type already exists")
	ErrUnauthorized      = errors.New("not authorized to modify this portfolio")
)

// PortfolioService handles portfolio business logic
type PortfolioService struct {
	portfolioRepo *repository.PortfolioRepository
}

// NewPortfolioService creates a new PortfolioService
func NewPortfolioService(portfolioRepo *repository.PortfolioRepository) *PortfolioService {
	return &PortfolioService{
		portfolioRepo: portfolioRepo,
	}
}

// CreatePortfolio creates a new portfolio with memberships
func (s *PortfolioService) CreatePortfolio(ctx context.Context, req *models.CreatePortfolioRequest) (*models.PortfolioWithMemberships, error) {
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

	// Update portfolio name if provided
	if req.Name != "" {
		portfolio.Name = req.Name
		if err := s.portfolioRepo.Update(ctx, tx, portfolio); err != nil {
			return nil, fmt.Errorf("failed to update portfolio: %w", err)
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

// GetUserPortfolios retrieves all portfolios for a user (metadata only)
func (s *PortfolioService) GetUserPortfolios(ctx context.Context, userID int64) ([]models.PortfolioListItem, error) {
	portfolios, err := s.portfolioRepo.GetByUserID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user portfolios: %w", err)
	}
	return portfolios, nil
}

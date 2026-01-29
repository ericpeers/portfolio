package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

// MembershipService handles membership computation and comparison
type MembershipService struct {
	secRepo       *repository.SecurityRepository
	portfolioRepo *repository.PortfolioRepository
	pricingSvc    *PricingService
	avClient      *alphavantage.Client
}

// NewMembershipService creates a new MembershipService
func NewMembershipService(
	secRepo *repository.SecurityRepository,
	portfolioRepo *repository.PortfolioRepository,
	pricingSvc *PricingService,
	avClient *alphavantage.Client,
) *MembershipService {
	return &MembershipService{
		secRepo:       secRepo,
		portfolioRepo: portfolioRepo,
		pricingSvc:    pricingSvc,
		avClient:      avClient,
	}
}

// ComputeMembership computes expanded memberships for a portfolio, recursively expanding ETFs
// For Ideal portfolios: multiply ETF allocation × security percentage
// For Active portfolios: shares × end_price × allocation ÷ portfolio_value
func (s *MembershipService) ComputeMembership(ctx context.Context, portfolioID int64, portfolioType models.PortfolioType, endDate time.Time) ([]models.ExpandedMembership, error) {
	memberships, err := s.portfolioRepo.GetMemberships(ctx, portfolioID)
	if err != nil {
		return nil, fmt.Errorf("failed to get memberships: %w", err)
	}

	// Collect security IDs
	secIDs := make([]int64, len(memberships))
	for i, m := range memberships {
		secIDs[i] = m.SecurityID
	}

	// Get security details
	securities, err := s.secRepo.GetMultipleByIDs(ctx, secIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to get securities: %w", err)
	}

	// Calculate total portfolio value for active portfolios
	var totalValue float64
	if portfolioType == models.PortfolioTypeActive {
		for _, m := range memberships {
			price, err := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, endDate)
			if err != nil {
				return nil, fmt.Errorf("failed to get price for security %d: %w", m.SecurityID, err)
			}
			totalValue += m.PercentageOrShares * price
		}
	} else {
		// For ideal portfolios, percentages should sum to 100
		for _, m := range memberships {
			totalValue += m.PercentageOrShares
		}
	}

	if totalValue == 0 {
		return nil, nil
	}

	// Expand memberships
	expanded := make(map[int64]*models.ExpandedMembership)

	for _, m := range memberships {
		sec := securities[m.SecurityID]
		if sec == nil {
			continue
		}

		// Calculate allocation percentage
		var allocation float64
		if portfolioType == models.PortfolioTypeIdeal {
			allocation = m.PercentageOrShares / totalValue * 100
		} else {
			price, _ := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, endDate)
			allocation = (m.PercentageOrShares * price / totalValue) * 100
		}

		// Check if this is an ETF or mutual fund that needs expansion
		if sec.SecurityType == models.SecurityTypeETF || sec.SecurityType == models.SecurityTypeMutualFund {
			// Get ETF holdings
			etfHoldings, err := s.getETFHoldings(ctx, m.SecurityID, sec.Symbol)
			if err != nil {
				// If we can't expand, treat it as a single holding
				s.addToExpanded(expanded, m.SecurityID, sec.Symbol, allocation)
				continue
			}

			// Expand ETF holdings recursively
			for _, holding := range etfHoldings {
				// Find or create the underlying security
				underlyingSec, err := s.secRepo.GetBySymbol(ctx, holding.Symbol)
				if err != nil {
					// Skip if we can't find the underlying security
					continue
				}

				// Calculate the allocation for this underlying holding
				// allocation × holding percentage / 100
				underlyingAllocation := allocation * holding.Percentage / 100
				s.addToExpanded(expanded, underlyingSec.ID, underlyingSec.Symbol, underlyingAllocation)
			}
		} else {
			s.addToExpanded(expanded, m.SecurityID, sec.Symbol, allocation)
		}
	}

	// Convert map to slice
	result := make([]models.ExpandedMembership, 0, len(expanded))
	for _, em := range expanded {
		result = append(result, *em)
	}

	return result, nil
}

// getETFHoldings retrieves ETF holdings, fetching from AlphaVantage if needed
func (s *MembershipService) getETFHoldings(ctx context.Context, etfID int64, symbol string) ([]alphavantage.ParsedETFHolding, error) {
	// Check if we have cached holdings that are fresh enough (e.g., within 24 hours)
	fetchedAt, err := s.secRepo.GetETFMembershipFetchedAt(ctx, etfID)
	if err != nil {
		return nil, err
	}

	if !fetchedAt.IsZero() && time.Since(fetchedAt) < 24*time.Hour {
		// Use cached holdings
		memberships, err := s.secRepo.GetETFMembership(ctx, etfID)
		if err != nil {
			return nil, err
		}

		// Get security symbols
		secIDs := make([]int64, len(memberships))
		for i, m := range memberships {
			secIDs[i] = m.SecurityID
		}
		securities, err := s.secRepo.GetMultipleByIDs(ctx, secIDs)
		if err != nil {
			return nil, err
		}

		var holdings []alphavantage.ParsedETFHolding
		for _, m := range memberships {
			sec := securities[m.SecurityID]
			if sec != nil {
				holdings = append(holdings, alphavantage.ParsedETFHolding{
					Symbol:     sec.Symbol,
					Percentage: m.Percentage,
				})
			}
		}
		return holdings, nil
	}

	// Fetch from AlphaVantage
	holdings, err := s.avClient.GetETFHoldings(ctx, symbol)
	if err != nil {
		return nil, err
	}

	// Cache the holdings (we would need to resolve symbols to security IDs here)
	// For now, just return the holdings without caching
	// do we need to track individual stock sources...

	return holdings, nil
}

func (s *MembershipService) addToExpanded(expanded map[int64]*models.ExpandedMembership, secID int64, symbol string, allocation float64) {
	if em, exists := expanded[secID]; exists {
		em.Allocation += allocation
	} else {
		expanded[secID] = &models.ExpandedMembership{
			SecurityID: secID,
			Symbol:     symbol,
			Allocation: allocation,
		}
	}
}

// DiffMembership compares two sets of expanded memberships
func (s *MembershipService) DiffMembership(membershipA, membershipB []models.ExpandedMembership) []models.MembershipDiff {
	// Create maps for easy lookup
	mapA := make(map[int64]models.ExpandedMembership)
	mapB := make(map[int64]models.ExpandedMembership)

	for _, m := range membershipA {
		mapA[m.SecurityID] = m
	}
	for _, m := range membershipB {
		mapB[m.SecurityID] = m
	}

	// Collect all unique security IDs
	allIDs := make(map[int64]bool)
	for id := range mapA {
		allIDs[id] = true
	}
	for id := range mapB {
		allIDs[id] = true
	}

	// Compute differences
	var diffs []models.MembershipDiff
	for id := range allIDs {
		mA := mapA[id]
		mB := mapB[id]

		symbol := mA.Symbol
		if symbol == "" {
			symbol = mB.Symbol
		}

		diff := models.MembershipDiff{
			SecurityID:  id,
			Symbol:      symbol,
			AllocationA: mA.Allocation,
			AllocationB: mB.Allocation,
			Difference:  mA.Allocation - mB.Allocation,
		}
		diffs = append(diffs, diff)
	}

	return diffs
}

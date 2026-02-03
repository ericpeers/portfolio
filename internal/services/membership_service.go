package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
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
// FIXME: This should track which ETF contributed what, and be able to provide a total view, as well as a per ETF view of securities
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
		isETFOrMF, err := s.secRepo.IsETFOrMutualFund(ctx, m.SecurityID)
		if err != nil {
			return nil, fmt.Errorf("failed to check if security %d is ETF/MF: %w", m.SecurityID, err)
		}
		if isETFOrMF {
			// Get ETF holdings
			etfHoldings, _, err := s.GetETFHoldings(ctx, m.SecurityID, sec.Symbol)
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

// GetETFHoldings retrieves ETF holdings, fetching from AlphaVantage if needed
// Returns holdings with security metadata when available from cache
func (s *MembershipService) GetETFHoldings(ctx context.Context, etfID int64, symbol string) ([]alphavantage.ParsedETFHolding, *time.Time, error) {
	// Check if we have cached holdings that are still fresh (based on next_update)
	pullRange, err := s.secRepo.GetETFPullRange(ctx, etfID)
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()
	if pullRange != nil && now.Before(pullRange.NextUpdate) {
		// Use cached holdings
		memberships, err := s.secRepo.GetETFMembership(ctx, etfID)
		if err != nil {
			return nil, nil, err
		}

		// Get security symbols
		secIDs := make([]int64, len(memberships))
		for i, m := range memberships {
			secIDs[i] = m.SecurityID
		}
		securities, err := s.secRepo.GetMultipleByIDs(ctx, secIDs)
		if err != nil {
			return nil, nil, err
		}

		var holdings []alphavantage.ParsedETFHolding
		for _, m := range memberships {
			sec := securities[m.SecurityID]
			if sec != nil {
				holdings = append(holdings, alphavantage.ParsedETFHolding{
					Symbol:     sec.Symbol,
					Name:       sec.Name,
					Percentage: m.Percentage,
				})
			}
		}
		pullDate := pullRange.PullDate
		return holdings, &pullDate, nil
	}

	// Fetch from AlphaVantage
	holdings, err := s.avClient.GetETFHoldings(ctx, symbol)
	if err != nil {
		return nil, nil, err
	}

	// Persist the holdings
	err = s.persistETFHoldings(ctx, etfID, holdings)
	if err != nil {
		log.Errorf("Issue in saving ETF holdings: %w", err)
		// Log error but don't fail - we still have the holdings to return
	}

	return holdings, nil, nil
}

// persistETFHoldings saves ETF holdings to the database
func (s *MembershipService) persistETFHoldings(ctx context.Context, etfID int64, holdings []alphavantage.ParsedETFHolding) error {
	// Resolve symbols to security IDs
	var memberships []models.ETFMembership
	for _, h := range holdings {
		//FIXME: This is going to be slow. This needs to bulk fetch the securities instead of fetching them one at a time.
		//get a full list of holdings SYMBOLS, then fetch them from sql in a single call (select ticker, id from dim_security where ticker in ('STOCK1', 'STOCK2', 'STOCK3') ), then map from Symbol to ID.
		sec, err := s.secRepo.GetBySymbol(ctx, h.Symbol)
		if err != nil {
			// Skip securities we don't have in our database
			continue
		}
		memberships = append(memberships, models.ETFMembership{
			SecurityID: sec.ID,
			ETFID:      etfID,
			Percentage: h.Percentage,
		})
	}

	// Calculate next update time (next business day at 4:15 PM ET)
	nextUpdate := calculateNextBusinessDay(time.Now())

	// Start transaction
	tx, err := s.secRepo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.secRepo.UpsertETFMembership(ctx, tx, etfID, memberships, nextUpdate); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// FIXME: This is violating DRY. The code in pricing_service already does this at 4:30pm which is a better time to allow alphavantage to suck in data with a 15min trailing and 15min buffer.
// the two should likely move to a helper routine or other?
// calculateNextBusinessDay returns the next business day at 4:15 PM ET
func calculateNextBusinessDay(now time.Time) time.Time {
	// Load Eastern Time
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		loc = time.UTC
	}
	nowET := now.In(loc)

	// Start with today
	next := time.Date(nowET.Year(), nowET.Month(), nowET.Day(), 16, 15, 0, 0, loc)

	// If it's already past 4:15 PM, move to next day
	if nowET.After(next) {
		next = next.AddDate(0, 0, 1)
	}

	// Skip weekends
	for next.Weekday() == time.Saturday || next.Weekday() == time.Sunday {
		next = next.AddDate(0, 0, 1)
	}

	return next.UTC()
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

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

// expandedBuilder tracks allocation sources during membership expansion.
// Raw source contributions are normalized to sum to 1.0 when converting to the final model.
type expandedBuilder struct {
	secID      int64
	symbol     string
	allocation float64
	sources    map[int64]*sourceContribution // keyed by source security ID
}

type sourceContribution struct {
	secID    int64
	symbol   string
	rawAlloc float64 // raw portfolio allocation contributed by this source
}

func TrackTime(funcName string, start time.Time) {
	elapsed := time.Since(start)
	log.Debugf("%s took %d ms", funcName, elapsed.Milliseconds())
}

// ComputeMembership computes expanded memberships for a portfolio, recursively expanding ETFs.
// For Ideal portfolios: multiply ETF allocation × security percentage
// For Active portfolios: shares × end_price × allocation ÷ portfolio_value
// Each expanded membership includes sources showing which holdings (direct or ETF) contributed
// to the security's total allocation, with source allocations normalized to sum to 1.0.
func (s *MembershipService) ComputeMembership(ctx context.Context, portfolioID int64, portfolioType models.PortfolioType, endDate time.Time) ([]models.ExpandedMembership, error) {
	defer TrackTime("ComputeMembership", time.Now())
	memberships, err := s.portfolioRepo.GetMemberships(ctx, portfolioID)
	if err != nil {
		return nil, fmt.Errorf("failed to get memberships: %s", err)
	}

	// Collect security IDs
	secIDs := make([]int64, len(memberships))
	for i, m := range memberships {
		secIDs[i] = m.SecurityID
	}

	// Get security details
	securities, err := s.secRepo.GetMultipleByIDs(ctx, secIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to get securities: %s", err)
	}

	// Calculate total portfolio value for active portfolios
	var totalValue float64
	if portfolioType == models.PortfolioTypeActive {
		for _, m := range memberships {
			price, err := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, endDate)
			if err != nil {
				return nil, fmt.Errorf("failed to get price for security %d: %s", m.SecurityID, err)
			}
			totalValue += m.PercentageOrShares * price
		}
	} else {
		// For ideal portfolios, percentages should sum to 1.0
		for _, m := range memberships {
			totalValue += m.PercentageOrShares
		}
	}

	if totalValue == 0 {
		return nil, nil
	}

	// Expand memberships, tracking sources
	expanded := make(map[int64]*expandedBuilder)

	for _, m := range memberships {
		sec := securities[m.SecurityID]
		if sec == nil {
			continue
		}

		// Calculate allocation percentage
		var allocation float64
		if portfolioType == models.PortfolioTypeIdeal {
			allocation = m.PercentageOrShares / totalValue
		} else {
			price, _ := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, endDate)
			allocation = m.PercentageOrShares * price / totalValue
		}

		// Check if this is an ETF or mutual fund that needs expansion
		isETFOrMF, err := s.secRepo.IsETFOrMutualFund(ctx, m.SecurityID)
		if err != nil {
			return nil, fmt.Errorf("failed to check if security %d is ETF/MF: %s", m.SecurityID, err)
		}
		if isETFOrMF {
			// Get ETF holdings
			etfHoldings, pullDate, err := s.GetETFHoldings(ctx, m.SecurityID, sec.Symbol)
			if err != nil {
				// If we can't expand, treat it as a single holding; source is itself
				s.addToExpanded(expanded, m.SecurityID, sec.Symbol, allocation, m.SecurityID, sec.Symbol)
				log.Errorf("Couldn't expand ETF: %s", sec.Symbol)
				continue
			}

			// Resolver chain: merge swaps into real equities, then handle special symbols
			resolved, unresolved := ResolveSwapHoldings(etfHoldings)
			resolved2, unresolved2 := ResolveSpecialSymbols(unresolved)
			resolved = append(resolved, resolved2...)

			// Warn about anything still unresolved
			for _, uh := range unresolved2 {
				AddWarning(ctx, models.Warning{
					Code:    models.WarnUnresolvedETFHolding,
					Message: fmt.Sprintf("ETF %s: unresolved holding %q (weight %.4f)", sec.Symbol, uh.Name, uh.Percentage),
				})
			}

			// Validate that all resolved symbols exist in dim_security.
			// This prevents unknown symbols (e.g. FGXXX) from inflating
			// the normalization sum and then being lost in the expansion loop.
			resolvedSymbols := make([]string, len(resolved))
			for i, h := range resolved {
				resolvedSymbols[i] = h.Symbol
			}
			knownSecurities, err := s.secRepo.GetMultipleBySymbols(ctx, resolvedSymbols)
			if err != nil {
				log.Errorf("Failed to validate resolved symbols for ETF %s: %s", sec.Symbol, err)
			} else {
				var validated []alphavantage.ParsedETFHolding
				for _, h := range resolved {
					if _, ok := knownSecurities[h.Symbol]; ok {
						validated = append(validated, h)
					} else {
						AddWarning(ctx, models.Warning{
							Code:    models.WarnUnresolvedETFHolding,
							Message: fmt.Sprintf("ETF %s: symbol %q not found in database (weight %.4f)", sec.Symbol, h.Symbol, h.Percentage),
						})
					}
				}
				resolved = validated
			}

			// Normalize to 1.0
			resolved = NormalizeHoldings(ctx, resolved, sec.Symbol)

			// Persist resolved holdings if freshly fetched from AlphaVantage.
			// This must happen after the resolver chain so we store merged swap
			// weights under real symbols, not the raw "n/a" entries from AV.
			if pullDate == nil {
				if err := s.PersistETFHoldings(ctx, m.SecurityID, resolved); err != nil {
					log.Errorf("Issue in saving ETF holdings: %s", err)
				}
			}

			// Expand resolved holdings — source is the ETF
			for _, holding := range resolved {
				//FIXME. This should both A) never fail (even though Gemini thinks it might), and B) not get by symbol because we have previously fetched a suite of holdings with security ID's.
				//we should not need to GetBySymbol a second time - performance issue here to do 500 singleton fetches.
				underlyingSec, err := s.secRepo.GetBySymbol(ctx, holding.Symbol)
				if err != nil {
					log.Errorf("Couldn't retrieve symbol: %s", holding.Symbol)
					continue
				}

				underlyingAllocation := allocation * holding.Percentage
				s.addToExpanded(expanded, underlyingSec.ID, underlyingSec.Symbol, underlyingAllocation, m.SecurityID, sec.Symbol)
			}
		} else {
			// Direct holding — source is itself
			s.addToExpanded(expanded, m.SecurityID, sec.Symbol, allocation, m.SecurityID, sec.Symbol)
		}
	}

	// Convert builders to model slice, normalizing source allocations
	result := make([]models.ExpandedMembership, 0, len(expanded))
	for _, b := range expanded {
		if b.allocation == 0 {
			continue // skip zero-allocation entries to avoid NaN from division
		}
		sources := make([]models.MembershipSource, 0, len(b.sources))
		for _, src := range b.sources {
			sources = append(sources, models.MembershipSource{
				SecurityID: src.secID,
				Symbol:     src.symbol,
				Allocation: src.rawAlloc / b.allocation, // normalize so sources sum to 1.0
			})
		}
		result = append(result, models.ExpandedMembership{
			SecurityID: b.secID,
			Symbol:     b.symbol,
			Allocation: b.allocation,
			Sources:    sources,
		})
	}

	return result, nil
}

// GetETFHoldings retrieves ETF holdings, fetching from AlphaVantage if needed
// Returns holdings with security metadata when available from cache
func (s *MembershipService) GetETFHoldings(ctx context.Context, etfID int64, symbol string) ([]alphavantage.ParsedETFHolding, *time.Time, error) {
	defer TrackTime("GetETFHoldings", time.Now())

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

	// NOTE: persistence is the caller's responsibility so that the resolver
	// chain can run first and we store resolved holdings (with swap weights
	// merged into real equities) rather than the raw AV response which
	// contains "n/a" symbols that can't be stored.
	return holdings, nil, nil
}

// PersistETFHoldings saves ETF holdings to the database.
// Callers should run the resolver chain before persisting so that
// swap-merged holdings are stored rather than raw AV data.
func (s *MembershipService) PersistETFHoldings(ctx context.Context, etfID int64, holdings []alphavantage.ParsedETFHolding) error {
	// Collect all symbols for bulk lookup
	symbols := make([]string, len(holdings))
	for i, h := range holdings {
		symbols[i] = h.Symbol
	}

	// Bulk fetch securities by symbol
	securities, err := s.secRepo.GetMultipleBySymbols(ctx, symbols)
	if err != nil {
		return fmt.Errorf("failed to bulk fetch securities: %s", err)
	}

	// Build memberships using the fetched securities
	var memberships []models.ETFMembership
	for _, h := range holdings {
		sec := securities[h.Symbol]
		if sec == nil {
			// Skip securities we don't have in our database
			continue
		}
		memberships = append(memberships, models.ETFMembership{
			SecurityID: sec.ID,
			ETFID:      etfID,
			Percentage: h.Percentage,
		})
	}

	// Calculate next update time (next business day at 4:30 PM ET)
	nextUpdate := NextMarketDate(time.Now())

	// Start transaction
	tx, err := s.secRepo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %s", err)
	}
	defer tx.Rollback(ctx)

	if err := s.secRepo.UpsertETFMembership(ctx, tx, etfID, memberships, nextUpdate); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *MembershipService) addToExpanded(expanded map[int64]*expandedBuilder, secID int64, symbol string, allocation float64, sourceID int64, sourceSymbol string) {
	if b, exists := expanded[secID]; exists {
		b.allocation += allocation
		if src, exists := b.sources[sourceID]; exists {
			src.rawAlloc += allocation
		} else {
			b.sources[sourceID] = &sourceContribution{secID: sourceID, symbol: sourceSymbol, rawAlloc: allocation}
		}
	} else {
		expanded[secID] = &expandedBuilder{
			secID:      secID,
			symbol:     symbol,
			allocation: allocation,
			sources: map[int64]*sourceContribution{
				sourceID: {secID: sourceID, symbol: sourceSymbol, rawAlloc: allocation},
			},
		}
	}
}

// ComputeDirectMembership returns the raw portfolio holdings as decimal percentages
// without expanding ETFs. For Ideal portfolios, allocation = PercentageOrShares / total.
// For Active portfolios, allocation = (shares * price) / totalValue.
func (s *MembershipService) ComputeDirectMembership(ctx context.Context, portfolioID int64, portfolioType models.PortfolioType, endDate time.Time) ([]models.ExpandedMembership, error) {
	memberships, err := s.portfolioRepo.GetMemberships(ctx, portfolioID)
	if err != nil {
		return nil, fmt.Errorf("failed to get memberships: %s", err)
	}

	secIDs := make([]int64, len(memberships))
	for i, m := range memberships {
		secIDs[i] = m.SecurityID
	}

	securities, err := s.secRepo.GetMultipleByIDs(ctx, secIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to get securities: %s", err)
	}

	var totalValue float64
	if portfolioType == models.PortfolioTypeActive {
		for _, m := range memberships {
			price, err := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, endDate)
			if err != nil {
				return nil, fmt.Errorf("failed to get price for security %d: %s", m.SecurityID, err)
			}
			totalValue += m.PercentageOrShares * price
		}
	} else {
		for _, m := range memberships {
			totalValue += m.PercentageOrShares
		}
	}

	if totalValue == 0 {
		return nil, nil
	}

	result := make([]models.ExpandedMembership, 0, len(memberships))
	for _, m := range memberships {
		sec := securities[m.SecurityID]
		if sec == nil {
			continue
		}

		var allocation float64
		if portfolioType == models.PortfolioTypeIdeal {
			allocation = m.PercentageOrShares / totalValue
		} else {
			price, _ := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, endDate)
			allocation = m.PercentageOrShares * price / totalValue
		}

		result = append(result, models.ExpandedMembership{
			SecurityID: m.SecurityID,
			Symbol:     sec.Symbol,
			Allocation: allocation,
		})
	}

	return result, nil
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

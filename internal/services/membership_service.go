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

// GetAllSecurities fetches all securities from the database and returns
// a by-ID map (single winner per ID) and a by-symbol slice map (all exchange
// listings per ticker) for multi-exchange resolution via PreferUSListing/OnlyUSListings.
func (s *MembershipService) GetAllSecurities(ctx context.Context) (map[int64]*models.Security, map[string][]*models.SecurityWithCountry, error) {
	defer TrackTime("GetAllSecurities", time.Now())
	all, err := s.secRepo.GetAllWithCountry(ctx)
	if err != nil {
		return nil, nil, err
	}
	byID := make(map[int64]*models.Security, len(all))
	bySymbol := make(map[string][]*models.SecurityWithCountry, len(all))
	for _, sec := range all {
		byID[sec.ID] = &sec.Security
		bySymbol[sec.Symbol] = append(bySymbol[sec.Symbol], sec)
	}
	return byID, bySymbol, nil
}

// ComputeMembership computes expanded memberships for a portfolio, recursively expanding ETFs.
// For Ideal portfolios: multiply ETF allocation × security percentage
// For Active portfolios: split-adjusted shares × end_price × allocation ÷ portfolio_value
// startDate is used to determine which splits to apply (splits between startDate and endDate).
// Each expanded membership includes sources showing which holdings (direct or ETF) contributed
// to the security's total allocation, with source allocations normalized to sum to 1.0.
// prefetchedSecurities and prefetchedBySymbol must be non-nil (use GetAllSecurities to obtain them).
func (s *MembershipService) ComputeMembership(ctx context.Context, portfolioID int64, portfolioType models.PortfolioType, startDate, endDate time.Time, prefetchedSecurities map[int64]*models.Security, prefetchedBySymbol map[string][]*models.SecurityWithCountry) ([]models.ExpandedMembership, error) {
	defer TrackTime("ComputeMembership ", time.Now())
	memberships, err := s.portfolioRepo.GetMemberships(ctx, portfolioID)
	if err != nil {
		return nil, fmt.Errorf("failed to get memberships: %s", err)
	}

	// Collect security IDs
	secIDs := make([]int64, len(memberships))
	for i, m := range memberships {
		secIDs[i] = m.SecurityID
	}

	securities := prefetchedSecurities

	// Calculate total portfolio value for active portfolios
	var totalValue float64
	priceMap := make(map[int64]float64)
	adjustedSharesMap := make(map[int64]float64)
	if portfolioType == models.PortfolioTypeActive {
		for _, m := range memberships {
			price, err := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, endDate) //FIXME: bulk
			if err != nil {
				return nil, fmt.Errorf("failed to get price for security %d: %s", m.SecurityID, err)
			}
			splitCoeff, err := s.pricingSvc.GetSplitAdjustment(ctx, m.SecurityID, startDate, endDate) //FIXME: bulk
			if err != nil {
				return nil, fmt.Errorf("failed to get split adjustment for security %d: %s", m.SecurityID, err)
			}
			adjustedShares := m.PercentageOrShares * splitCoeff
			priceMap[m.SecurityID] = price
			adjustedSharesMap[m.SecurityID] = adjustedShares
			totalValue += adjustedShares * price
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
			price := priceMap[m.SecurityID]
			allocation = adjustedSharesMap[m.SecurityID] * price / totalValue
		}

		if sec.Type == string(models.SecurityTypeETF) || sec.Type == string(models.SecurityTypeMutualFund) {
			// Get ETF holdings
			etfHoldings, pullDate, err := s.GetETFHoldings(ctx, m.SecurityID, sec.Symbol, prefetchedSecurities)
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
			{
				var validated []alphavantage.ParsedETFHolding
				for _, h := range resolved {
					if len(prefetchedBySymbol[h.Symbol]) > 0 {
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
				if err := s.PersistETFHoldings(ctx, m.SecurityID, resolved, prefetchedBySymbol); err != nil {
					log.Errorf("Issue in saving ETF holdings: %s", err)
				}
			}

			// Choose resolution strategy based on the specific ETF listing the
			// user added to their portfolio (identified by m.SecurityID), not an
			// arbitrary candidate. A ticker like "VB" can appear on both NYSE ARCA
			// (USD) and the Mexican exchange (MXN); using index 0 would be wrong.
			resolveHolding := repository.PreferUSListing
			for _, c := range prefetchedBySymbol[sec.Symbol] {
				if c.ID == m.SecurityID {
					if repository.ShouldPreferNonUSForETF(c) {
						if repository.IsEmergingMarketsETF(c) {
							resolveHolding = repository.PreferEmergingNonUSListing
						} else {
							resolveHolding = repository.PreferDevelopedNonUSListing
						}
					}
					break
				}
			}

			// Expand resolved holdings. The validation filter above guarantees
			// all symbols in resolved exist in prefetchedBySymbol.
			for _, holding := range resolved {
				candidates := prefetchedBySymbol[holding.Symbol]
				underlyingSec := resolveHolding(candidates)
				if underlyingSec == nil {
					log.Errorf("Couldn't retrieve symbol held by ETF: %s, Symbol: %s", sec.Symbol, holding.Symbol)
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
func (s *MembershipService) GetETFHoldings(ctx context.Context, etfID int64, symbol string, prefetchedByID map[int64]*models.Security) ([]alphavantage.ParsedETFHolding, *time.Time, error) {
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

		securities := prefetchedByID

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
// knownSecurities must be non-nil (use GetAllSecurities to obtain it).
func (s *MembershipService) PersistETFHoldings(ctx context.Context, etfID int64, holdings []alphavantage.ParsedETFHolding, knownSecurities map[string][]*models.SecurityWithCountry) error {
	// Build memberships using the fetched securities
	var memberships []models.ETFMembership
	for _, h := range holdings {
		sec := repository.PreferUSListing(knownSecurities[h.Symbol])
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
// For Active portfolios, allocation = (split-adjusted shares * price) / totalValue.
// startDate is used to determine which splits to apply (splits between startDate and endDate).
// prefetchedSecurities must be non-nil (use GetAllSecurities to obtain it).
func (s *MembershipService) ComputeDirectMembership(ctx context.Context, portfolioID int64, portfolioType models.PortfolioType, startDate, endDate time.Time, prefetchedSecurities map[int64]*models.Security) ([]models.ExpandedMembership, error) {
	defer TrackTime("ComputeDirectMembership", time.Now())
	memberships, err := s.portfolioRepo.GetMemberships(ctx, portfolioID)
	if err != nil {
		return nil, fmt.Errorf("failed to get memberships: %s", err)
	}

	secIDs := make([]int64, len(memberships))
	for i, m := range memberships {
		secIDs[i] = m.SecurityID
	}

	securities := prefetchedSecurities

	var totalValue float64
	adjustedSharesMap := make(map[int64]float64)
	priceMap := make(map[int64]float64)
	if portfolioType == models.PortfolioTypeActive {
		for _, m := range memberships {
			price, err := s.pricingSvc.GetPriceAtDate(ctx, m.SecurityID, endDate)
			if err != nil {
				return nil, fmt.Errorf("failed to get price for security %d: %s", m.SecurityID, err)
			}
			splitCoeff, err := s.pricingSvc.GetSplitAdjustment(ctx, m.SecurityID, startDate, endDate)
			if err != nil {
				return nil, fmt.Errorf("failed to get split adjustment for security %d: %s", m.SecurityID, err)
			}
			adjustedShares := m.PercentageOrShares * splitCoeff
			priceMap[m.SecurityID] = price
			adjustedSharesMap[m.SecurityID] = adjustedShares
			totalValue += adjustedShares * price
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
			allocation = adjustedSharesMap[m.SecurityID] * priceMap[m.SecurityID] / totalValue
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

package services

import (
	"context"
	"fmt"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// MembershipService handles membership computation and comparison
type MembershipService struct {
	secRepo       *repository.SecurityRepository
	portfolioRepo *repository.PortfolioRepository
	pricingSvc    *PricingService
	avClient      providers.ETFHoldingsFetcher
}

// NewMembershipService creates a new MembershipService
func NewMembershipService(
	secRepo *repository.SecurityRepository,
	portfolioRepo *repository.PortfolioRepository,
	pricingSvc *PricingService,
	avClient providers.ETFHoldingsFetcher,
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
	ticker     string
	allocation float64
	sources    map[int64]*sourceContribution // keyed by source security ID
}

type sourceContribution struct {
	secID    int64
	ticker   string
	rawAlloc float64 // raw portfolio allocation contributed by this source
}

// ComputeMembership computes expanded memberships for a portfolio, recursively expanding ETFs.
// For Ideal portfolios: multiply ETF allocation × security percentage
// For Active portfolios: split-adjusted shares × end_price × allocation ÷ portfolio_value
// startDate is used to determine which splits to apply (splits between startDate and endDate).
// Each expanded membership includes sources showing which holdings (direct or ETF) contributed
// to the security's total allocation, with source allocations normalized to sum to 1.0.
// prefetchedSecurities and prefetchedByTicker must be non-nil (use GetAllSecurities to obtain them).
func (s *MembershipService) ComputeMembership(ctx context.Context, portfolioID int64, portfolioType models.PortfolioType, startDate, endDate time.Time, prefetchedSecurities map[int64]*models.Security, prefetchedByTicker map[string][]*models.SecurityWithCountry) ([]models.ExpandedMembership, error) {
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
		batchPrices, err := s.pricingSvc.GetPricesAtDateBatch(ctx, secIDs, endDate)
		if err != nil {
			return nil, fmt.Errorf("failed to batch-fetch prices: %s", err)
		}
		batchCoeffs, err := s.pricingSvc.GetSplitAdjustmentsBatch(ctx, secIDs, startDate, endDate)
		if err != nil {
			return nil, fmt.Errorf("failed to batch-fetch split coefficients: %s", err)
		}
		for _, m := range memberships {
			price, ok := batchPrices[m.SecurityID]
			if !ok {
				return nil, fmt.Errorf("no price found for security %d", m.SecurityID)
			}
			splitCoeff, ok := batchCoeffs[m.SecurityID]
			if !ok {
				splitCoeff = 1.0
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

	// Pre-batch ETF cache lookups to avoid one round-trip per ETF.
	etfMemberIDs := make([]int64, 0)
	for _, m := range memberships {
		sec := securities[m.SecurityID]
		if sec != nil && (sec.Type == string(models.SecurityTypeETF) || sec.Type == string(models.SecurityTypeMutualFund)) {
			etfMemberIDs = append(etfMemberIDs, m.SecurityID)
		}
	}

	pullRanges, err := s.secRepo.GetETFPullRanges(ctx, etfMemberIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to batch-fetch ETF pull ranges: %s", err)
	}

	now := time.Now()
	freshIDs := make([]int64, 0, len(etfMemberIDs))
	staleIDs := make(map[int64]bool)
	for _, id := range etfMemberIDs {
		pr := pullRanges[id]
		if pr != nil && now.Before(pr.NextUpdate) {
			freshIDs = append(freshIDs, id)
		} else {
			staleIDs[id] = true
		}
	}

	batchETFMemberships, err := s.secRepo.GetETFMemberships(ctx, freshIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to batch-fetch ETF memberships: %s", err)
	}

	etfHoldingsCache := make(map[int64][]providers.ParsedETFHolding)
	for etfID, mbs := range batchETFMemberships {
		var holdings []providers.ParsedETFHolding
		for _, mem := range mbs {
			sec := prefetchedSecurities[mem.SecurityID]
			if sec != nil {
				holdings = append(holdings, providers.ParsedETFHolding{
					Ticker:     sec.Ticker,
					Name:       sec.Name,
					Percentage: mem.Percentage,
				})
			}
		}
		etfHoldingsCache[etfID] = holdings
	}
	// Ensure fresh ETFs with no holdings have an entry (empty slice, not nil)
	for _, id := range freshIDs {
		if _, exists := etfHoldingsCache[id]; !exists {
			etfHoldingsCache[id] = []providers.ParsedETFHolding{}
		}
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
			var etfHoldings []providers.ParsedETFHolding
			if staleIDs[m.SecurityID] {
				// Rare path (1-month TTL): fetch from AV, resolve, persist
				var fetchErr error
				etfHoldings, _, fetchErr = s.FetchOrRefreshETFHoldings(ctx, m.SecurityID, sec.Ticker, prefetchedSecurities, prefetchedByTicker)
				if fetchErr != nil {
					s.addToExpanded(expanded, m.SecurityID, sec.Ticker, allocation, m.SecurityID, sec.Ticker)
					log.Errorf("Couldn't expand ETF: %s", sec.Ticker)
					continue
				}
			} else {
				etfHoldings = etfHoldingsCache[m.SecurityID]
			}

			// Choose resolution strategy based on the specific ETF listing the
			// user added to their portfolio (identified by m.SecurityID), not an
			// arbitrary candidate. A ticker like "VB" can appear on both NYSE ARCA
			// (USD) and the Mexican exchange (MXN); using index 0 would be wrong.
			resolveHolding := repository.PreferUSListing
			for _, c := range prefetchedByTicker[sec.Ticker] {
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

			// Expand resolved holdings. FetchOrRefreshETFHoldings guarantees
			// all returned symbols exist in prefetchedByTicker.
			for _, holding := range etfHoldings {
				candidates := prefetchedByTicker[holding.Ticker]
				underlyingSec := resolveHolding(candidates)
				if underlyingSec == nil {
					log.Errorf("Couldn't retrieve symbol held by ETF: %s, Symbol: %s", sec.Ticker, holding.Ticker)
					continue
				}

				underlyingAllocation := allocation * holding.Percentage
				s.addToExpanded(expanded, underlyingSec.ID, underlyingSec.Ticker, underlyingAllocation, m.SecurityID, sec.Ticker)
			}
		} else {
			// Direct holding — source is itself
			s.addToExpanded(expanded, m.SecurityID, sec.Ticker, allocation, m.SecurityID, sec.Ticker)
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
				Ticker:     src.ticker,
				Allocation: src.rawAlloc / b.allocation, // normalize so sources sum to 1.0
			})
		}
		result = append(result, models.ExpandedMembership{
			SecurityID: b.secID,
			Ticker:     b.ticker,
			Allocation: b.allocation,
			Sources:    sources,
		})
	}

	return result, nil
}

// ResolveAndPersistETFHoldings runs the full resolver chain on raw holdings,
// validates symbols against knownSecurities, normalizes weights to sum to 1.0,
// persists the result, and returns the resolved holdings.
// Warnings (unresolved symbols, unknown tickers) are added to ctx via AddWarning.
// knownSecurities must be non-nil (use GetAllSecurities to obtain it).
func (s *MembershipService) ResolveAndPersistETFHoldings(
	ctx context.Context,
	etfID int64,
	etfTicker string,
	rawHoldings []providers.ParsedETFHolding,
	knownSecurities map[string][]*models.SecurityWithCountry,
) ([]providers.ParsedETFHolding, error) {
	// Check source data integrity before any resolution.
	CheckSourceSum(ctx, rawHoldings, etfTicker)

	// Merge swap holdings into real equities, then handle special symbols.
	resolved, unresolved := ResolveSwapHoldings(rawHoldings)
	resolved2, unresolved2 := ResolveSpecialSymbols(unresolved)
	resolved = append(resolved, resolved2...)

	//These are just for the N/A style unresolved. It won't warn on symbol variants.
	for _, uh := range unresolved2 {
		AddWarning(ctx, models.Warning{
			Code:    models.WarnUnresolvedETFHolding,
			Message: fmt.Sprintf("ETF %s: unresolved holding %q (weight %.4f)", etfTicker, uh.Name, uh.Percentage),
		})
	}

	// Rewrite punctuation variants (BRK.B → BRK-B, etc.) before validation.
	resolved = ResolveSymbolVariants(resolved, knownSecurities)

	// Validate that all resolved symbols exist in dim_security.
	// This prevents unknown symbols (e.g. FGXXX) from inflating
	// the normalization sum and then being silently lost in the expansion loop.
	var validated []providers.ParsedETFHolding
	for _, h := range resolved {
		if len(knownSecurities[h.Ticker]) > 0 {
			validated = append(validated, h)
		} else {
			AddWarning(ctx, models.Warning{
				Code:    models.WarnUnresolvedETFHolding,
				Message: fmt.Sprintf("ETF %s: symbol %q / Name: %s not found in database (weight %.4f)", etfTicker, h.Ticker, h.Name, h.Percentage),
			})
		}
	}
	resolved = validated

	// Normalize to 1.0 after dropping unknown symbols.
	resolved = NormalizeHoldings(ctx, resolved, etfTicker)

	if err := s.PersistETFHoldings(ctx, etfID, resolved, knownSecurities); err != nil {
		log.Errorf("Issue in saving ETF holdings: %s", err)
	}

	return resolved, nil
}

// FetchOrRefreshETFHoldings returns ETF holdings, serving from cache when fresh
// or fetching from AlphaVantage, resolving, and persisting when stale.
// pullDate is non-nil when holdings came from cache (indicates last AV fetch date).
// prefetchedByID and prefetchedByTicker must be non-nil (use GetAllSecurities).
func (s *MembershipService) FetchOrRefreshETFHoldings(
	ctx context.Context,
	etfID int64,
	ticker string,
	prefetchedByID map[int64]*models.Security,
	prefetchedByTicker map[string][]*models.SecurityWithCountry,
) ([]providers.ParsedETFHolding, *time.Time, error) {
	defer TrackTime("FetchOrRefreshETFHoldings: "+ticker, time.Now())

	pullRange, err := s.secRepo.GetETFPullRange(ctx, etfID)
	if err != nil {
		return nil, nil, err
	}

	if pullRange != nil && time.Now().Before(pullRange.NextUpdate) {
		// Serve from cache.
		memberships, err := s.secRepo.GetETFMembership(ctx, etfID)
		if err != nil {
			return nil, nil, err
		}
		var holdings []providers.ParsedETFHolding
		for _, m := range memberships {
			sec := prefetchedByID[m.SecurityID]
			if sec != nil {
				holdings = append(holdings, providers.ParsedETFHolding{
					Ticker:     sec.Ticker,
					Name:       sec.Name,
					Percentage: m.Percentage,
				})
			}
		}
		pullDate := pullRange.PullDate
		return holdings, &pullDate, nil
	}

	// Cache is stale — fetch from AlphaVantage, resolve, and persist.
	rawHoldings, err := s.avClient.GetETFHoldings(ctx, ticker)
	if err != nil {
		return nil, nil, err
	}

	resolved, err := s.ResolveAndPersistETFHoldings(ctx, etfID, ticker, rawHoldings, prefetchedByTicker)
	if err != nil {
		return nil, nil, err
	}
	return resolved, nil, nil
}

// PersistETFHoldings saves ETF holdings to the database.
// Callers should run the resolver chain before persisting so that
// swap-merged holdings are stored rather than raw AV data.
// knownSecurities must be non-nil (use GetAllSecurities to obtain it).
func (s *MembershipService) PersistETFHoldings(ctx context.Context, etfID int64, holdings []providers.ParsedETFHolding, knownSecurities map[string][]*models.SecurityWithCountry) error {
	// Build memberships using the fetched securities.
	// Multiple CSV symbols can resolve to the same security ID (e.g., two
	// regional listings of the same company both mapping to one DB record),
	// so accumulate percentages by ID to avoid a duplicate-key error on insert.
	seen := make(map[int64]float64)
	var membershipOrder []int64
	for _, h := range holdings {
		sec := repository.PreferUSListing(knownSecurities[h.Ticker])
		if sec == nil {
			continue
		}
		if _, exists := seen[sec.ID]; !exists {
			membershipOrder = append(membershipOrder, sec.ID)
		}
		seen[sec.ID] += h.Percentage
	}
	memberships := make([]models.ETFMembership, 0, len(membershipOrder))
	for _, id := range membershipOrder {
		memberships = append(memberships, models.ETFMembership{
			SecurityID: id,
			ETFID:      etfID,
			Percentage: seen[id],
		})
	}

	// Calculate next update time to 1 month out. We don't need to refetch ETF holdings regularly.
	// Historically this was (next business day at 4:30 PM ET)
	nextUpdate := NextMarketDate(time.Now().AddDate(0, 1, 0))

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

func (s *MembershipService) addToExpanded(expanded map[int64]*expandedBuilder, secID int64, ticker string, allocation float64, sourceID int64, sourceTicker string) {
	if b, exists := expanded[secID]; exists {
		b.allocation += allocation
		if src, exists := b.sources[sourceID]; exists {
			src.rawAlloc += allocation
		} else {
			b.sources[sourceID] = &sourceContribution{secID: sourceID, ticker: sourceTicker, rawAlloc: allocation}
		}
	} else {
		expanded[secID] = &expandedBuilder{
			secID:      secID,
			ticker:     ticker,
			allocation: allocation,
			sources: map[int64]*sourceContribution{
				sourceID: {secID: sourceID, ticker: sourceTicker, rawAlloc: allocation},
			},
		}
	}
}

// GetCachedETFMembership returns the cached constituent holdings of an ETF.
// ComputeMembership must have run first (it warms the cache via FetchOrRefreshETFHoldings).
func (s *MembershipService) GetCachedETFMembership(ctx context.Context, etfSecurityID int64) ([]models.ETFMembership, error) {
	return s.secRepo.GetETFMembership(ctx, etfSecurityID)
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
		batchPrices, err := s.pricingSvc.GetPricesAtDateBatch(ctx, secIDs, endDate)
		if err != nil {
			return nil, fmt.Errorf("failed to batch-fetch prices: %s", err)
		}
		batchCoeffs, err := s.pricingSvc.GetSplitAdjustmentsBatch(ctx, secIDs, startDate, endDate)
		if err != nil {
			return nil, fmt.Errorf("failed to batch-fetch split coefficients: %s", err)
		}
		for _, m := range memberships {
			price, ok := batchPrices[m.SecurityID]
			if !ok {
				return nil, fmt.Errorf("no price found for security %d", m.SecurityID)
			}
			splitCoeff, ok := batchCoeffs[m.SecurityID]
			if !ok {
				splitCoeff = 1.0
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
			Ticker:     sec.Ticker,
			Allocation: allocation,
		})
	}

	return result, nil
}


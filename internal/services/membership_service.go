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

	etfHoldingsCache := make(map[int64][]models.ETFMembership)
	for etfID, mbs := range batchETFMemberships {
		etfHoldingsCache[etfID] = mbs
	}
	// Ensure fresh ETFs with no holdings have an entry (empty slice, not nil)
	for _, id := range freshIDs {
		if _, exists := etfHoldingsCache[id]; !exists {
			etfHoldingsCache[id] = []models.ETFMembership{}
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
			var etfHoldings []models.ETFMembership
			if staleIDs[m.SecurityID] {
				// Rare path (1-month TTL): fetch from AV, resolve, persist
				var fetchErr error
				etfHoldings, _, fetchErr = s.FetchOrRefreshETFHoldings(ctx, m.SecurityID, sec.Ticker, prefetchedSecurities, prefetchedByTicker)
				if fetchErr != nil {
					// Expansion failed — log the error and fall back to treating the ETF
					// as a direct holding rather than dropping it from the comparison entirely.
					log.Errorf("Couldn't expand ETF: %s: %v", sec.Ticker, fetchErr)
					s.addToExpanded(expanded, m.SecurityID, sec.Ticker, allocation, m.SecurityID, sec.Ticker)
					continue
				}
			} else {
				etfHoldings = etfHoldingsCache[m.SecurityID]
			}

			// Expand holdings using the stored security IDs. Exchange selection
			// was applied at insert time by PersistETFHoldings, so no re-resolution
			// by ticker is needed here.
			for _, holding := range etfHoldings {
				underlyingSec := prefetchedSecurities[holding.SecurityID]
				if underlyingSec == nil {
					log.Errorf("Couldn't retrieve security held by ETF: %s, SecurityID: %d", sec.Ticker, holding.SecurityID)
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
// etfSec may be nil; nil falls back to PreferUSListing for security-ID selection.
// knownSecurities must be non-nil (use GetAllSecurities to obtain it).
func (s *MembershipService) ResolveAndPersistETFHoldings(
	ctx context.Context,
	etfSec *models.SecurityWithCountry,
	rawHoldings []providers.ParsedETFHolding,
	knownSecurities map[string][]*models.SecurityWithCountry,
) ([]providers.ParsedETFHolding, error) {
	etfTicker := ""
	var etfID int64
	if etfSec != nil {
		etfTicker = etfSec.Ticker
		etfID = etfSec.ID
	}

	resolved := ResolveHoldingsToTickers(ctx, rawHoldings, etfTicker, knownSecurities)

	memberships := ResolveTickersToMemberships(etfID, etfSec, resolved, knownSecurities)
	if err := s.PersistETFHoldings(ctx, etfID, memberships); err != nil {
		log.Errorf("Issue in saving ETF holdings: %s", err)
	}

	return resolved, nil
}

// FetchOrRefreshETFHoldings returns ETF holdings as resolved security IDs,
// serving from cache when fresh or fetching from AlphaVantage, resolving, and
// persisting when stale. pullDate is non-nil when holdings came from cache.
// prefetchedByID and prefetchedByTicker must be non-nil (use GetAllSecurities).
func (s *MembershipService) FetchOrRefreshETFHoldings(
	ctx context.Context,
	etfID int64,
	ticker string,
	prefetchedByID map[int64]*models.Security,
	prefetchedByTicker map[string][]*models.SecurityWithCountry,
) ([]models.ETFMembership, *time.Time, error) {
	defer TrackTime("FetchOrRefreshETFHoldings: "+ticker, time.Now())

	pullRange, err := s.secRepo.GetETFPullRange(ctx, etfID)
	if err != nil {
		return nil, nil, err
	}

	if pullRange != nil && time.Now().Before(pullRange.NextUpdate) {
		// Serve from cache — return stored security IDs directly.
		memberships, err := s.secRepo.GetETFMembership(ctx, etfID)
		if err != nil {
			return nil, nil, err
		}
		pullDate := pullRange.PullDate
		return memberships, &pullDate, nil
	}

	// Cache is stale — fetch from AlphaVantage, resolve, and persist.
	rawHoldings, err := s.avClient.GetETFHoldings(ctx, ticker)
	if err != nil {
		// AV unavailable (no key, rate-limit, etc.). If stale data exists in the
		// cache, serve it rather than failing and dropping the ETF from the result.
		if pullRange != nil {
			log.Warnf("FetchOrRefreshETFHoldings: AV fetch failed for %s, serving stale cache from %s: %v",
				ticker, pullRange.PullDate.Format("2006-01-02"), err)
			memberships, memErr := s.secRepo.GetETFMembership(ctx, etfID)
			if memErr != nil {
				return nil, nil, err // cache read also failed; surface original AV error
			}
			pullDate := pullRange.PullDate
			return memberships, &pullDate, nil
		}
		return nil, nil, err
	}

	// Find the specific ETF listing for exchange-context-aware security-ID selection.
	var etfSec *models.SecurityWithCountry
	for _, c := range prefetchedByTicker[ticker] {
		if c.ID == etfID {
			etfSec = c
			break
		}
	}

	if _, err := s.ResolveAndPersistETFHoldings(ctx, etfSec, rawHoldings, prefetchedByTicker); err != nil {
		return nil, nil, err
	}

	// Read back what was just persisted to return ETFMembership objects.
	memberships, err := s.secRepo.GetETFMembership(ctx, etfID)
	if err != nil {
		return nil, nil, err
	}
	return memberships, nil, nil
}

// ResolveTickersToMemberships converts normalized, validated holdings (output of
// ResolveHoldingsToTickers) into ETFMembership records by selecting the correct
// dim_security_id for each ticker.
//
// Note on the two "prefer non-US" concepts: ResolveSymbolVariants step 0
// checks for a non-US listing to validate that stripping "-F" gives the right
// ticker (preventing BH-F→BHF, a different company). ResolveTickersToMemberships
// picks among multiple candidates for an already-correct ticker. They are at
// different pipeline stages and cannot be merged.
//
// Selection priority per holding:
//  1. Single candidate — no ambiguity, use it directly.
//  2. Name matching — h.Name from the CSV disambiguates multiple listings of
//     the same ticker (e.g. "Delta Electronics (Thailand) PCL" picks Thai DELTA
//     over Hungarian DELTA).
//  3. ETF-context resolver — PreferEmergingNonUSListing, PreferDevelopedNonUSListing,
//     or PreferUSListing based on the ETF's name/currency.
//
// etfSec may be nil (ETF not found in knownSecurities); nil falls back to PreferUSListing.
// knownSecurities must be non-nil (use GetAllSecurities to obtain it).
func ResolveTickersToMemberships(etfID int64, etfSec *models.SecurityWithCountry, holdings []providers.ParsedETFHolding, knownSecurities map[string][]*models.SecurityWithCountry) []models.ETFMembership {
	// Multiple CSV symbols can resolve to the same security ID (e.g., two regional
	// listings of the same company), so accumulate percentages by ID.
	seen := make(map[int64]float64)
	var membershipOrder []int64
	for _, h := range holdings {
		candidates := knownSecurities[h.Ticker]
		if len(candidates) == 0 {
			continue
		}

		var secID int64
		var found bool

		// 1. Single candidate — no ambiguity.
		if len(candidates) == 1 {
			secID = candidates[0].ID
			found = true
		}

		// 2. Name matching — use the CSV holding name to pick among multiple listings.
		if !found && h.Name != "" {
			if swc := ResolveByName(h.Name, candidates); swc != nil {
				secID = swc.ID
				found = true
			}
		}

		// 3. ETF-context resolver — fall back to exchange-preference logic.
		if !found {
			var sec *models.Security
			if etfSec != nil && repository.ShouldPreferNonUSForETF(etfSec) {
				if repository.IsEmergingMarketsETF(etfSec) {
					sec = repository.PreferEmergingNonUSListing(candidates)
				} else {
					sec = repository.PreferDevelopedNonUSListing(candidates)
				}
			} else {
				sec = repository.PreferUSListing(candidates)
			}
			if sec != nil {
				secID = sec.ID
				found = true
			}
		}

		if !found {
			continue
		}
		if _, exists := seen[secID]; !exists {
			membershipOrder = append(membershipOrder, secID)
		}
		seen[secID] += h.Percentage
	}

	memberships := make([]models.ETFMembership, 0, len(membershipOrder))
	for _, id := range membershipOrder {
		memberships = append(memberships, models.ETFMembership{
			SecurityID: id,
			ETFID:      etfID,
			Percentage: seen[id],
		})
	}
	return memberships
}

// PersistETFHoldings writes resolved ETF memberships to the database.
// Callers must run the full resolver chain (ResolveAndPersistETFHoldings) to
// produce correct memberships before calling this.
func (s *MembershipService) PersistETFHoldings(ctx context.Context, etfID int64, memberships []models.ETFMembership) error {
	nextUpdate := NextMarketDate(time.Now().AddDate(0, 1, 0))

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


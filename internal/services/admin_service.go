package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// SyncSecuritiesResult contains the results of a security sync operation
type SyncSecuritiesResult struct {
	SecuritiesInserted int      `json:"securities_inserted"`
	SecuritiesSkipped  int      `json:"securities_skipped"`
	ExchangesCreated   []string `json:"exchanges_created"`
	Errors             []string `json:"errors"`
}

// AdminService handles administrative operations
type AdminService struct {
	securityRepo *repository.SecurityRepository
	exchangeRepo *repository.ExchangeRepository
	avClient     *alphavantage.Client
}

// NewAdminService creates a new AdminService
func NewAdminService(
	securityRepo *repository.SecurityRepository,
	exchangeRepo *repository.ExchangeRepository,
	avClient *alphavantage.Client,
) *AdminService {
	return &AdminService{
		securityRepo: securityRepo,
		exchangeRepo: exchangeRepo,
		avClient:     avClient,
	}
}

// SyncSecurities fetches securities from AlphaVantage and syncs them to dim_security
func (s *AdminService) SyncSecurities(ctx context.Context) (*SyncSecuritiesResult, error) {
	result := &SyncSecuritiesResult{
		ExchangesCreated: []string{},
		Errors:           []string{},
	}

	// Fetch listing status from AlphaVantage. We need to fetch listed and delisted that may be now trading on OTC.
	listed, err := s.avClient.GetListingStatus(ctx, "listed")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch listing status for listed entries: %w", err)
	}
	log.Debugf("Fetched %d listed records", len(listed))

	delisted, err := s.avClient.GetListingStatus(ctx, "delisted")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch listing status for delisted entries: %w", err)
	}
	log.Debugf("Fetched %d delisted records", len(delisted))

	entries := append(listed, delisted...)

	// Pre-load exchanges map
	exchanges, err := s.exchangeRepo.GetAllExchanges(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load exchanges: %w", err)
	}

	// First pass: collect all valid securities to insert
	var securitiesToInsert []repository.DimSecurityInput
	for _, entry := range entries {
		// historically we only process active securities, but we want to insert delisted, and now OTC ones as well.

		if entry.Symbol == "NXT(EXP20091224)" || entry.Symbol == "ASRV 8.45 06-30-28" || len(entry.Symbol) > 10 {
			//AFAICT, this is a fake stock. I suspect it might be a mountweazel/fake town/map trap.
			//filed an email with support@AV on 1/29/26.
			log.Debugf("Skipping security %s", entry.Symbol)
			continue
		}

		// Get or create exchange ID
		exchangeID, ok := exchanges[entry.Exchange]
		if !ok {
			// Create new exchange with country="USA"
			// we might need to change this if we end up parsing other CSV or add new exchanges.
			newID, err := s.exchangeRepo.CreateExchange(ctx, entry.Exchange, "USA")
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("failed to create exchange '%s': %v", entry.Exchange, err))
				continue
			}
			exchanges[entry.Exchange] = newID
			exchangeID = newID
			result.ExchangesCreated = append(result.ExchangesCreated, entry.Exchange)
		}

		// Map assetType to enum string
		secType, err := mapAssetType(entry.AssetType)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("unknown asset type '%s' for %s", entry.AssetType, entry.Symbol))
			continue
		}

		// Truncate name to 80 chars
		name := entry.Name
		if len(name) > 80 {
			name = name[:80]
		}

		securitiesToInsert = append(securitiesToInsert, repository.DimSecurityInput{
			Ticker:     entry.Symbol,
			Name:       name,
			ExchangeID: exchangeID,
			Type:       secType,
			Inception:  entry.IPODate,
		})
	}

	// Second pass: bulk insert all securities
	inserted, skipped, bulkErrs := s.securityRepo.BulkCreateDimSecurities(ctx, securitiesToInsert)
	result.SecuritiesInserted = inserted
	result.SecuritiesSkipped = skipped

	for _, err := range bulkErrs {
		result.Errors = append(result.Errors, err.Error())
		log.Errorf("Error: %s", err)
	}

	return result, nil
}

// DryRunSyncResult contains the analysis from a dry-run of SyncSecurities
type DryRunSyncResult struct {
	NewSecurities     int            `json:"new_securities"`      // tickers in AV not yet in DB
	ExchangesNeeded   []string       `json:"exchanges_needed"`    // new exchanges AV would create
	InceptionUpdates  int            `json:"inception_updates"`   // securities whose inception date was updated
	NameDifferences   int            `json:"name_differences"`    // existing securities where AV name differs
	TypeMismatches    map[string]int `json:"type_mismatches"`     // "av_type->db_type" → count
	UnknownAssetTypes map[string]int `json:"unknown_asset_types"` // AV types we can't map → count
	LongName          int            `json:"long_name"`           // securities with names exceeding 200 characters
	Errors            []string       `json:"errors"`
}

// DryRunSyncSecurities simulates SyncSecurities against the current database state
// without making structural changes (no new securities, no new exchanges).
// It does update inception dates for existing securities where AV provides data the DB lacks.
func (s *AdminService) DryRunSyncSecurities(ctx context.Context) (*DryRunSyncResult, error) {
	result := &DryRunSyncResult{
		TypeMismatches:    make(map[string]int),
		UnknownAssetTypes: make(map[string]int),
		Errors:            []string{},
	}

	// Fetch from AlphaVantage — same as SyncSecurities
	listed, err := s.avClient.GetListingStatus(ctx, "listed")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch listing status for listed entries: %w", err)
	}
	log.Debugf("DryRun: fetched %d listed records from AV", len(listed))

	delisted, err := s.avClient.GetListingStatus(ctx, "delisted")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch listing status for delisted entries: %w", err)
	}
	log.Debugf("DryRun: fetched %d delisted records from AV", len(delisted))

	//entries := append(listed, delisted...)
	entries := listed
	log.Error("Need to include delisted again")

	// Prefetch all securities from DB (populated by EODHD) into a by-symbol map
	allSecurities, err := s.securityRepo.GetAllUS(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to prefetch securities: %w", err)
	}
	bySymbol := make(map[string]*models.Security, len(allSecurities))
	for _, sec := range allSecurities {
		bySymbol[sec.Symbol] = sec
	}
	log.Debugf("DryRun: prefetched %d securities from database", len(allSecurities))

	// Prefetch exchanges
	exchanges, err := s.exchangeRepo.GetAllExchanges(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to prefetch exchanges: %w", err)
	}

	newExchangeSet := make(map[string]struct{})

	for _, entry := range entries {
		if entry.Symbol == "NXT(EXP20091224)" || entry.Symbol == "ASRV 8.45 06-30-28" || entry.Symbol == "-P-HIZ" {
			continue
		}

		// Track exchanges AV would need to create
		if _, ok := exchanges[entry.Exchange]; !ok {
			newExchangeSet[entry.Exchange] = struct{}{}
		}

		// Map AV asset type
		secType, typeErr := mapAssetType(entry.AssetType)
		if typeErr != nil {
			result.UnknownAssetTypes[entry.AssetType]++
		}

		existing, found := bySymbol[entry.Symbol]
		if !found {
			result.NewSecurities++
			continue
		}

		// Asset type mismatch against what's in DB
		if typeErr == nil && secType != existing.Type {
			key := fmt.Sprintf("%s->%s", secType, existing.Type)
			result.TypeMismatches[key]++
		}

		// Inception date: update DB when AV has a date and DB is missing or different
		if entry.IPODate != nil && (existing.Inception == nil || !entry.IPODate.Equal(*existing.Inception)) {
			result.InceptionUpdates++
			/*
				if err := s.securityRepo.UpdateInceptionDate(ctx, existing.ID, entry.IPODate); err != nil {
					result.Errors = append(result.Errors, fmt.Sprintf("failed to update inception for %s: %v", entry.Symbol, err))
				}
			*/
		}

		// Name difference
		name := entry.Name
		if len(name) > 200 {
			result.LongName++
			name = name[:200]
		}
		if name != existing.Name {
			result.NameDifferences++
		}
	}

	for name := range newExchangeSet {
		result.ExchangesNeeded = append(result.ExchangesNeeded, name)
	}

	// Summary logging
	log.Debugf("DryRun: %d AV tickers not found in DB (would be inserted)", result.NewSecurities)
	log.Debugf("DryRun: %d exchanges would need to be created: %v", len(result.ExchangesNeeded), result.ExchangesNeeded)
	log.Debugf("DryRun: %d inception would be updated from AV data", result.InceptionUpdates)
	log.Debugf("DryRun: %d name field differences (AV vs DB)", result.NameDifferences)
	log.Debugf("DryRun: asset type mismatches (av_mapped->db): %v", result.TypeMismatches)
	log.Debugf("DryRun: unknown AV asset types (no mapping): %v", result.UnknownAssetTypes)
	log.Debugf("DryRun: %d long Names", result.LongName)

	return result, nil
}

// mapAssetType maps AlphaVantage asset types to ds_type enum values
func mapAssetType(assetType string) (string, error) {
	switch strings.ToLower(assetType) {
	case "stock":
		return string(models.SecurityTypeStock), nil
	case "etf":
		return string(models.SecurityTypeETF), nil
	default:
		return "", fmt.Errorf("unsupported asset type: %s", assetType)
	}
}

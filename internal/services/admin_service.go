package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/epeers/portfolio/internal/alphavantage"
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

// mapAssetType maps AlphaVantage asset types to ds_type enum values
func mapAssetType(assetType string) (string, error) {
	switch strings.ToLower(assetType) {
	case "stock":
		return "stock", nil
	case "etf":
		return "etf", nil
	default:
		return "", fmt.Errorf("unsupported asset type: %s", assetType)
	}
}

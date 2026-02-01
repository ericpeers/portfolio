package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/repository"
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
	securityRepo     *repository.SecurityRepository
	exchangeRepo     *repository.ExchangeRepository
	securityTypeRepo *repository.SecurityTypeRepository
	avClient         *alphavantage.Client
}

// NewAdminService creates a new AdminService
func NewAdminService(
	securityRepo *repository.SecurityRepository,
	exchangeRepo *repository.ExchangeRepository,
	securityTypeRepo *repository.SecurityTypeRepository,
	avClient *alphavantage.Client,
) *AdminService {
	return &AdminService{
		securityRepo:     securityRepo,
		exchangeRepo:     exchangeRepo,
		securityTypeRepo: securityTypeRepo,
		avClient:         avClient,
	}
}

// SyncSecurities fetches securities from AlphaVantage and syncs them to dim_security
func (s *AdminService) SyncSecurities(ctx context.Context) (*SyncSecuritiesResult, error) {
	result := &SyncSecuritiesResult{
		ExchangesCreated: []string{},
		Errors:           []string{},
	}

	// Fetch listing status from AlphaVantage
	entries, err := s.avClient.GetListingStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch listing status: %w", err)
	}

	// Pre-load exchanges map
	exchanges, err := s.exchangeRepo.GetAllExchanges(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load exchanges: %w", err)
	}

	// Pre-load security types map
	securityTypes, err := s.securityTypeRepo.GetAllSecurityTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load security types: %w", err)
	}

	// First pass: collect all valid securities to insert
	var securitiesToInsert []repository.DimSecurityInput
	for _, entry := range entries {
		// Only process active securities
		if entry.Status != "Active" {
			continue
		}

		if entry.Symbol == "NXT(EXP20091224)" {
			//AFAICT, this is a fake stock. I suspect it might be a mountweazel/fake town/map trap.
			//filed an email with support@AV on 1/29/26.
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

		// Map assetType to type ID
		typeID, err := s.mapAssetTypeToID(entry.AssetType, securityTypes)
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
			TypeID:     typeID,
			Inception:  entry.IPODate,
		})
	}

	// Second pass: bulk insert all securities
	inserted, skipped, bulkErrs := s.securityRepo.BulkCreateDimSecurities(ctx, securitiesToInsert)
	result.SecuritiesInserted = inserted
	result.SecuritiesSkipped = skipped

	for _, err := range bulkErrs {
		result.Errors = append(result.Errors, err.Error())
	}

	return result, nil
}

// mapAssetTypeToID maps AlphaVantage asset types to security type IDs
func (s *AdminService) mapAssetTypeToID(assetType string, types map[string]int) (int, error) {
	// Normalize to lowercase for comparison
	assetTypeLower := strings.ToLower(assetType)

	switch assetTypeLower {
	case "stock":
		if id, ok := types["stock"]; ok {
			return id, nil
		}
	case "etf":
		if id, ok := types["etf"]; ok {
			return id, nil
		}
	}

	return 0, fmt.Errorf("unsupported asset type: %s", assetType)
}

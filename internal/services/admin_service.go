package services

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// SyncSecuritiesResult contains the results of a security sync operation
type SyncSecuritiesResult struct {
	SecuritiesInserted  int      `json:"securities_inserted"`
	SecuritiesSkipped   int      `json:"securities_skipped"`
	SkippedBadType      int      `json:"skipped_bad_type"`
	SkippedLongTicker   int      `json:"skipped_long_ticker"`
	ExchangesCreated    []string `json:"exchanges_created"`
	Errors              []string `json:"errors"`
}

// DryRunSyncResult contains the analysis from a dry-run of SyncSecurities
type DryRunSyncResult struct {
	NewSecurities     int            `json:"new_securities"`
	ExchangesNeeded   []string       `json:"exchanges_needed"`
	UnknownAssetTypes map[string]int `json:"unknown_asset_types"`
	LongTickers       int            `json:"long_tickers"`
	Errors            []string       `json:"errors"`
}

// AdminService handles administrative operations
type AdminService struct {
	securityRepo *repository.SecurityRepository
	exchangeRepo *repository.ExchangeRepository
	priceRepo    *repository.PriceRepository
	eodhdClient  providers.SecurityListFetcher
}

// NewAdminService creates a new AdminService
func NewAdminService(
	securityRepo *repository.SecurityRepository,
	exchangeRepo *repository.ExchangeRepository,
	priceRepo *repository.PriceRepository,
	eodhdClient providers.SecurityListFetcher,
) *AdminService {
	return &AdminService{
		securityRepo: securityRepo,
		exchangeRepo: exchangeRepo,
		priceRepo:    priceRepo,
		eodhdClient:  eodhdClient,
	}
}

// skipExchanges is the set of EODHD exchange codes that should not be imported.
var skipExchanges = map[string]bool{
	"EUFUND": true,
	"FOREX":  true,
	"CC":     true,
	"MONEY":  true,
}

// exchangeAliases maps EODHD exchange codes to the canonical name used in dim_exchanges.
var exchangeAliases = map[string]string{
	"GBOND": "BONDS/CASH/TREASURIES",
}

// validEODHDTypes is the set of valid ds_type enum values that EODHD data can map to.
var validEODHDTypes = map[string]bool{
	"COMMON STOCK":    true,
	"PREFERRED STOCK": true,
	"BOND":            true,
	"ETC":             true,
	"ETF":             true,
	"FUND":            true,
	"INDEX":           true,
	"NOTES":           true,
	"UNIT":            true,
	"WARRANT":         true,
	"CURRENCY":        true,
	"COMMODITY":       true,
	"OPTION":          true,
}

// mapEODHDAssetType converts a raw EODHD type string to the database ds_type enum value.
// Returns ("", false) for unrecognised types.
func mapEODHDAssetType(rawType string) (string, bool) {
	t := strings.ToUpper(strings.TrimSpace(rawType))
	if t == "MUTUAL FUND" {
		t = "FUND"
	}
	if validEODHDTypes[t] {
		return t, true
	}
	return "", false
}

// exchangeWorkerCount is the number of goroutines used to fetch symbol lists concurrently.
const exchangeWorkerCount = 8

// exchangeJob is sent to worker goroutines.
type exchangeJob struct {
	code    string
	dbName  string // canonical name in dim_exchanges (after alias resolution)
	country string
	id      int
}

// exchangeResult is returned by each worker goroutine.
type exchangeResult struct {
	code      string
	inserted  int
	skipped   int
	badType   int
	longTick  int
	errors    []string
}

// SyncSecurities fetches all exchange symbol lists from EODHD and syncs them into dim_security.
func (s *AdminService) SyncSecurities(ctx context.Context) (*SyncSecuritiesResult, error) {
	result := &SyncSecuritiesResult{
		ExchangesCreated: []string{},
		Errors:           []string{},
	}

	// 1. Fetch exchange list from EODHD.
	eohdExchanges, err := s.eodhdClient.GetExchangeList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch EODHD exchange list: %w", err)
	}
	log.Debugf("SyncSecurities: fetched %d exchanges from EODHD", len(eohdExchanges))

	// 2. Pre-fetch existing exchanges from DB.
	dbExchanges, err := s.exchangeRepo.GetAllExchanges(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load exchanges from DB: %w", err)
	}

	// 3. Sequentially create any missing exchanges before the goroutine pool starts
	//    (avoids concurrent writes to dim_exchanges).
	jobs := make([]exchangeJob, 0, len(eohdExchanges))
	for _, ex := range eohdExchanges {
		if skipExchanges[ex.Code] {
			continue
		}

		dbName := ex.Code
		if alias, ok := exchangeAliases[ex.Code]; ok {
			dbName = alias
		}

		dbID, exists := dbExchanges[dbName]
		if !exists {
			newID, err := s.exchangeRepo.CreateExchange(ctx, dbName, ex.Country)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("failed to create exchange %q: %v", dbName, err))
				continue
			}
			dbExchanges[dbName] = newID
			dbID = newID
			result.ExchangesCreated = append(result.ExchangesCreated, dbName)
			log.Debugf("Created exchange %q (country: %s, id: %d)", dbName, ex.Country, newID)
		}

		jobs = append(jobs, exchangeJob{
			code:    ex.Code,
			dbName:  dbName,
			country: ex.Country,
			id:      dbID,
		})
	}

	// 4. Launch goroutine pool to fetch and insert symbol lists concurrently.
	jobCh := make(chan exchangeJob, len(jobs))
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)

	resultCh := make(chan exchangeResult, len(jobs))

	var wg sync.WaitGroup
	for i := 0; i < exchangeWorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				r := s.processExchange(ctx, job)
				resultCh <- r
			}
		}()
	}

	wg.Wait()
	close(resultCh)

	// 5. Aggregate results.
	for r := range resultCh {
		result.SecuritiesInserted += r.inserted
		result.SecuritiesSkipped += r.skipped
		result.SkippedBadType += r.badType
		result.SkippedLongTicker += r.longTick
		result.Errors = append(result.Errors, r.errors...)
	}

	log.Debugf("SyncSecurities complete: inserted=%d skipped=%d badType=%d longTicker=%d errors=%d",
		result.SecuritiesInserted, result.SecuritiesSkipped,
		result.SkippedBadType, result.SkippedLongTicker, len(result.Errors))

	return result, nil
}

// processExchange fetches the symbol list for one exchange and bulk-inserts into dim_security.
func (s *AdminService) processExchange(ctx context.Context, job exchangeJob) exchangeResult {
	r := exchangeResult{code: job.code}

	symbols, err := s.eodhdClient.GetExchangeSymbolList(ctx, job.code)
	if err != nil {
		r.errors = append(r.errors, fmt.Sprintf("[%s] failed to fetch symbol list: %v", job.code, err))
		return r
	}

	seen := make(map[string]bool, len(symbols))
	toInsert := make([]repository.DimSecurityInput, 0, len(symbols))

	for _, sym := range symbols {
		if len(sym.Code) > 30 {
			r.longTick++
			continue
		}

		secType, ok := mapEODHDAssetType(sym.Type)
		if !ok {
			r.badType++
			continue
		}

		name := sym.Name
		if len(name) > 200 {
			name = name[:200]
		}

		var currency *string
		if sym.Currency != "" {
			c := sym.Currency
			if len(c) > 3 {
				c = c[:3]
			}
			currency = &c
		}

		var isin *string
		if sym.Isin != "" {
			isin = &sym.Isin
		}

		key := sym.Code + "\x00" + fmt.Sprint(job.id)
		if seen[key] {
			continue
		}
		seen[key] = true

		toInsert = append(toInsert, repository.DimSecurityInput{
			Ticker:     sym.Code,
			Name:       name,
			ExchangeID: job.id,
			Type:       secType,
			Currency:   currency,
			ISIN:       isin,
		})
	}

	if len(toInsert) == 0 {
		return r
	}

	inserted, skipped, errs := s.securityRepo.BulkCreateDimSecurities(ctx, toInsert)
	r.inserted = inserted
	r.skipped = skipped
	for _, e := range errs {
		r.errors = append(r.errors, fmt.Sprintf("[%s] %v", job.code, e))
	}

	log.Debugf("[%s] inserted=%d skipped=%d badType=%d longTicker=%d",
		job.code, inserted, skipped, r.badType, r.longTick)

	return r
}

// DryRunSyncSecurities simulates SyncSecurities without making any database writes.
func (s *AdminService) DryRunSyncSecurities(ctx context.Context) (*DryRunSyncResult, error) {
	result := &DryRunSyncResult{
		UnknownAssetTypes: make(map[string]int),
		Errors:            []string{},
	}

	// Fetch exchange list.
	eohdExchanges, err := s.eodhdClient.GetExchangeList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch EODHD exchange list: %w", err)
	}

	// Pre-fetch existing exchanges.
	dbExchanges, err := s.exchangeRepo.GetAllExchanges(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load exchanges from DB: %w", err)
	}

	// Pre-fetch existing tickers.
	existingTickers, err := s.securityRepo.GetUSTickerSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing tickers: %w", err)
	}

	newExchangeSet := make(map[string]struct{})

	for _, ex := range eohdExchanges {
		if skipExchanges[ex.Code] {
			continue
		}

		dbName := ex.Code
		if alias, ok := exchangeAliases[ex.Code]; ok {
			dbName = alias
		}

		if _, exists := dbExchanges[dbName]; !exists {
			newExchangeSet[dbName] = struct{}{}
		}

		symbols, err := s.eodhdClient.GetExchangeSymbolList(ctx, ex.Code)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("[%s] failed to fetch symbols: %v", ex.Code, err))
			continue
		}

		for _, sym := range symbols {
			if len(sym.Code) > 30 {
				result.LongTickers++
				continue
			}

			_, ok := mapEODHDAssetType(sym.Type)
			if !ok {
				result.UnknownAssetTypes[strings.ToUpper(sym.Type)]++
				continue
			}

			if !existingTickers[sym.Code] {
				result.NewSecurities++
			}
		}
	}

	for name := range newExchangeSet {
		result.ExchangesNeeded = append(result.ExchangesNeeded, name)
	}

	log.Debugf("DryRun: new_securities=%d exchanges_needed=%d long_tickers=%d unknown_types=%v",
		result.NewSecurities, len(result.ExchangesNeeded), result.LongTickers, result.UnknownAssetTypes)

	return result, nil
}

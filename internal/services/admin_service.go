package services

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/repository"
	log "github.com/sirupsen/logrus"
)

// SyncSecuritiesResult contains the results of a security sync operation.
// When DryRun is true no DB writes are made; counts reflect what would happen.
// The Diagnostic* fields are only populated during a dry-run.
type SyncSecuritiesResult struct {
	DryRun             bool           `json:"dry_run"`
	SecuritiesInserted int            `json:"securities_inserted"`
	SecuritiesSkipped  int            `json:"securities_skipped"`
	SkippedBadType     int            `json:"skipped_bad_type"`
	SkippedLongTicker  int            `json:"skipped_long_ticker"`
	ExchangesCreated   []string       `json:"exchanges_created"`
	UnknownAssetTypes  map[string]int `json:"unknown_asset_types,omitempty"`
	Errors             []string       `json:"errors"`

	// Diagnostic fields — populated only for dry-run.
	DatabaseSecurities int `json:"database_securities,omitempty"` // total rows in dim_security before sync
	EODHDFetched       int `json:"eodhd_fetched,omitempty"`       // total raw symbols returned by EODHD (before any filtering)
	MissingSecurities  int `json:"missing_securities,omitempty"`  // in DB but not returned by EODHD
}

// AdminService handles administrative operations
type AdminService struct {
	securityRepo *repository.SecurityRepository
	exchangeRepo *repository.ExchangeRepository
	priceRepo    *repository.PriceRepository
	eodhdClient  providers.SecurityListFetcher
	syncWorkers  int
}

// NewAdminService creates a new AdminService. syncWorkers controls how many exchanges are
// fetched concurrently during SyncSecurities (matches the CONCURRENCY env var).
func NewAdminService(
	securityRepo *repository.SecurityRepository,
	exchangeRepo *repository.ExchangeRepository,
	priceRepo *repository.PriceRepository,
	eodhdClient providers.SecurityListFetcher,
	syncWorkers int,
) *AdminService {
	if syncWorkers <= 0 {
		syncWorkers = 10
	}
	return &AdminService{
		securityRepo: securityRepo,
		exchangeRepo: exchangeRepo,
		priceRepo:    priceRepo,
		eodhdClient:  eodhdClient,
		syncWorkers:  syncWorkers,
	}
}

// skipExchanges is the set of EODHD exchange codes that should not be imported.
var skipExchanges = map[string]bool{
	"EUFUND": true,
	"FOREX":  true,
	"CC":     true,
	"MONEY":  true,
}


// exchangeJob is sent to worker goroutines.
type exchangeJob struct {
	code    string
	dbName  string // canonical name in dim_exchanges (after alias resolution)
	country string
	id      int
}

// exchangeResult is returned by each worker goroutine.
type exchangeResult struct {
	code         string
	fetched      int // raw symbol count returned by EODHD before any filtering
	inserted     int
	skipped      int
	badType      int
	longTick     int
	unknownTypes map[string]int
	errors       []string
}

// SyncSecurities fetches all exchange symbol lists from EODHD and syncs them into dim_security.
// When dryRun is true, no DB writes are made; the result reflects what would have been inserted.
func (s *AdminService) SyncSecurities(ctx context.Context, dryRun bool) (*SyncSecuritiesResult, error) {
	result := &SyncSecuritiesResult{
		DryRun:            dryRun,
		ExchangesCreated:  []string{},
		UnknownAssetTypes: make(map[string]int),
		Errors:            []string{},
	}

	// 1. Fetch exchange list from EODHD.
	eohdExchanges, err := s.eodhdClient.GetExchangeList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch EODHD exchange list: %w", err)
	}
	log.Debugf("SyncSecurities(dryRun=%v): fetched %d exchanges from EODHD", dryRun, len(eohdExchanges))

	// 2. Pre-fetch existing exchanges from DB.
	dbExchanges, err := s.exchangeRepo.GetAllExchanges(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load exchanges from DB: %w", err)
	}

	// 3. Pre-fetch existing (ticker, exchange_id) keys so dry-run can count new securities
	//    using the same logic as the live path.
	var existingKeys map[string]bool
	if dryRun {
		byID, _, err := s.securityRepo.GetAllSecurities(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load existing securities: %w", err)
		}
		existingKeys = make(map[string]bool, len(byID))
		for _, sec := range byID {
			existingKeys[sec.Ticker+"\x00"+strconv.Itoa(sec.Exchange)] = true
		}
		result.DatabaseSecurities = len(byID)
	}

	// 4. Build job list, creating missing exchanges sequentially (live) or assigning fake
	//    negative IDs (dry-run) to avoid concurrent writes to dim_exchanges.
	jobs := make([]exchangeJob, 0, len(eohdExchanges))
	nextFakeID := -1
	for _, ex := range eohdExchanges {
		if skipExchanges[ex.Code] {
			continue
		}

		dbName := ex.Code
		if alias, ok := models.ExchangeAliases[ex.Code]; ok {
			dbName = alias
		}

		dbID, exists := dbExchanges[dbName]
		if !exists {
			result.ExchangesCreated = append(result.ExchangesCreated, dbName)
			if dryRun {
				// Assign a fake negative ID so symbols on new exchanges all look
				// new (no existingKeys entry will ever have a negative exchange ID).
				dbID = nextFakeID
				nextFakeID--
			} else {
				newID, err := s.exchangeRepo.CreateExchange(ctx, dbName, ex.Country)
				if err != nil {
					result.Errors = append(result.Errors, fmt.Sprintf("failed to create exchange %q: %v", dbName, err))
					continue
				}
				dbExchanges[dbName] = newID
				dbID = newID
				log.Debugf("Created exchange %q (country: %s, id: %d)", dbName, ex.Country, newID)
			}
		}

		jobs = append(jobs, exchangeJob{
			code:    ex.Code,
			dbName:  dbName,
			country: ex.Country,
			id:      dbID,
		})
	}

	// 5. Launch goroutine pool to fetch and process symbol lists concurrently.
	jobCh := make(chan exchangeJob, len(jobs))
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)

	resultCh := make(chan exchangeResult, len(jobs))

	var wg sync.WaitGroup
	for i := 0; i < s.syncWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				r := s.processExchange(ctx, job, dryRun, existingKeys, dbExchanges)
				resultCh <- r
			}
		}()
	}

	wg.Wait()
	close(resultCh)

	// 6. Aggregate results.
	for r := range resultCh {
		result.EODHDFetched += r.fetched
		result.SecuritiesInserted += r.inserted
		result.SecuritiesSkipped += r.skipped
		result.SkippedBadType += r.badType
		result.SkippedLongTicker += r.longTick
		for t, n := range r.unknownTypes {
			result.UnknownAssetTypes[t] += n
		}
		result.Errors = append(result.Errors, r.errors...)
	}

	if dryRun {
		// Securities in DB that EODHD didn't return at all (potential delists/acquisitions).
		result.MissingSecurities = result.DatabaseSecurities - result.SecuritiesSkipped
	}

	log.Debugf("SyncSecurities(dryRun=%v) complete: db=%d eodhd_fetched=%d inserted=%d skipped=%d missing=%d badType=%d longTicker=%d errors=%d",
		dryRun, result.DatabaseSecurities, result.EODHDFetched,
		result.SecuritiesInserted, result.SecuritiesSkipped, result.MissingSecurities,
		result.SkippedBadType, result.SkippedLongTicker, len(result.Errors))

	return result, nil
}

// processExchange fetches the symbol list for one exchange and either bulk-inserts into
// dim_security (live) or counts what would be inserted (dry-run).
func (s *AdminService) processExchange(ctx context.Context, job exchangeJob, dryRun bool, existingKeys map[string]bool, dbExchanges map[string]int) exchangeResult {
	r := exchangeResult{
		code:         job.code,
		unknownTypes: make(map[string]int),
	}

	symbols, err := s.eodhdClient.GetExchangeSymbolList(ctx, job.code)
	if err != nil {
		r.errors = append(r.errors, fmt.Sprintf("[%s] failed to fetch symbol list: %v", job.code, err))
		return r
	}
	r.fetched = len(symbols)

	seen := make(map[string]bool, len(symbols))
	toInsert := make([]repository.DimSecurityInput, 0, len(symbols))

	for _, sym := range symbols {
		if len(sym.Ticker) > 30 {
			r.longTick++
			continue
		}

		secType, ok := models.NormalizeSecurityType(sym.Type)
		if !ok {
			r.unknownTypes[strings.ToUpper(strings.TrimSpace(sym.Type))]++
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

		// Resolve the actual exchange ID from the per-symbol Exchange field.
		// EODHD aggregate exchanges (e.g. "US") list symbols from many real exchanges
		// (NYSE, NASDAQ, AMEX, PINK, …); each symbol's Exchange field tells us which.
		// Fall back to job.id if the symbol's Exchange isn't recognised.
		exchangeID := job.id
		if sym.Exchange != "" {
			lookupName := sym.Exchange
			if alias, ok := models.ExchangeAliases[lookupName]; ok {
				lookupName = alias
			}
			if id, ok := dbExchanges[lookupName]; ok && id > 0 {
				exchangeID = id
			}
		}

		key := sym.Ticker + "\x00" + fmt.Sprint(exchangeID)
		if seen[key] {
			continue
		}
		seen[key] = true

		if dryRun {
			if existingKeys[key] {
				r.skipped++
			} else {
				r.inserted++
			}
			continue
		}

		toInsert = append(toInsert, repository.DimSecurityInput{
			Ticker:     sym.Ticker,
			Name:       name,
			ExchangeID: exchangeID,
			Type:       secType,
			Currency:   currency,
			ISIN:       isin,
		})
	}

	if dryRun {
		log.Debugf("[%s] dry-run: would_insert=%d would_skip=%d badType=%d longTicker=%d",
			job.code, r.inserted, r.skipped, r.badType, r.longTick)
		return r
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

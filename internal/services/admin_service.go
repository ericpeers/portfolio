package services

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/eodhd"
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

	DelistedInserted int `json:"delisted_inserted,omitempty"`
	DelistedSkipped  int `json:"delisted_skipped,omitempty"`
	DelistedFetched  int `json:"delisted_fetched,omitempty"`

	// Diagnostic fields — populated only for dry-run.
	DatabaseSecurities int `json:"database_securities,omitempty"` // total rows in dim_security before sync
	EODHDFetched       int `json:"eodhd_fetched,omitempty"`       // total raw symbols returned by EODHD (before any filtering)
	MissingSecurities  int `json:"missing_securities,omitempty"`  // in DB but not returned by EODHD
}

// ImportFundamentalsResult summarises the result of a single ImportFundamentalsJSON call.
type ImportFundamentalsResult struct {
	Ticker             string `json:"ticker"`
	Exchange           string `json:"exchange"`
	SecurityID         int64  `json:"security_id"`
	ListingsUpserted   int    `json:"listings_upserted"`
	HistoryRowsWritten int    `json:"history_rows_written"`
}

// AdminService handles administrative operations
type AdminService struct {
	securityRepo        *repository.SecurityRepository
	exchangeRepo        *repository.ExchangeRepository
	priceRepo           *repository.PriceRepository
	fundamentalsRepo    *repository.FundamentalsRepository
	eodhdClient         providers.SecurityListFetcher
	fundamentalsFetcher providers.FundamentalsFetcher // set if eodhdClient implements the interface
	syncWorkers         int
}

// NewAdminService creates a new AdminService. syncWorkers controls how many exchanges are
// fetched concurrently during SyncSecurities (matches the CONCURRENCY env var).
func NewAdminService(
	securityRepo *repository.SecurityRepository,
	exchangeRepo *repository.ExchangeRepository,
	priceRepo *repository.PriceRepository,
	fundamentalsRepo *repository.FundamentalsRepository,
	eodhdClient providers.SecurityListFetcher,
	syncWorkers int,
) *AdminService {
	if syncWorkers <= 0 {
		syncWorkers = 10
	}
	svc := &AdminService{
		securityRepo:     securityRepo,
		exchangeRepo:     exchangeRepo,
		priceRepo:        priceRepo,
		fundamentalsRepo: fundamentalsRepo,
		eodhdClient:      eodhdClient,
		syncWorkers:      syncWorkers,
	}
	if ff, ok := eodhdClient.(providers.FundamentalsFetcher); ok {
		svc.fundamentalsFetcher = ff
	}
	return svc
}

// SortBackfillCandidates sorts candidates in-place using the backfill priority rules:
//  1. Bucket 0 (post-earnings stale) before Bucket 1 (never fetched) before Bucket 2 (oldest first).
//  2. Within each bucket: US securities first, then ETFs, then highest volume.
//
// Exported so tests can verify ordering without a database.
func SortBackfillCandidates(candidates []models.BackfillCandidate, now time.Time) {
	bucket := func(c models.BackfillCandidate) int {
		if c.NextEarnings != nil && !c.NextEarnings.After(now) &&
			(c.LastUpdate == nil || c.LastUpdate.Before(*c.NextEarnings)) {
			return 0
		}
		if c.LastUpdate == nil {
			return 1
		}
		return 2
	}
	usRank := func(c models.BackfillCandidate) int {
		if c.Country == "USA" {
			return 0
		}
		return 1
	}
	etfRank := func(c models.BackfillCandidate) int {
		if c.Type == string(models.SecurityTypeETF) {
			return 0
		}
		return 1
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		ci, cj := candidates[i], candidates[j]
		bi, bj := bucket(ci), bucket(cj)
		if bi != bj {
			return bi < bj
		}
		if bi == 2 && !ci.LastUpdate.Equal(*cj.LastUpdate) {
			return ci.LastUpdate.Before(*cj.LastUpdate)
		}
		if usRank(ci) != usRank(cj) {
			return usRank(ci) < usRank(cj)
		}
		if etfRank(ci) != etfRank(cj) {
			return etfRank(ci) < etfRank(cj)
		}
		return ci.Volume > cj.Volume
	})
}

// SelectBackfillCandidates queries all securities, attaches recent volume, sorts by priority,
// and returns the top n. No EODHD API calls are made — this is fast enough to run synchronously
// before returning a 202 to the caller.
func (s *AdminService) SelectBackfillCandidates(ctx context.Context, n int) ([]models.BackfillCandidate, error) {
	rows, err := s.fundamentalsRepo.GetBackfillCandidates(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetBackfillCandidates: %w", err)
	}

	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.SecurityID
	}

	volumes, err := s.priceRepo.GetLastVolumesBatch(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("GetLastVolumesBatch: %w", err)
	}

	candidates := make([]models.BackfillCandidate, len(rows))
	for i, r := range rows {
		candidates[i] = models.BackfillCandidate{
			SecurityID:   r.SecurityID,
			Ticker:       r.Ticker,
			ExchangeCode: r.ExchangeCode,
			Type:         r.Type,
			Country:      r.Country,
			LastUpdate:   r.LastUpdate,
			NextEarnings: r.NextEarnings,
			Volume:       volumes[r.SecurityID],
		}
	}

	SortBackfillCandidates(candidates, time.Now().UTC())

	if n < len(candidates) {
		candidates = candidates[:n]
	}
	return candidates, nil
}

// RunBackfillFundamentals fetches fundamentals from EODHD for each candidate and upserts
// the result to the database. Designed to run in a background goroutine — logs per-security
// outcomes and a final summary. Errors for individual securities are non-fatal; the run
// continues so as many securities as possible are refreshed.
func (s *AdminService) RunBackfillFundamentals(ctx context.Context, candidates []models.BackfillCandidate) {
	if s.fundamentalsFetcher == nil {
		log.Error("[backfill fundamentals] fundamentalsFetcher not available — backfill aborted")
		return
	}
	if len(candidates) == 0 {
		return
	}

	sem := make(chan struct{}, s.syncWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded, failed := 0, 0

	for _, c := range candidates {
		sem <- struct{}{}
		wg.Add(1)
		go func(c models.BackfillCandidate) {
			defer func() {
				<-sem
				wg.Done()
			}()

			pf, err := s.fundamentalsFetcher.GetFundamentals(ctx, c)
			if err != nil {
				log.Errorf("[backfill fundamentals] %s: GetFundamentals: %v", c.Ticker, err)
				mu.Lock()
				failed++
				mu.Unlock()
				return
			}
			/*log.Debugf("[backfill fundamentals] %s: parsed isin=%q cik=%q ipo=%v gic=%q/%q listings=%d history=%d",
			c.Ticker, pf.ISIN, pf.CIK, pf.IPODate, pf.GicSector, pf.GicSubIndustry,
			len(pf.Listings), len(pf.History))
			*/
			var upsertErr error
			if upsertErr = s.securityRepo.UpdateFundamentalsMeta(ctx, c.SecurityID, parsedToMetaUpdate(pf)); upsertErr == nil {
				upsertErr = s.fundamentalsRepo.UpsertFundamentals(ctx, c.SecurityID, pf.Snapshot)
			}
			if upsertErr == nil {
				upsertErr = s.fundamentalsRepo.UpsertFinancialsHistory(ctx, c.SecurityID, pf.History)
			}
			if upsertErr == nil {
				upsertErr = s.fundamentalsRepo.UpsertSecurityListings(ctx, c.SecurityID, pf.Listings)
			}
			if upsertErr != nil {
				log.Errorf("[backfill fundamentals] %s: upsert: %v", c.Ticker, upsertErr)
				mu.Lock()
				failed++
				mu.Unlock()
				return
			}

			mu.Lock()
			succeeded++
			mu.Unlock()
		}(c)
	}

	wg.Wait()
	log.Infof("[backfill fundamentals] complete: %d/%d succeeded, %d failed", succeeded, len(candidates), failed)
}

// parsedToMetaUpdate converts the provider-level ParsedFundamentals into the
// repository-level FundamentalsMetaUpdate, keeping the repository free of provider types.
func parsedToMetaUpdate(pf *providers.ParsedFundamentals) *models.FundamentalsMetaUpdate {
	return &models.FundamentalsMetaUpdate{
		CIK:             pf.CIK,
		CUSIP:           pf.CUSIP,
		LEI:             pf.LEI,
		Description:     pf.Description,
		Employees:       pf.Employees,
		CountryISO:      pf.CountryISO,
		FiscalYearEnd:   pf.FiscalYearEnd,
		GicSector:       pf.GicSector,
		GicGroup:        pf.GicGroup,
		GicIndustry:     pf.GicIndustry,
		GicSubIndustry:  pf.GicSubIndustry,
		ISIN:            pf.ISIN,
		IPODate:         pf.IPODate,
		URL:             pf.URL,
		ETFURL:          pf.ETFURL,
		NetExpenseRatio: pf.NetExpenseRatio,
		TotalAssets:     pf.TotalAssets,
		ETFYield:        pf.ETFYield,
		NAV:             pf.NAV,
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
// A concurrent pass processes all exchanges; after it completes, a second delisted pass fetches
// delisted symbols for the US virtual exchange and inserts them with delisted=true, leaving any
// already-present ticker unchanged (ON CONFLICT DO NOTHING).
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
				r := s.processExchange(ctx, job, dryRun, existingKeys, dbExchanges, false)
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

	// 7. Delisted pass — US virtual exchange only. Ignore other exchanges for now to avoid bloat.
	var usJob *exchangeJob
	for _, j := range jobs {
		if j.code == "US" {
			jCopy := j
			usJob = &jCopy
			break
		}
	}
	if usJob != nil {
		dr := s.processExchange(ctx, *usJob, dryRun, existingKeys, dbExchanges, true)
		result.DelistedInserted += dr.inserted
		result.DelistedSkipped += dr.skipped
		result.DelistedFetched = dr.fetched
		result.Errors = append(result.Errors, dr.errors...)
	}

	log.Debugf("SyncSecurities(dryRun=%v) complete: db=%d eodhd_fetched=%d inserted=%d skipped=%d missing=%d badType=%d longTicker=%d errors=%d",
		dryRun, result.DatabaseSecurities, result.EODHDFetched,
		result.SecuritiesInserted, result.SecuritiesSkipped, result.MissingSecurities,
		result.SkippedBadType, result.SkippedLongTicker, len(result.Errors))

	return result, nil
}

// processExchange fetches the symbol list for one exchange and either bulk-inserts into
// dim_security (live) or counts what would be inserted (dry-run). When delisted is true,
// fetches the delisted symbol list and marks all inserted records with delisted=true.
func (s *AdminService) processExchange(ctx context.Context, job exchangeJob, dryRun bool, existingKeys map[string]bool, dbExchanges map[string]int, delisted bool) exchangeResult {
	r := exchangeResult{
		code:         job.code,
		unknownTypes: make(map[string]int),
	}

	var symbols []providers.SymbolRecord
	var err error
	if delisted {
		symbols, err = s.eodhdClient.GetExchangeSymbolListDelisted(ctx, job.code)
	} else {
		symbols, err = s.eodhdClient.GetExchangeSymbolList(ctx, job.code)
	}
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
			Delisted:   delisted,
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

// ImportFundamentalsJSON parses raw EODHD fundamentals JSON and persists all derived
// data to the database. Resolves the security by PrimaryTicker + Exchange from the JSON.
// Returns an error if the security is not found in dim_security.
func (s *AdminService) ImportFundamentalsJSON(ctx context.Context, data []byte) (*ImportFundamentalsResult, error) {
	pf, err := eodhd.ParseFundamentalsJSON(data)
	if err != nil {
		return nil, fmt.Errorf("parse failed: %w", err)
	}

	if pf.Ticker == "" || pf.ExchangeName == "" {
		return nil, fmt.Errorf("JSON is missing General.PrimaryTicker or General.Exchange")
	}

	sec, err := s.securityRepo.GetByTickerAndExchangeName(ctx, pf.Ticker, pf.ExchangeName)
	if err != nil {
		return nil, fmt.Errorf("security %q on %q not found in dim_security: %w", pf.Ticker, pf.ExchangeName, err)
	}

	if err := s.securityRepo.UpdateFundamentalsMeta(ctx, sec.ID, parsedToMetaUpdate(pf)); err != nil {
		return nil, fmt.Errorf("UpdateFundamentalsMeta: %w", err)
	}
	if err := s.fundamentalsRepo.UpsertFundamentals(ctx, sec.ID, pf.Snapshot); err != nil {
		return nil, fmt.Errorf("UpsertFundamentals: %w", err)
	}
	if err := s.fundamentalsRepo.UpsertFinancialsHistory(ctx, sec.ID, pf.History); err != nil {
		return nil, fmt.Errorf("UpsertFinancialsHistory: %w", err)
	}
	if err := s.fundamentalsRepo.UpsertSecurityListings(ctx, sec.ID, pf.Listings); err != nil {
		return nil, fmt.Errorf("UpsertSecurityListings: %w", err)
	}

	log.Infof("[admin] imported fundamentals for %s (id=%d) on %s: %d listings, %d history rows",
		pf.Ticker, sec.ID, pf.ExchangeName, len(pf.Listings), len(pf.History))

	return &ImportFundamentalsResult{
		Ticker:             pf.Ticker,
		Exchange:           pf.ExchangeName,
		SecurityID:         sec.ID,
		ListingsUpserted:   len(pf.Listings),
		HistoryRowsWritten: len(pf.History),
	}, nil
}

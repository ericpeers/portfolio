package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

const securityCacheTTL = 1 * time.Hour

// securitySnapshot is an immutable snapshot of all securities built once and
// reused across requests. Callers read from the maps but never modify them.
// expiresAt is checked by GetAllSecurities; single-lookup callers (GetByID, etc.)
// simply treat any non-nil snapshot as valid.
type securitySnapshot struct {
	byID            map[int64]*models.Security
	byTicker        map[string][]*models.SecurityWithCountry
	byIDWithCountry map[int64]*models.SecurityWithCountry
	expiresAt       time.Time
}

var ErrSecurityNotFound = errors.New("security not found")

// developedMarketCountries is the MSCI-aligned set of developed-market country
// strings as they appear in dim_exchanges.country. Anything absent is treated as
// emerging / frontier / unknown.
var developedMarketCountries = map[string]bool{
	"USA": true, "Canada": true,
	"UK": true, "Germany": true, "France": true, "Switzerland": true,
	"Netherlands": true, "Sweden": true, "Denmark": true, "Norway": true,
	"Finland": true, "Belgium": true, "Austria": true, "Ireland": true,
	"Portugal": true, "Spain": true, "Greece": true, "Luxembourg": true, "Iceland": true,
	"Australia": true, "Japan": true, "Hong Kong": true, "Singapore": true,
	"New Zealand": true, "Israel": true, "Korea": true, "Taiwan": true,
}

// IsDevelopedMarket reports whether a country string (from dim_exchanges.country)
// is classified as a developed market. Absent entries (including the "Unkown" typo
// that exists in dim_exchanges) are treated as emerging/frontier.
func IsDevelopedMarket(country string) bool {
	return developedMarketCountries[country]
}

// SecurityRepository handles database operations for securities
type SecurityRepository struct {
	pool     *pgxpool.Pool
	snapshot atomic.Pointer[securitySnapshot]
}

// NewSecurityRepository creates a new SecurityRepository
func NewSecurityRepository(pool *pgxpool.Pool) *SecurityRepository {
	return &SecurityRepository{pool: pool}
}

// GetAllUS retrieves all securities listed on US exchanges (country = 'USA').
// Uses a read-only JOIN on dim_exchanges per the repository join exception.
func (r *SecurityRepository) GetAllUS(ctx context.Context) ([]*models.Security, error) {
	if snap := r.snapshot.Load(); snap != nil {
		seen := make(map[int64]struct{})
		var result []*models.Security
		for _, candidates := range snap.byTicker {
			for _, c := range candidates {
				if c.Country == "USA" {
					if _, dup := seen[c.ID]; !dup {
						seen[c.ID] = struct{}{}
						result = append(result, &c.Security)
					}
				}
			}
		}
		return result, nil
	}

	query := `
		SELECT ds.id, ds.ticker, ds.name, ds.exchange, ds.inception, ds.url, ds.type
		FROM dim_security ds
		JOIN dim_exchanges de ON de.id = ds.exchange
		WHERE de.country = 'USA'
	`
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query US securities: %w", err)
	}
	defer rows.Close()

	var result []*models.Security
	for rows.Next() {
		s := &models.Security{}
		if err := rows.Scan(&s.ID, &s.Ticker, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type); err != nil {
			return nil, fmt.Errorf("failed to scan security: %w", err)
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// GetByID retrieves a security by ID
func (r *SecurityRepository) GetByID(ctx context.Context, id int64) (*models.Security, error) {
	if snap := r.snapshot.Load(); snap != nil {
		s, ok := snap.byID[id]
		if ok {
			return s, nil
		}
		return nil, ErrSecurityNotFound
	}

	query := `
		SELECT id, ticker, name, exchange, inception, url, type
		FROM dim_security
		WHERE id = $1
	`
	s := &models.Security{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&s.ID, &s.Ticker, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}
	return s, nil
}

// GetByTicker retrieves a security by ticker, preferring the US listing
// when the same ticker exists on multiple exchanges.
func (r *SecurityRepository) GetByTicker(ctx context.Context, ticker string) (*models.Security, error) {
	if snap := r.snapshot.Load(); snap != nil {
		candidates, ok := snap.byTicker[ticker]
		if !ok {
			return nil, ErrSecurityNotFound
		}
		// PreferUSListing handles multi-exchange resolution.
		res := PreferUSListing(candidates)
		if res == nil {
			return nil, ErrSecurityNotFound
		}
		return res, nil
	}

	query := `
		SELECT ds.id, ds.ticker, ds.name, ds.exchange, ds.inception, ds.url, ds.type
		FROM dim_security ds
		LEFT JOIN dim_exchanges de ON de.id = ds.exchange
		WHERE ds.ticker = $1
		ORDER BY (CASE WHEN de.country = 'USA' THEN 0 ELSE 1 END), ds.id
		LIMIT 1
	`
	s := &models.Security{}
	err := r.pool.QueryRow(ctx, query, ticker).Scan(
		&s.ID, &s.Ticker, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}
	return s, nil
}

// GetUSTickerSet returns the set of all ticker symbols that have at least one
// listing on a USA exchange. Used by LoadSecurities to detect cross-exchange
// US duplicates before insertion.
func (r *SecurityRepository) GetUSTickerSet(ctx context.Context) (map[string]bool, error) {
	allUS, err := r.GetAllUS(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool, len(allUS))
	for _, s := range allUS {
		result[s.Ticker] = true
	}
	return result, nil
}


// GetAllWithCountry retrieves all securities joined with their exchange country.
// Used to build multi-exchange resolution maps.
func (r *SecurityRepository) GetAllWithCountry(ctx context.Context) ([]*models.SecurityWithCountry, error) {
	query := `
		SELECT ds.id, ds.ticker, ds.name, ds.exchange, ds.inception, ds.url, ds.type,
		       COALESCE(de.country, '') AS country,
		       COALESCE(ds.currency, '') AS currency,
		       COALESCE(de.name, '') AS exchange_name
		FROM dim_security ds
		LEFT JOIN dim_exchanges de ON de.id = ds.exchange
	`
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all securities with country: %w", err)
	}
	defer rows.Close()

	var result []*models.SecurityWithCountry
	for rows.Next() {
		s := &models.SecurityWithCountry{}
		if err := rows.Scan(&s.ID, &s.Ticker, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type, &s.Country, &s.Currency, &s.ExchangeName); err != nil {
			return nil, fmt.Errorf("failed to scan security with country: %w", err)
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// GetAllSecurities fetches all securities and returns two lookup maps:
// a by-ID map (one Security per ID) and a by-ticker slice map (all exchange
// listings per ticker) for multi-exchange resolution via PreferUSListing/OnlyUSListings.
// The returned maps are shared from an immutable snapshot — callers must not modify them.
func (r *SecurityRepository) GetAllSecurities(ctx context.Context) (map[int64]*models.Security, map[string][]*models.SecurityWithCountry, error) {
	if snap := r.snapshot.Load(); snap != nil && time.Now().Before(snap.expiresAt) {
		return snap.byID, snap.byTicker, nil
	}

	// Cache miss or TTL expired: fetch from DB and store a fresh snapshot.
	// Concurrent callers may also fetch; the last Store wins; all return valid data.
	start := time.Now()
	all, err := r.GetAllWithCountry(ctx)
	if err != nil {
		return nil, nil, err
	}
	byID := make(map[int64]*models.Security, len(all))
	byTicker := make(map[string][]*models.SecurityWithCountry, len(all))
	byIDWithCountry := make(map[int64]*models.SecurityWithCountry, len(all))
	for _, sec := range all {
		byID[sec.ID] = &sec.Security
		byTicker[sec.Ticker] = append(byTicker[sec.Ticker], sec)
		// Prefer the USA listing for FD client routing; overwrite only when upgrading to USA.
		if existing, ok := byIDWithCountry[sec.ID]; !ok || (existing.Country != "USA" && sec.Country == "USA") {
			byIDWithCountry[sec.ID] = sec
		}
	}
	r.snapshot.Store(&securitySnapshot{byID: byID, byTicker: byTicker, byIDWithCountry: byIDWithCountry, expiresAt: time.Now().Add(securityCacheTTL)})
	log.Debugf("GetAllSecurities (DB) took: %v", time.Since(start))
	return byID, byTicker, nil
}

// GetByIDWithCountry retrieves a security by ID, joined with its exchange country and name.
// Used by PricingService to obtain routing metadata for the FD client.
func (r *SecurityRepository) GetByIDWithCountry(ctx context.Context, id int64) (*models.SecurityWithCountry, error) {
	if snap := r.snapshot.Load(); snap != nil {
		s, ok := snap.byIDWithCountry[id]
		if ok {
			return s, nil
		}
		return nil, ErrSecurityNotFound
	}

	query := `
		SELECT ds.id, ds.ticker, ds.name, ds.exchange, ds.inception, ds.url, ds.type,
		       COALESCE(de.country, '') AS country,
		       COALESCE(ds.currency, '') AS currency,
		       COALESCE(de.name, '') AS exchange_name
		FROM dim_security ds
		LEFT JOIN dim_exchanges de ON de.id = ds.exchange
		WHERE ds.id = $1
	`
	s := &models.SecurityWithCountry{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&s.ID, &s.Ticker, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type,
		&s.Country, &s.Currency, &s.ExchangeName,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security with country: %w", err)
	}
	return s, nil
}

// PreferUSListing returns the most US-like listing, in priority order:
//  1. country = "USA" (strongest signal — exchange is in the US)
//  2. currency = "USD" with exactly one such candidate (secondary signal — USD-denominated
//     listing when the exchange mapping doesn't carry a USA country; e.g. EME listed on
//     NYSE, ASX, and LSE where the DB exchange lacks a country tag)
//  3. The only candidate when there is no ambiguity
//  4. nil when multiple candidates exist with no way to disambiguate
func PreferUSListing(candidates []*models.SecurityWithCountry) *models.Security {
	if len(candidates) == 0 {
		return nil
	}
	var us []*models.SecurityWithCountry
	for _, c := range candidates {
		if c.Country == "USA" {
			us = append(us, c)
		}
	}
	if len(us) >= 1 {
		return &us[0].Security
	}
	// No country=USA listing — fall back to USD currency as a secondary signal.
	// Handles cases where the exchange record lacks a country tag but the security
	// is denominated in USD (strong proxy for a US or USD-primary listing).
	var usd []*models.SecurityWithCountry
	for _, c := range candidates {
		if c.Currency == "USD" {
			usd = append(usd, c)
		}
	}
	if len(usd) == 1 {
		return &usd[0].Security
	}
	// Step 3: Among remaining candidates, prefer developed-market over emerging.
	// Handles the rare case of multiple USD listings or no USD listing with
	// multiple candidates where no country=USA exists.
	developed, _ := partitionByMarketTier(candidates)
	if len(developed) == 1 {
		return &developed[0].Security
	}
	// Step 4: single candidate fallback — no other disambiguation possible
	if len(candidates) == 1 {
		return &candidates[0].Security
	}
	return nil
}

// OnlyUSListings filters candidates to those listed on USA exchanges.
func OnlyUSListings(candidates []*models.SecurityWithCountry) []*models.SecurityWithCountry {
	var result []*models.SecurityWithCountry
	for _, c := range candidates {
		if c.Country == "USA" {
			result = append(result, c)
		}
	}
	return result
}

// ShouldPreferNonUSForETF returns true when an ETF's metadata suggests its holdings
// are local-exchange securities rather than US-listed stocks or ADRs.
//
// Rules (in priority order):
//  1. Override → false: name contains a major US equity index keyword
//     ("S&P 500", "S&P500", "NASDAQ", "DOW JONES", "RUSSELL")
//  2. Strong → true: ETF currency is non-USD and non-empty
//  3. Strong → true: name contains an explicit ex-US marker
//     ("EX US", "EX-US", "EX UNITED STATES", "EXCLUDING US")
//  4. Medium → true: name contains a geographic-focus keyword
//     ("EMERGING", "DEVELOPED", "INTERNATIONAL") — applies regardless of
//     whether the ETF is US-listed, since US-listed international funds
//     (AVEM, VWO, IEMG, VEA, VXUS, etc.) hold local-market shares, not ADRs
//  5. Default → false: assume US listings
func ShouldPreferNonUSForETF(etf *models.SecurityWithCountry) bool {
	name := strings.ToUpper(etf.Name)

	// Rule 1 (override): tracks a US equity index — holdings are US-listed
	for _, usIndex := range []string{"S&P 500", "S&P500", "NASDAQ", "DOW JONES", "RUSSELL"} {
		if strings.Contains(name, usIndex) {
			return false
		}
	}

	// Rule 2 (strong): non-USD currency implies non-US ETF
	if etf.Currency != "" && etf.Currency != "USD" {
		return true
	}

	// Rule 3 (strong): explicit ex-US branding
	for _, kw := range []string{"EX US", "EX-US", "EX UNITED STATES", "EXCLUDING US"} {
		if strings.Contains(name, kw) {
			return true
		}
	}

	// Rule 4 (medium): geographic-focus keyword — US-listed international funds
	// hold local-market shares (not ADRs), so the country check is not needed.
	for _, kw := range []string{"EMERGING", "DEVELOPED", "INTERNATIONAL"} {
		if strings.Contains(name, kw) {
			return true
		}
	}

	return false
}

// IsEmergingMarketsETF reports whether an ETF's name indicates it targets
// emerging or frontier markets. Only meaningful when ShouldPreferNonUSForETF
// has already returned true for the same ETF.
func IsEmergingMarketsETF(etf *models.SecurityWithCountry) bool {
	name := strings.ToUpper(etf.Name)
	for _, kw := range []string{"EMERGING", "FRONTIER"} {
		if strings.Contains(name, kw) {
			return true
		}
	}
	return false
}

// partitionByMarketTier splits candidates into developed-market and other
// (emerging / frontier / unknown) buckets using IsDevelopedMarket.
func partitionByMarketTier(candidates []*models.SecurityWithCountry) (developed, other []*models.SecurityWithCountry) {
	for _, c := range candidates {
		if IsDevelopedMarket(c.Country) {
			developed = append(developed, c)
		} else {
			other = append(other, c)
		}
	}
	return
}

// PreferDevelopedNonUSListing resolves the best listing for a holding inside a
// developed-world ex-US ETF (e.g. SPDW, AVDV, EFG).  Priority:
//  1. First developed non-US listing (Canada, UK, Germany, Australia, …)
//  2. First emerging / frontier non-US listing (no developed non-US available)
//  3. US listing as last resort (no non-US listings at all)
//  4. nil only when candidates is empty
//
// Never returns nil when at least one candidate exists.
func PreferDevelopedNonUSListing(candidates []*models.SecurityWithCountry) *models.Security {
	if len(candidates) == 0 {
		return nil
	}
	var nonUS []*models.SecurityWithCountry
	for _, c := range candidates {
		if c.Country != "USA" {
			nonUS = append(nonUS, c)
		}
	}
	if len(nonUS) > 0 {
		developed, other := partitionByMarketTier(nonUS)
		if len(developed) > 0 {
			return &developed[0].Security // first developed non-US wins
		}
		return &other[0].Security // no developed non-US → first emerging
	}
	return &candidates[0].Security // no non-US at all → US listing as last resort
}

// PreferEmergingNonUSListing resolves the best listing for a holding inside an
// emerging-market ETF (e.g. AVEM, VWO, IEMG).  Priority:
//  1. First emerging / frontier non-US listing
//  2. First developed non-US listing (no emerging available)
//  3. US listing as last resort (no non-US listings at all)
//  4. nil only when candidates is empty
//
// Never returns nil when at least one candidate exists.
func PreferEmergingNonUSListing(candidates []*models.SecurityWithCountry) *models.Security {
	if len(candidates) == 0 {
		return nil
	}
	var nonUS []*models.SecurityWithCountry
	for _, c := range candidates {
		if c.Country != "USA" {
			nonUS = append(nonUS, c)
		}
	}
	if len(nonUS) > 0 {
		developed, other := partitionByMarketTier(nonUS)
		if len(other) > 0 {
			return &other[0].Security // emerging wins for EM ETFs
		}
		return &developed[0].Security // no emerging non-US → first developed
	}
	return &candidates[0].Security // no non-US at all → US listing as last resort
}

// GetETFMembership retrieves the holdings of an ETF from dim_etf_membership
func (r *SecurityRepository) GetETFMembership(ctx context.Context, etfID int64) ([]models.ETFMembership, error) {
	query := `
		SELECT dim_security_id, dim_composite_id, percentage
		FROM dim_etf_membership
		WHERE dim_composite_id = $1
	`
	rows, err := r.pool.Query(ctx, query, etfID)
	if err != nil {
		return nil, fmt.Errorf("failed to query ETF memberships: %w", err)
	}
	defer rows.Close()

	var memberships []models.ETFMembership
	for rows.Next() {
		var m models.ETFMembership
		if err := rows.Scan(&m.SecurityID, &m.ETFID, &m.Percentage); err != nil {
			return nil, fmt.Errorf("failed to scan ETF membership: %w", err)
		}
		memberships = append(memberships, m)
	}
	return memberships, rows.Err()
}

// UpsertETFMembership inserts or updates ETF holdings in dim_etf_membership and dim_etf_pull_range
func (r *SecurityRepository) UpsertETFMembership(ctx context.Context, tx pgx.Tx, etfID int64, holdings []models.ETFMembership, nextUpdate time.Time) error {
	// Delete existing holdings for this ETF
	deleteQuery := `DELETE FROM dim_etf_membership WHERE dim_composite_id = $1`
	if _, err := tx.Exec(ctx, deleteQuery, etfID); err != nil {
		return fmt.Errorf("failed to delete existing ETF memberships: %w", err)
	}

	// Insert new holdings in a single batch round-trip
	if len(holdings) > 0 {
		insertQuery := `
			INSERT INTO dim_etf_membership (dim_security_id, dim_composite_id, percentage)
			VALUES ($1, $2, $3)
		`
		batch := &pgx.Batch{}
		for _, h := range holdings {
			batch.Queue(insertQuery, h.SecurityID, etfID, h.Percentage)
		}
		br := tx.SendBatch(ctx, batch)
		for i, h := range holdings {
			if _, err := br.Exec(); err != nil {
				br.Close() // #nosec G104 -- already in error path, Close error discarded (idiomatic Go)
				return fmt.Errorf("failed to insert ETF membership %d (security %d): %w", i, h.SecurityID, err)
			}
		}
		br.Close() // #nosec G104 -- batch results already processed, Close error discarded (idiomatic Go)
	}

	// Update the pull range tracking
	pullRangeQuery := `
		INSERT INTO dim_etf_pull_range (composite_id, pull_date, next_update)
		VALUES ($1, $2, $3)
		ON CONFLICT (composite_id) DO UPDATE SET
			pull_date = EXCLUDED.pull_date,
			next_update = EXCLUDED.next_update
	`
	nyLoc, _ := time.LoadLocation("America/New_York")
	nyNow := time.Now().In(nyLoc)
	pullDate := time.Date(nyNow.Year(), nyNow.Month(), nyNow.Day(), 0, 0, 0, 0, nyLoc)
	if _, err := tx.Exec(ctx, pullRangeQuery, etfID, pullDate, nextUpdate); err != nil {
		return fmt.Errorf("failed to upsert ETF pull range: %w", err)
	}

	return nil
}

// ETFPullRange represents the pull range metadata for an ETF
type ETFPullRange struct {
	CompositeID int64
	PullDate    time.Time
	NextUpdate  time.Time
}

// GetETFPullRange returns the pull range for an ETF (when data was last fetched and next expected update)
func (r *SecurityRepository) GetETFPullRange(ctx context.Context, etfID int64) (*ETFPullRange, error) {
	query := `
		SELECT composite_id, pull_date, next_update
		FROM dim_etf_pull_range
		WHERE composite_id = $1
	`
	var pr ETFPullRange
	err := r.pool.QueryRow(ctx, query, etfID).Scan(&pr.CompositeID, &pr.PullDate, &pr.NextUpdate)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get ETF pull range: %w", err)
	}
	return &pr, nil
}

// GetETFPullRanges returns pull range metadata for all requested ETF IDs in one query.
// ETFs not found in dim_etf_pull_range are absent from the returned map.
func (r *SecurityRepository) GetETFPullRanges(ctx context.Context, etfIDs []int64) (map[int64]*ETFPullRange, error) {
	if len(etfIDs) == 0 {
		return map[int64]*ETFPullRange{}, nil
	}
	query := `
		SELECT composite_id, pull_date, next_update
		FROM dim_etf_pull_range
		WHERE composite_id = ANY($1)
	`
	rows, err := r.pool.Query(ctx, query, etfIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to batch-query ETF pull ranges: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]*ETFPullRange, len(etfIDs))
	for rows.Next() {
		pr := &ETFPullRange{}
		if err := rows.Scan(&pr.CompositeID, &pr.PullDate, &pr.NextUpdate); err != nil {
			return nil, fmt.Errorf("failed to scan ETF pull range: %w", err)
		}
		result[pr.CompositeID] = pr
	}
	return result, rows.Err()
}

// GetETFMemberships returns holdings for all requested ETF IDs in one query,
// grouped by ETF ID. ETFs with no holdings are absent from the returned map.
func (r *SecurityRepository) GetETFMemberships(ctx context.Context, etfIDs []int64) (map[int64][]models.ETFMembership, error) {
	if len(etfIDs) == 0 {
		return map[int64][]models.ETFMembership{}, nil
	}
	query := `
		SELECT dim_security_id, dim_composite_id, percentage
		FROM dim_etf_membership
		WHERE dim_composite_id = ANY($1)
	`
	rows, err := r.pool.Query(ctx, query, etfIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to batch-query ETF memberships: %w", err)
	}
	defer rows.Close()

	result := make(map[int64][]models.ETFMembership)
	for rows.Next() {
		var m models.ETFMembership
		if err := rows.Scan(&m.SecurityID, &m.ETFID, &m.Percentage); err != nil {
			return nil, fmt.Errorf("failed to scan ETF membership: %w", err)
		}
		result[m.ETFID] = append(result[m.ETFID], m)
	}
	return result, rows.Err()
}

// GetMultipleByTickers retrieves multiple securities by their ticker symbols.
// Returns a map from ticker to all exchange listings for that ticker so callers
// can apply their own resolution strategy (e.g. PreferUSListing, OnlyUSListings).
func (r *SecurityRepository) GetMultipleByTickers(ctx context.Context, tickers []string) (map[string][]*models.SecurityWithCountry, error) {
	if len(tickers) == 0 {
		return make(map[string][]*models.SecurityWithCountry), nil
	}

	if snap := r.snapshot.Load(); snap != nil {
		result := make(map[string][]*models.SecurityWithCountry, len(tickers))
		for _, ticker := range tickers {
			if candidates, ok := snap.byTicker[ticker]; ok {
				result[ticker] = candidates
			}
		}
		return result, nil
	}

	query := `
		SELECT ds.id, ds.ticker, ds.name, ds.exchange, ds.inception, ds.url, ds.type,
		       COALESCE(de.country, '') AS country,
		       COALESCE(ds.currency, '') AS currency,
		       COALESCE(de.name, '') AS exchange_name
		FROM dim_security ds
		LEFT JOIN dim_exchanges de ON de.id = ds.exchange
		WHERE ds.ticker = ANY($1)
	`
	rows, err := r.pool.Query(ctx, query, tickers)
	if err != nil {
		return nil, fmt.Errorf("failed to query securities by tickers: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]*models.SecurityWithCountry)
	for rows.Next() {
		s := &models.SecurityWithCountry{}
		if err := rows.Scan(&s.ID, &s.Ticker, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type, &s.Country, &s.Currency, &s.ExchangeName); err != nil {
			return nil, fmt.Errorf("failed to scan security: %w", err)
		}
		result[s.Ticker] = append(result[s.Ticker], s)
	}
	return result, rows.Err()
}

// BeginTx starts a new transaction
func (r *SecurityRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

// DimSecurityInput represents input for bulk security creation
type DimSecurityInput struct {
	Ticker     string
	Name       string
	ExchangeID int
	Type       string
	Inception  *time.Time
	Currency   *string // nullable VARCHAR(3)
	ISIN       *string // nullable VARCHAR(12)
}

// FindExistingForDryRun queries which of the given inputs already exist in
// dim_security and returns their current ISIN values. Used by the dry-run path
// of LoadSecurities to compute accurate would-insert / would-skip / would-update-isin
// counts without writing anything.
//
// Inputs with ExchangeID == 0 (new exchanges that don't exist yet) are excluded
// from the query; they are guaranteed to be new and are counted as inserts.
//
// Returns:
//   - existingKeys: "ticker|exchangeID" pairs that already exist
//   - existingISINs: same key → current isin value (nil if NULL in DB)
func (r *SecurityRepository) FindExistingForDryRun(
	ctx context.Context,
	securities []DimSecurityInput,
) (existingKeys map[string]bool, existingISINs map[string]*string, err error) {
	existingKeys = make(map[string]bool)
	existingISINs = make(map[string]*string)

	// Collect unique tickers and exchange IDs, skipping placeholder 0 IDs.
	tickerSet := make(map[string]struct{})
	exchSet := make(map[int]struct{})
	for _, s := range securities {
		if s.ExchangeID == 0 {
			continue
		}
		tickerSet[s.Ticker] = struct{}{}
		exchSet[s.ExchangeID] = struct{}{}
	}
	if len(tickerSet) == 0 {
		return existingKeys, existingISINs, nil
	}

	tickers := make([]string, 0, len(tickerSet))
	for t := range tickerSet {
		tickers = append(tickers, t)
	}
	exchIDs := make([]int, 0, len(exchSet))
	for e := range exchSet {
		exchIDs = append(exchIDs, e)
	}

	// Over-fetch by ticker+exchange (cross-product), then filter in-memory for
	// exact pairs. This is correct because most tickers appear on only one exchange.
	query := `
		SELECT ticker, exchange, isin
		FROM dim_security
		WHERE ticker = ANY($1::text[]) AND exchange = ANY($2::int[])
	`
	rows, queryErr := r.pool.Query(ctx, query, tickers, exchIDs)
	if queryErr != nil {
		return nil, nil, fmt.Errorf("FindExistingForDryRun: %w", queryErr)
	}
	defer rows.Close()

	for rows.Next() {
		var ticker string
		var exchangeID int
		var isin *string
		if scanErr := rows.Scan(&ticker, &exchangeID, &isin); scanErr != nil {
			return nil, nil, fmt.Errorf("FindExistingForDryRun scan: %w", scanErr)
		}
		key := fmt.Sprintf("%s|%d", ticker, exchangeID)
		existingKeys[key] = true
		existingISINs[key] = isin
	}
	return existingKeys, existingISINs, rows.Err()
}

// ClearCache invalidates the in-memory securities cache.
func (r *SecurityRepository) ClearCache() {
	r.snapshot.Store(nil)
	log.Debug("Securities cache cleared")
}

// UpdateISINsForExisting batch-updates the isin column on securities that
// already exist in dim_security and whose ISIN in the CSV differs from what
// is stored (or was null). Newly-inserted securities already carry the correct
// ISIN from BulkCreateDimSecurities and are not re-updated.
// Returns the count of rows actually changed.
func (r *SecurityRepository) UpdateISINsForExisting(
	ctx context.Context,
	securities []DimSecurityInput,
) (updated int, errs []error) {
	defer r.ClearCache() // Invalidate cache after update
	query := `
		UPDATE dim_security
		SET isin = $1
		WHERE ticker = $2 AND exchange = $3
		  AND isin IS DISTINCT FROM $1
	`
	batch := &pgx.Batch{}
	var eligible []DimSecurityInput
	for _, s := range securities {
		if s.ISIN != nil && *s.ISIN != "" {
			batch.Queue(query, s.ISIN, s.Ticker, s.ExchangeID)
			eligible = append(eligible, s)
		}
	}
	if len(eligible) == 0 {
		return 0, nil
	}

	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()

	for _, s := range eligible {
		ct, err := br.Exec()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to update ISIN for %s: %w", s.Ticker, err))
			continue
		}
		updated += int(ct.RowsAffected())
	}
	return updated, errs
}

// SetInceptionDate sets the inception date on a single security by ID.
// It clears the in-memory cache since inception is part of the Security struct.
func (r *SecurityRepository) SetInceptionDate(ctx context.Context, securityID int64, date time.Time) error {
	defer r.ClearCache()
	_, err := r.pool.Exec(ctx,
		`UPDATE dim_security SET inception = $1 WHERE id = $2`,
		date, securityID,
	)
	if err != nil {
		return fmt.Errorf("failed to set inception date for security id=%d: %w", securityID, err)
	}
	return nil
}

// BulkCreateDimSecurities inserts multiple securities using batch operations.
// Returns the count of inserted and skipped securities, plus any errors.
func (r *SecurityRepository) BulkCreateDimSecurities(
	ctx context.Context,
	securities []DimSecurityInput,
) (inserted int, skipped int, errs []error) {
	if len(securities) == 0 {
		return 0, 0, nil
	}
	defer r.ClearCache() // Invalidate cache after bulk insert

	query := `
		INSERT INTO dim_security (ticker, name, exchange, type, inception, currency, isin)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT DO NOTHING
		RETURNING id
	`

	batch := &pgx.Batch{}
	for _, s := range securities {
		batch.Queue(query, s.Ticker, s.Name, s.ExchangeID, s.Type, s.Inception, s.Currency, s.ISIN)
	}

	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i, s := range securities {
		var id int64
		err := br.QueryRow().Scan(&id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Conflict occurred, ticker already exists - this is a skip, not an error
				skipped++
				continue
			}
			errs = append(errs, fmt.Errorf("failed to insert security %d (%s): %w", i, s.Ticker, err))
			continue
		}
		inserted++
	}

	return inserted, skipped, errs
}

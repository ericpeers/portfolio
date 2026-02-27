package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
	pool *pgxpool.Pool
}

// NewSecurityRepository creates a new SecurityRepository
func NewSecurityRepository(pool *pgxpool.Pool) *SecurityRepository {
	return &SecurityRepository{pool: pool}
}

// GetAll retrieves all securities from dim_security
func (r *SecurityRepository) GetAll(ctx context.Context) ([]*models.Security, error) {
	query := `
		SELECT id, ticker, name, exchange, inception, url, type
		FROM dim_security
	`
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query all securities: %w", err)
	}
	defer rows.Close()

	var result []*models.Security
	for rows.Next() {
		s := &models.Security{}
		if err := rows.Scan(&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type); err != nil {
			return nil, fmt.Errorf("failed to scan security: %w", err)
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// GetAllUS retrieves all securities listed on US exchanges (country = 'USA').
// Uses a read-only JOIN on dim_exchanges per the repository join exception.
func (r *SecurityRepository) GetAllUS(ctx context.Context) ([]*models.Security, error) {
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
		if err := rows.Scan(&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type); err != nil {
			return nil, fmt.Errorf("failed to scan security: %w", err)
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// GetByID retrieves a security by ID
func (r *SecurityRepository) GetByID(ctx context.Context, id int64) (*models.Security, error) {
	query := `
		SELECT id, ticker, name, exchange, inception, url, type
		FROM dim_security
		WHERE id = $1
	`
	s := &models.Security{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}
	return s, nil
}

// GetBySymbol retrieves a security by symbol (ticker), preferring the US listing
// when the same ticker exists on multiple exchanges.
func (r *SecurityRepository) GetBySymbol(ctx context.Context, symbol string) (*models.Security, error) {
	query := `
		SELECT ds.id, ds.ticker, ds.name, ds.exchange, ds.inception, ds.url, ds.type
		FROM dim_security ds
		LEFT JOIN dim_exchanges de ON de.id = ds.exchange
		WHERE ds.ticker = $1
		ORDER BY (CASE WHEN de.country = 'USA' THEN 0 ELSE 1 END), ds.id
		LIMIT 1
	`
	s := &models.Security{}
	err := r.pool.QueryRow(ctx, query, symbol).Scan(
		&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSecurityNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get security: %w", err)
	}
	return s, nil
}

// GetByTicker retrieves a security by ticker, preferring the US listing.
// Deprecated: use GetBySymbol which is identical.
func (r *SecurityRepository) GetByTicker(ctx context.Context, ticker string) (*models.Security, error) {
	return r.GetBySymbol(ctx, ticker)
}

// GetAllWithCountry retrieves all securities joined with their exchange country.
// Used to build multi-exchange resolution maps.
func (r *SecurityRepository) GetAllWithCountry(ctx context.Context) ([]*models.SecurityWithCountry, error) {
	query := `
		SELECT ds.id, ds.ticker, ds.name, ds.exchange, ds.inception, ds.url, ds.type,
		       COALESCE(de.country, '') AS country,
		       COALESCE(ds.currency, '') AS currency
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
		if err := rows.Scan(&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type, &s.Country, &s.Currency); err != nil {
			return nil, fmt.Errorf("failed to scan security with country: %w", err)
		}
		result = append(result, s)
	}
	return result, rows.Err()
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

// PreferNonUSListing returns a non-US listing if one exists; falls back to the single
// candidate when there is no ambiguity; returns nil when candidates is empty or
// multiple non-US listings exist with no way to disambiguate.
//
// Deprecated: use PreferDevelopedNonUSListing or PreferEmergingNonUSListing instead.
// Those functions never return nil when candidates exist and apply market-tier
// ordering so the most-appropriate non-US listing is chosen.
func PreferNonUSListing(candidates []*models.SecurityWithCountry) *models.Security {
	if len(candidates) == 0 {
		return nil
	}
	var nonUS []*models.SecurityWithCountry
	for _, c := range candidates {
		if c.Country != "USA" {
			nonUS = append(nonUS, c)
		}
	}
	if len(nonUS) == 1 {
		return &nonUS[0].Security
	}
	// Multiple non-US listings — ambiguous, no winner
	if len(nonUS) > 1 {
		return nil
	}
	// No non-US listing — fall back to the only candidate if unambiguous
	if len(candidates) == 1 {
		return &candidates[0].Security
	}
	return nil
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

// IsETFOrMutualFund checks if a security is an ETF or mutual fund
func (r *SecurityRepository) IsETFOrMutualFund(ctx context.Context, securityID int64) (bool, error) {
	query := `
		SELECT type
		FROM dim_security
		WHERE id = $1
	`
	var secType string
	err := r.pool.QueryRow(ctx, query, securityID).Scan(&secType)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrSecurityNotFound
	}
	if err != nil {
		return false, fmt.Errorf("failed to check security type: %w", err)
	}
	return secType == string(models.SecurityTypeETF) || secType == string(models.SecurityTypeMutualFund), nil
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

	// Insert new holdings
	insertQuery := `
		INSERT INTO dim_etf_membership (dim_security_id, dim_composite_id, percentage)
		VALUES ($1, $2, $3)
	`
	for _, h := range holdings {
		if _, err := tx.Exec(ctx, insertQuery, h.SecurityID, etfID, h.Percentage); err != nil {
			return fmt.Errorf("failed to insert ETF membership: %w", err)
		}
	}

	// Update the pull range tracking
	pullRangeQuery := `
		INSERT INTO dim_etf_pull_range (composite_id, pull_date, next_update)
		VALUES ($1, $2, $3)
		ON CONFLICT (composite_id) DO UPDATE SET
			pull_date = EXCLUDED.pull_date,
			next_update = EXCLUDED.next_update
	`
	if _, err := tx.Exec(ctx, pullRangeQuery, etfID, time.Now().UTC().Truncate(24*time.Hour), nextUpdate); err != nil {
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

// GetMultipleBySymbols retrieves multiple securities by their ticker symbols.
// Returns a map from ticker to all exchange listings for that ticker so callers
// can apply their own resolution strategy (e.g. PreferUSListing, OnlyUSListings).
func (r *SecurityRepository) GetMultipleBySymbols(ctx context.Context, symbols []string) (map[string][]*models.SecurityWithCountry, error) {
	if len(symbols) == 0 {
		return make(map[string][]*models.SecurityWithCountry), nil
	}

	query := `
		SELECT ds.id, ds.ticker, ds.name, ds.exchange, ds.inception, ds.url, ds.type,
		       COALESCE(de.country, '') AS country,
		       COALESCE(ds.currency, '') AS currency
		FROM dim_security ds
		LEFT JOIN dim_exchanges de ON de.id = ds.exchange
		WHERE ds.ticker = ANY($1)
	`
	rows, err := r.pool.Query(ctx, query, symbols)
	if err != nil {
		return nil, fmt.Errorf("failed to query securities by symbols: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]*models.SecurityWithCountry)
	for rows.Next() {
		s := &models.SecurityWithCountry{}
		if err := rows.Scan(&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type, &s.Country, &s.Currency); err != nil {
			return nil, fmt.Errorf("failed to scan security: %w", err)
		}
		result[s.Symbol] = append(result[s.Symbol], s)
	}
	return result, rows.Err()
}

// GetMultipleByIDs retrieves multiple securities by their IDs
func (r *SecurityRepository) GetMultipleByIDs(ctx context.Context, ids []int64) (map[int64]*models.Security, error) {
	if len(ids) == 0 {
		return make(map[int64]*models.Security), nil
	}

	query := `
		SELECT id, ticker, name, exchange, inception, url, type
		FROM dim_security
		WHERE id = ANY($1)
	`
	rows, err := r.pool.Query(ctx, query, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to query securities: %w", err)
	}
	defer rows.Close()

	result := make(map[int64]*models.Security)
	for rows.Next() {
		s := &models.Security{}
		if err := rows.Scan(&s.ID, &s.Symbol, &s.Name, &s.Exchange, &s.Inception, &s.URL, &s.Type); err != nil {
			return nil, fmt.Errorf("failed to scan security: %w", err)
		}
		result[s.ID] = s
	}
	return result, rows.Err()
}

// UpdateInceptionDate sets the inception date for an existing security
func (r *SecurityRepository) UpdateInceptionDate(ctx context.Context, id int64, inception *time.Time) error {
	query := `UPDATE dim_security SET inception = $1 WHERE id = $2`
	_, err := r.pool.Exec(ctx, query, inception, id)
	if err != nil {
		return fmt.Errorf("failed to update inception date for security %d: %w", id, err)
	}
	return nil
}

// BeginTx starts a new transaction
func (r *SecurityRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

// CreateDimSecurity inserts a security into dim_security if it doesn't exist
// Returns (id, wasCreated, error)
func (r *SecurityRepository) CreateDimSecurity(
	ctx context.Context,
	ticker, name string,
	exchangeID int,
	secType string,
	inception *time.Time,
) (int64, bool, error) {
	query := `
		INSERT INTO dim_security (ticker, name, exchange, type, inception)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
		RETURNING id
	`
	//placing ON CONFLICT (only_one_ticker_per_exchange) DO NOTHING
	//results in SQL errs even though there is a constraint that prevent dupes from inserting.
	//I think it only works on index column names.
	var id int64
	err := r.pool.QueryRow(ctx, query, ticker, name, exchangeID, secType, inception).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Conflict occurred, ticker already exists
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("failed to insert dim_security: %w", err)
	}

	return id, true, nil
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

// UpdateISINsForExisting batch-updates the isin column on securities that
// already exist in dim_security and whose ISIN in the CSV differs from what
// is stored (or was null). Newly-inserted securities already carry the correct
// ISIN from BulkCreateDimSecurities and are not re-updated.
// Returns the count of rows actually changed.
func (r *SecurityRepository) UpdateISINsForExisting(
	ctx context.Context,
	securities []DimSecurityInput,
) (updated int, errs []error) {
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

// BulkCreateDimSecurities inserts multiple securities using batch operations.
// Returns the count of inserted and skipped securities, plus any errors.
func (r *SecurityRepository) BulkCreateDimSecurities(
	ctx context.Context,
	securities []DimSecurityInput,
) (inserted int, skipped int, errs []error) {
	if len(securities) == 0 {
		return 0, 0, nil
	}

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

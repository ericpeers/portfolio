package repository

import (
	"context"
	"errors"
	"time"

	"github.com/epeers/portfolio/internal/providers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

// FundamentalsRepository owns fact_fundamentals, fact_financials_history, and dim_security_listings.
// It does NOT own dim_security — use SecurityRepository.UpdateFundamentalsMeta for those columns.
type FundamentalsRepository struct {
	pool *pgxpool.Pool
}

// NewFundamentalsRepository creates a new FundamentalsRepository.
func NewFundamentalsRepository(pool *pgxpool.Pool) *FundamentalsRepository {
	return &FundamentalsRepository{pool: pool}
}

// FundamentalsScheduleRow is returned by GetAllScheduleRows and contains the fields
// the service layer needs to decide whether a security requires a fundamentals fetch.
// Scheduling logic (round-robin interval, post-earnings priority) lives in Go, not the DB.
type FundamentalsScheduleRow struct {
	SecurityID           int64
	LastUpdate           *time.Time // nil = never fetched
	NextEarningsAnnounce *time.Time // nil = unknown
}

// BackfillCandidateRow contains all fields the service layer needs to select and sort
// securities for a fundamentals backfill run.
type BackfillCandidateRow struct {
	SecurityID   int64
	Ticker       string
	ExchangeCode string     // EODHD exchange code: "US" for any USA-country exchange, otherwise dim_exchanges.name
	Type         string     // ds_type enum string, e.g. "COMMON STOCK", "ETF"
	Country      string     // dim_exchanges.country, e.g. "USA"
	LastUpdate   *time.Time // nil = never fetched
	NextEarnings *time.Time // nil = unknown
}

// GetBackfillCandidates returns all non-INDEX securities with the metadata needed for
// backfill priority sorting. Includes securities with no fact_fundamentals row (LastUpdate nil).
func (r *FundamentalsRepository) GetBackfillCandidates(ctx context.Context) ([]BackfillCandidateRow, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT ds.id, ds.ticker, ds.type, de.country,
		       CASE WHEN de.country = 'USA' THEN 'US' ELSE de.name END AS exchange_code,
		       ff.last_update, ff.next_earnings_announce
		FROM dim_security ds
		JOIN dim_exchanges de ON de.id = ds.exchange
		LEFT JOIN fact_fundamentals ff ON ff.security_id = ds.id
		WHERE ds.type != 'INDEX'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []BackfillCandidateRow
	for rows.Next() {
		var row BackfillCandidateRow
		if err := rows.Scan(
			&row.SecurityID, &row.Ticker, &row.Type, &row.Country, &row.ExchangeCode,
			&row.LastUpdate, &row.NextEarnings,
		); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// GetAllScheduleRows returns last_update and next_earnings_announce for every security,
// including those with no row in fact_fundamentals (LastUpdate will be nil).
// The service layer applies scheduling policy and selects which IDs to fetch.
func (r *FundamentalsRepository) GetAllScheduleRows(ctx context.Context) ([]FundamentalsScheduleRow, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT s.id, ff.last_update, ff.next_earnings_announce
		FROM dim_security s
		LEFT JOIN fact_fundamentals ff ON ff.security_id = s.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []FundamentalsScheduleRow
	for rows.Next() {
		var row FundamentalsScheduleRow
		if err := rows.Scan(&row.SecurityID, &row.LastUpdate, &row.NextEarningsAnnounce); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// UpsertFundamentals writes or replaces the fact_fundamentals snapshot for one security.
// Sets last_update to now; next_earnings_announce is left unchanged (updated separately
// by the earnings calendar job via UpdateEarningsAnnounce).
func (r *FundamentalsRepository) UpsertFundamentals(ctx context.Context, securityID int64, snap providers.ParsedFundamentalsSnapshot) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO fact_fundamentals (
			security_id, last_update,
			peg_ratio, eps_ttm,
			revenue_ttm, ebitda, profit_margin, operating_margin_ttm,
			return_on_assets_ttm, return_on_equity_ttm, revenue_per_share_ttm,
			dividend_per_share,
			quarterly_earnings_growth, quarterly_revenue_growth,
			eps_estimate_current_year, eps_estimate_next_year,
			most_recent_quarter,
			enterprise_value, price_book_mrq,
			ev_ebitda, ev_revenue,
			shares_short, short_percent, short_ratio,
			shares_outstanding, shares_float, percent_insiders, percent_institutions,
			analyst_target_price,
			analyst_strong_buy, analyst_buy, analyst_hold, analyst_sell, analyst_strong_sell
		) VALUES (
			$1, NOW(),
			$2, $3,
			$4, $5, $6, $7,
			$8, $9, $10,
			$11,
			$12, $13,
			$14, $15,
			$16,
			$17, $18,
			$19, $20,
			$21, $22, $23,
			$24, $25, $26, $27,
			$28,
			$29, $30, $31, $32, $33
		)
		ON CONFLICT (security_id) DO UPDATE SET
			last_update               = NOW(),
			peg_ratio                 = $2,
			eps_ttm                   = $3,
			revenue_ttm               = $4,
			ebitda                    = $5,
			profit_margin             = $6,
			operating_margin_ttm      = $7,
			return_on_assets_ttm      = $8,
			return_on_equity_ttm      = $9,
			revenue_per_share_ttm     = $10,
			dividend_per_share        = $11,
			quarterly_earnings_growth = $12,
			quarterly_revenue_growth  = $13,
			eps_estimate_current_year = $14,
			eps_estimate_next_year    = $15,
			most_recent_quarter       = $16,
			enterprise_value          = $17,
			price_book_mrq            = $18,
			ev_ebitda                 = $19,
			ev_revenue                = $20,
			shares_short              = $21,
			short_percent             = $22,
			short_ratio               = $23,
			shares_outstanding        = $24,
			shares_float              = $25,
			percent_insiders          = $26,
			percent_institutions      = $27,
			analyst_target_price      = $28,
			analyst_strong_buy        = $29,
			analyst_buy               = $30,
			analyst_hold              = $31,
			analyst_sell              = $32,
			analyst_strong_sell       = $33
	`,
		securityID,
		snap.PEGRatio, snap.EpsTTM,
		snap.RevenueTTM, snap.EBITDA, snap.ProfitMargin, snap.OperatingMarginTTM,
		snap.ReturnOnAssetsTTM, snap.ReturnOnEquityTTM, snap.RevenuePerShareTTM,
		snap.DividendPerShare,
		snap.QuarterlyEarningsGrowth, snap.QuarterlyRevenueGrowth,
		snap.EpsEstimateCurrentYear, snap.EpsEstimateNextYear,
		snap.MostRecentQuarter,
		snap.EnterpriseValue, snap.PriceBookMRQ,
		snap.EvEBITDA, snap.EvRevenue,
		snap.SharesShort, snap.ShortPercent, snap.ShortRatio,
		snap.SharesOutstanding, snap.SharesFloat, snap.PercentInsiders, snap.PercentInstitutions,
		snap.AnalystTargetPrice,
		snap.AnalystStrongBuy, snap.AnalystBuy, snap.AnalystHold, snap.AnalystSell, snap.AnalystStrongSell,
	)
	return err
}

// UpdateEarningsAnnounce sets next_earnings_announce for a security.
// Called by the earnings calendar job; creates the row if it doesn't exist yet
// so that the calendar job can run independently of the fundamentals backfill.
func (r *FundamentalsRepository) UpdateEarningsAnnounce(ctx context.Context, securityID int64, date time.Time) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO fact_fundamentals (security_id, next_earnings_announce)
		VALUES ($1, $2)
		ON CONFLICT (security_id) DO UPDATE SET
			next_earnings_announce = $2
	`, securityID, date)
	return err
}

// UpsertFinancialsHistory merges a batch of time-series rows into fact_financials_history.
// Existing rows for the same (security_id, period_end, period_type) are updated in place,
// preserving non-null values already present where the incoming row has null.
// All statements are sent in a single pgx Batch to avoid O(n) round-trips.
func (r *FundamentalsRepository) UpsertFinancialsHistory(ctx context.Context, securityID int64, rows []providers.ParsedFinancialsRow) error {
	if len(rows) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO fact_financials_history (
				security_id, period_end, period_type,
				shares_outstanding,
				eps_actual, eps_estimate, eps_difference, surprise_percent,
				report_date, before_after_market
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (security_id, period_end, period_type) DO UPDATE SET
				shares_outstanding  = COALESCE($4,  fact_financials_history.shares_outstanding),
				eps_actual          = COALESCE($5,  fact_financials_history.eps_actual),
				eps_estimate        = COALESCE($6,  fact_financials_history.eps_estimate),
				eps_difference      = COALESCE($7,  fact_financials_history.eps_difference),
				surprise_percent    = COALESCE($8,  fact_financials_history.surprise_percent),
				report_date         = COALESCE($9,  fact_financials_history.report_date),
				before_after_market = COALESCE($10, fact_financials_history.before_after_market)
		`,
			securityID, row.PeriodEnd, row.PeriodType,
			row.SharesOutstanding,
			row.EpsActual, row.EpsEstimate, row.EpsDifference, row.SurprisePercent,
			row.ReportDate, row.BeforeAfterMarket,
		)
	}
	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	log.Debugf("[fundamentals_repo] upserted %d fact_financials_history rows for security_id=%d", len(rows), securityID)
	return nil
}

// UpsertSecurityListings replaces all cross-exchange listing rows for a security.
// Listings with empty ticker or exchange are skipped.
// All statements are sent in a single pgx Batch to avoid O(n) round-trips.
func (r *FundamentalsRepository) UpsertSecurityListings(ctx context.Context, securityID int64, listings []providers.ParsedSecurityListing) error {
	if len(listings) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	queued := 0
	for _, l := range listings {
		if l.TickerCode == "" || l.ExchangeCode == "" {
			continue
		}
		batch.Queue(`
			INSERT INTO dim_security_listings (security_id, exchange_code, ticker_code, security_name)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (security_id, exchange_code) DO UPDATE SET
				ticker_code   = EXCLUDED.ticker_code,
				security_name = EXCLUDED.security_name
		`, securityID, l.ExchangeCode, l.TickerCode, l.Name)
		queued++
	}
	if queued == 0 {
		return nil
	}
	br := r.pool.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < queued; i++ {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	log.Debugf("[fundamentals_repo] upserted %d dim_security_listings rows for security_id=%d", queued, securityID)
	return nil
}

// ResolveByExchangeTicker looks up a security_id from dim_security_listings by
// (ticker_code, exchange_code). Returns 0, nil if not found; propagates real DB errors.
func (r *FundamentalsRepository) ResolveByExchangeTicker(ctx context.Context, ticker, exchangeCode string) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, `
		SELECT security_id
		FROM dim_security_listings
		WHERE ticker_code = $1 AND exchange_code = $2
		LIMIT 1
	`, ticker, exchangeCode).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return id, nil
}

package providers

import "time"

// ParsedFundamentals contains all fundamental data for one security parsed from EODHD.
// The dim_security fields update the existing security row; the rest populate the new tables.
type ParsedFundamentals struct {
	// Identity — extracted from General for security lookup and API calls
	// Ticker matches the convention used everywhere else in this codebase (dim_security.ticker).
	// For common stocks: stripped from PrimaryTicker ("NVDA.US" → "NVDA").
	// For ETFs: EODHD omits PrimaryTicker, so Code is used directly ("SPY").
	Ticker       string
	ExchangeName string // EODHD exchange name ("NASDAQ", "NYSE ARCA"), matches dim_exchanges.name
	Code         string // raw EODHD Code — same as Ticker in practice, kept for API URL construction

	// dim_security updates
	ISIN            string
	CIK             string
	CUSIP           string
	LEI             string
	Description     string
	Employees       *int32
	CountryISO      string
	FiscalYearEnd   string
	GicSector       string
	GicGroup        string
	GicIndustry     string
	GicSubIndustry  string
	IPODate         *time.Time
	URL             string     // General.WebURL (stocks) or ETF_Data.Company_URL (ETFs)
	ETFURL          string     // ETF_Data.ETF_URL — product page; empty for stocks/funds
	NetExpenseRatio *float64   // ETF_Data.NetExpenseRatio / MutualFund_Data.Expense_Ratio; nil for stocks
	TotalAssets     *int64     // ETF_Data.TotalAssets / MutualFund_Data.Share_Class_Net_Assets; nil for stocks
	ETFYield        *float64   // ETF_Data.Yield / MutualFund_Data.Yield; nil for stocks
	NAV             *float64   // MutualFund_Data.Nav; nil for stocks and ETFs

	// dim_security_listings rows
	Listings []ParsedSecurityListing

	// fact_fundamentals snapshot fields
	Snapshot ParsedFundamentalsSnapshot

	// fact_financials_history rows (quarterly and annual merged)
	History []ParsedFinancialsRow
}

// ParsedSecurityListing is one row destined for dim_security_listings.
type ParsedSecurityListing struct {
	ExchangeCode string
	TickerCode   string
	Name         string
}

// ParsedFundamentalsSnapshot maps to all non-time-series columns of fact_fundamentals.
type ParsedFundamentalsSnapshot struct {
	// Highlights
	MarketCap               *int64
	PERatio                 *float64
	PEGRatio                *float64
	EpsTTM                  *float64
	RevenueTTM              *int64
	EBITDA                  *int64
	ProfitMargin            *float64
	OperatingMarginTTM      *float64
	ReturnOnAssetsTTM       *float64
	ReturnOnEquityTTM       *float64
	RevenuePerShareTTM      *float64
	BookValuePerShare       *float64
	DividendYield           *float64
	DividendPerShare        *float64
	QuarterlyEarningsGrowth *float64
	QuarterlyRevenueGrowth  *float64
	EpsEstimateCurrentYear  *float64
	EpsEstimateNextYear     *float64
	WallStreetTargetPrice   *float64
	MostRecentQuarter       *time.Time

	// Valuation
	EnterpriseValue *int64
	ForwardPE       *float64
	PriceBookMRQ    *float64
	PriceSalesTTM   *float64
	EvEBITDA        *float64
	EvRevenue       *float64

	// Technicals
	Beta          *float64
	Week52High    *float64
	Week52Low     *float64
	MA50          *float64
	MA200         *float64
	SharesShort   *int64
	ShortPercent  *float64
	ShortRatio    *float64

	// SharesStats
	SharesOutstanding   *int64
	SharesFloat         *int64
	PercentInsiders     *float64
	PercentInstitutions *float64

	// Analyst Ratings — raw vote counts only; Rating field dropped (see EODHD parser comment)
	AnalystTargetPrice *float64
	AnalystStrongBuy   *int32
	AnalystBuy         *int32
	AnalystHold        *int32
	AnalystSell        *int32
	AnalystStrongSell  *int32
}

// EarningsAnnouncement is one entry from the EODHD upcoming earnings calendar.
type EarningsAnnouncement struct {
	Ticker       string
	ExchangeCode string
	ReportDate   time.Time
}

// ParsedFinancialsRow is one row for fact_financials_history.
// PeriodType is "A" (annual) or "Q" (quarterly).
type ParsedFinancialsRow struct {
	PeriodEnd         time.Time
	PeriodType        string
	SharesOutstanding *int64
	EpsActual         *float64
	EpsEstimate       *float64
	EpsDifference     *float64
	SurprisePercent   *float64
	ReportDate        *time.Time
	BeforeAfterMarket *string
}

// BulkEventRecord represents a corporate action (split or dividend) from a bulk events fetch.
type BulkEventRecord struct {
	Code             string
	Date             time.Time
	Dividend         float64
	SplitCoefficient float64
}

// BulkEODRecord represents a single end-of-day record from a bulk price fetch.
type BulkEODRecord struct {
	Code     string
	Date     time.Time
	Open     float64
	High     float64
	Low      float64
	Close    float64
	AdjClose float64
	Volume   int64
}

// MergeEventsByDate combines split and dividend slices into one slice, merging
// entries that share the same date. Split-only dates get Dividend=0.
// Dividend-only dates get SplitCoefficient=1.0.
func MergeEventsByDate(splits, dividends []ParsedEventData) []ParsedEventData {
	merged := make(map[time.Time]ParsedEventData)
	for _, s := range splits {
		e := merged[s.Date]
		e.Date = s.Date
		e.SplitCoefficient = s.SplitCoefficient
		merged[s.Date] = e
	}
	for _, d := range dividends {
		e := merged[d.Date]
		e.Date = d.Date
		if e.SplitCoefficient == 0 {
			e.SplitCoefficient = 1.0
		}
		e.Dividend = d.Dividend
		merged[d.Date] = e
	}
	result := make([]ParsedEventData, 0, len(merged))
	for _, e := range merged {
		result = append(result, e)
	}
	return result
}

// ParsedEventData represents a corporate action event (split or dividend) for a security.
type ParsedEventData struct {
	Date             time.Time
	Dividend         float64
	SplitCoefficient float64
}

// ParsedPriceData represents parsed price data ready for use by pricing services.
type ParsedPriceData struct {
	Date             time.Time
	Open             float64
	High             float64
	Low              float64
	Close            float64
	Volume           int64
	Dividend         float64
	SplitCoefficient float64
}

// ParsedETFHolding represents a parsed ETF holding.
type ParsedETFHolding struct {
	Ticker     string
	Name       string
	Percentage float64
}

// ListingStatusEntry represents a row from the LISTING_STATUS CSV endpoint.
type ListingStatusEntry struct {
	Ticker        string
	Name          string
	Exchange      string
	AssetType     string
	IPODate       *time.Time
	DelistingDate *time.Time
	Status        string
}

// ExchangeInfo represents a single exchange returned by the EODHD exchanges-list endpoint.
type ExchangeInfo struct {
	Code        string
	Name        string
	Country     string
	Currency    string
	CountryISO2 string
	CountryISO3 string
}

// SymbolRecord represents a single security returned by the EODHD exchange-symbol-list endpoint.
type SymbolRecord struct {
	Ticker   string
	Name     string
	Country  string
	Exchange string
	Currency string
	Type     string
	Isin     string
}

package eodhd

import "encoding/json"

// eohdEODRecord is the raw JSON record from the EODHD EOD endpoint.
type eohdEODRecord struct {
	Date          string  `json:"date"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	AdjustedClose float64 `json:"adjusted_close"`
	Volume        float64 `json:"volume"` // float64 because EODHD occasionally returns fractional volumes
}

// eohdDividendRecord is the raw JSON record from the EODHD dividends endpoint.
type eohdDividendRecord struct {
	Date  string  `json:"date"`
	Value float64 `json:"value"`
}

// eohdSplitRecord is the raw JSON record from the EODHD splits endpoint.
// Split is formatted as "numerator/denominator" with decimals (e.g., "4.0000/1.0000").
type eohdSplitRecord struct {
	Date  string `json:"date"`
	Split string `json:"split"`
}

// eohdBulkSplitRecord is the raw JSON record from the EODHD bulk splits endpoint.
type eohdBulkSplitRecord struct {
	Code  string `json:"code"`
	Date  string `json:"date"`
	Split string `json:"split"` // "numerator/denominator", e.g. "1.000000/80.000000"
}

// eohdBulkDividendRecord is the raw JSON record from the EODHD bulk dividends endpoint.
type eohdBulkDividendRecord struct {
	Code     string `json:"code"`
	Date     string `json:"date"`
	Dividend string `json:"dividend"` // quoted decimal string in API response, e.g. "0.15954"
}

// eohdBulkEODRecord is the raw JSON record from the EODHD bulk EOD endpoint.
type eohdBulkEODRecord struct {
	Code          string  `json:"code"`
	Date          string  `json:"date"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	AdjustedClose float64 `json:"adjusted_close"`
	Volume        float64 `json:"volume"` // float64 because EODHD occasionally returns fractional volumes
}

// eohdExchangeRecord is the raw JSON record from the EODHD exchanges-list endpoint.
type eohdExchangeRecord struct {
	Name         string `json:"Name"`
	Code         string `json:"Code"`
	OperatingMIC string `json:"OperatingMIC"`
	Country      string `json:"Country"`
	Currency     string `json:"Currency"`
	CountryISO2  string `json:"CountryISO2"`
	CountryISO3  string `json:"CountryISO3"`
}

// eohdSymbolRecord is the raw JSON record from the EODHD exchange-symbol-list endpoint.
type eohdSymbolRecord struct {
	Code     string `json:"Code"`
	Name     string `json:"Name"`
	Country  string `json:"Country"`
	Exchange string `json:"Exchange"`
	Currency string `json:"Currency"`
	Type     string `json:"Type"`
	Isin     string `json:"Isin"`
}

// eohdFundamentalsResponse is the top-level JSON from the EODHD fundamentals endpoint.
// Only the sections used by this project are decoded; everything else is ignored.
type eohdFundamentalsResponse struct {
	General           eohdFundamentalsGeneral  `json:"General"`
	Highlights        eohdHighlights           `json:"Highlights"`
	Valuation         eohdValuation            `json:"Valuation"`
	Technicals        eohdTechnicals           `json:"Technicals"`
	SharesStats       eohdSharesStats          `json:"SharesStats"`
	AnalystRatings    eohdAnalystRatings       `json:"AnalystRatings"`
	Earnings          eohdEarnings             `json:"Earnings"`
	// ETF_Data is present only for ETFs; stocks and mutual funds leave this empty.
	// ETFs omit General.IPODate, placing it here instead.
	ETFData           eohdETFData              `json:"ETF_Data"`
	// MutualFund_Data is present only for mutual funds (General.Type == "FUND").
	// Mutual funds omit General.IPODate and use Fund_Summary instead of Description.
	MutualFundData    eohdMutualFundData       `json:"MutualFund_Data"`
}

// eohdMutualFundData holds the MutualFund_Data section of the EODHD fundamentals response.
// Present only when General.Type == "FUND"; all other security types leave this empty.
type eohdMutualFundData struct {
	InceptionDate        string `json:"Inception_Date"`
	Nav                  string `json:"Nav"`              // quoted decimal, e.g. "629.31"
	Yield                string `json:"Yield"`            // quoted decimal, e.g. "0.0118"
	ExpenseRatio         string `json:"Expense_Ratio"`    // quoted decimal, e.g. "0.0400"
	ShareClassNetAssets  string `json:"Share_Class_Net_Assets"` // quoted int, e.g. "598067630000"
}

// eohdETFData holds the ETF-specific section of the EODHD fundamentals response.
// Stocks and mutual funds leave this section absent; only fields used by this project are decoded.
type eohdETFData struct {
	ISIN             string `json:"ISIN"`
	InceptionDate    string `json:"Inception_Date"`
	CompanyURL       string `json:"Company_URL"`
	ETFURL           string `json:"ETF_URL"`
	NetExpenseRatio  string `json:"NetExpenseRatio"` // quoted decimal, e.g. "0.00095"
	TotalAssets      string `json:"TotalAssets"`     // quoted decimal, e.g. "689207414294.00"
	Yield            string `json:"Yield"`           // quoted decimal, e.g. "1.140000"
}

type eohdFundamentalsGeneral struct {
	Code           string                 `json:"Code"`
	Name           string                 `json:"Name"`
	PrimaryTicker  string                 `json:"PrimaryTicker"` // e.g. "NVDA.US" — strip last segment for bare ticker
	Exchange       string                 `json:"Exchange"`      // e.g. "NASDAQ" — matches dim_exchanges.name
	ISIN           string                 `json:"ISIN"`
	CUSIP          string                 `json:"CUSIP"`
	CIK            string                 `json:"CIK"`
	LEI            string                 `json:"LEI"`
	Description    string                 `json:"Description"`
	GicSector      string                 `json:"GicSector"`
	GicGroup       string                 `json:"GicGroup"`
	GicIndustry    string                 `json:"GicIndustry"`
	GicSubIndustry string                 `json:"GicSubIndustry"`
	IPODate           string                 `json:"IPODate"`
	FiscalYearEnd     string                 `json:"FiscalYearEnd"`    // stocks/ETFs
	FiscalYearEndMF   string                 `json:"Fiscal_Year_End"`  // mutual funds use underscored key
	Employees         *int32                 `json:"FullTimeEmployees"`
	CountryISO     string                 `json:"CountryISO"`
	WebURL         string                 `json:"WebURL"`
	FundSummary    string                 `json:"Fund_Summary"` // mutual funds use this instead of Description
	Listings       map[string]eohdListing `json:"Listings"`
}

type eohdListing struct {
	Code     string `json:"Code"`
	Exchange string `json:"Exchange"`
	Name     string `json:"Name"`
}

type eohdHighlights struct {
	MarketCapitalization       *int64   `json:"MarketCapitalization"`
	PERatio                    *float64 `json:"PERatio"`
	PEGRatio                   *float64 `json:"PEGRatio"`
	EarningsShare              *float64 `json:"EarningsShare"`
	RevenueTTM                 *int64   `json:"RevenueTTM"`
	EBITDA                     *int64   `json:"EBITDA"`
	ProfitMargin               *float64 `json:"ProfitMargin"`
	OperatingMarginTTM         *float64 `json:"OperatingMarginTTM"`
	ReturnOnAssetsTTM          *float64 `json:"ReturnOnAssetsTTM"`
	ReturnOnEquityTTM          *float64 `json:"ReturnOnEquityTTM"`
	RevenuePerShareTTM         *float64 `json:"RevenuePerShareTTM"`
	BookValuePerShare          *float64 `json:"BookValuePerShare"`
	DividendYield              *float64 `json:"DividendYield"`
	DividendShare              *float64 `json:"DividendShare"`
	QuarterlyEarningsGrowthYOY *float64 `json:"QuarterlyEarningsGrowthYOY"`
	QuarterlyRevenueGrowthYOY  *float64 `json:"QuarterlyRevenueGrowthYOY"`
	AnalystTargetPrice         *float64 `json:"AnalystTargetPrice"`
	EpsEstimateCurrentYear     *float64 `json:"EpsEstimateCurrentYear"`
	EpsEstimateNextYear        *float64 `json:"EpsEstimateNextYear"`
	MostRecentQuarter          string   `json:"MostRecentQuarter"`
}

type eohdValuation struct {
	EnterpriseValue        *int64   `json:"EnterpriseValue"`
	ForwardPE              *float64 `json:"ForwardPE"`
	PriceSalesTTM          *float64 `json:"PriceSalesTTM"`
	PriceBookMRQ           *float64 `json:"PriceBookMRQ"`
	EnterpriseValueRevenue *float64 `json:"EnterpriseValueRevenue"`
	EnterpriseValueEbitda  *float64 `json:"EnterpriseValueEbitda"`
}

type eohdTechnicals struct {
	Beta         *float64 `json:"Beta"`
	Week52High   *float64 `json:"52WeekHigh"`
	Week52Low    *float64 `json:"52WeekLow"`
	MA50         *float64 `json:"50DayMA"`
	MA200        *float64 `json:"200DayMA"`
	SharesShort  *int64   `json:"SharesShort"`
	ShortRatio   *float64 `json:"ShortRatio"`
	ShortPercent *float64 `json:"ShortPercent"`
}

type eohdSharesStats struct {
	SharesOutstanding   *int64   `json:"SharesOutstanding"`
	SharesFloat         *int64   `json:"SharesFloat"`
	PercentInsiders     *float64 `json:"PercentInsiders"`
	PercentInstitutions *float64 `json:"PercentInstitutions"`
}

type eohdAnalystRatings struct {
	Rating      *float64 `json:"Rating"`
	TargetPrice *float64 `json:"TargetPrice"`
	StrongBuy   *int32   `json:"StrongBuy"`
	Buy         *int32   `json:"Buy"`
	Hold        *int32   `json:"Hold"`
	Sell        *int32   `json:"Sell"`
	StrongSell  *int32   `json:"StrongSell"`
}

// eohdEarnings holds Earnings.History and Earnings.Annual from the fundamentals response.
// Both fields are decoded via json.RawMessage because EODHD returns them as an empty
// array [] (not an empty object) when there is no data, which would break map unmarshaling.
type eohdEarnings struct {
	HistoryRaw json.RawMessage `json:"History"`
	AnnualRaw  json.RawMessage `json:"Annual"`
}

type eohdEarningsHistoryEntry struct {
	ReportDate        string   `json:"reportDate"`
	Date              string   `json:"date"`
	BeforeAfterMarket string   `json:"beforeAfterMarket"`
	EpsActual         *float64 `json:"epsActual"`
	EpsEstimate       *float64 `json:"epsEstimate"`
	EpsDifference     *float64 `json:"epsDifference"`
	SurprisePercent   *float64 `json:"surprisePercent"`
}

type eohdEarningsAnnualEntry struct {
	Date      string   `json:"date"`
	EpsActual *float64 `json:"epsActual"`
}


// eohdEarningsCalendarEntry is one record from the EODHD upcoming earnings calendar endpoint.
// Endpoint: GET /api/calendar/earnings?api_token=...&from=YYYY-MM-DD&to=YYYY-MM-DD
// The response includes both past and future entries when the range spans today.
// Future entries carry actual=0 (not null) as EODHD's placeholder for unreported —
// that is not a real EPS value; EPS data comes from the fundamentals endpoint.
type eohdEarningsCalendarEntry struct {
	Code              string `json:"code"`                // "AAPL.US"
	ReportDate        string `json:"report_date"`         // "2026-01-29"
	BeforeAfterMarket string `json:"before_after_market"` // "AfterMarket", "BeforeMarket", or null
	// actual, estimate, difference, percent are present but unused here.
}

// eohdEarningsCalendarResponse wraps the earnings calendar endpoint response.
type eohdEarningsCalendarResponse struct {
	Earnings []eohdEarningsCalendarEntry `json:"earnings"`
}

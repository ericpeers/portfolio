package models

import "time"

// FundamentalsMetaUpdate carries the dim_security columns populated from EODHD fundamentals.
// Passed to SecurityRepository.UpdateFundamentalsMeta; keeps the repository layer free of
// provider-specific types.
type FundamentalsMetaUpdate struct {
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
	ISIN            string
	IPODate         *time.Time
	URL             string
	ETFURL          string
	NetExpenseRatio *float64
	TotalAssets     *int64
	ETFYield        *float64
	NAV             *float64
}

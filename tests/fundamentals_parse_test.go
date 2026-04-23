package tests

import (
	"os"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/providers/eodhd"
)

// TestParseFundamentalsNVDA verifies that the EODHD JSON parser correctly extracts
// key fields from a real NVDA fundamentals response. No DB or API calls are made.
func TestParseFundamentalsNVDA(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/NVDA.US.fundamentals.json")
	if err != nil {
		t.Fatalf("testdata/NVDA.US.fundamentals.json not found — place the EODHD response file there: %v", err)
	}

	pf, err := eodhd.ParseFundamentalsJSON(data)
	if err != nil {
		t.Fatalf("ParseFundamentalsJSON: %v", err)
	}

	// --- Identity / dim_security fields ---

	if pf.Ticker != "NVDA" {
		t.Errorf("Ticker = %q, want %q", pf.Ticker, "NVDA")
	}
	if pf.Code != "NVDA" {
		t.Errorf("Code = %q, want %q", pf.Code, "NVDA")
	}
	if pf.ExchangeName == "" {
		t.Error("ExchangeName is empty")
	}
	if pf.ISIN != "US67066G1040" {
		t.Errorf("ISIN = %q, want %q", pf.ISIN, "US67066G1040")
	}
	if pf.CIK != "0001045810" {
		t.Errorf("CIK = %q, want %q", pf.CIK, "0001045810")
	}
	if pf.CUSIP != "67066G104" {
		t.Errorf("CUSIP = %q, want %q", pf.CUSIP, "67066G104")
	}
	if pf.CountryISO != "US" {
		t.Errorf("CountryISO = %q, want %q", pf.CountryISO, "US")
	}
	if pf.Description == "" {
		t.Error("Description is empty")
	}
	if pf.FiscalYearEnd != "January" {
		t.Errorf("FiscalYearEnd = %q, want %q", pf.FiscalYearEnd, "January")
	}
	if pf.IPODate == nil {
		t.Error("IPODate is nil for NVDA")
	} else {
		want := time.Date(1999, 1, 22, 0, 0, 0, 0, time.UTC)
		if !pf.IPODate.Equal(want) {
			t.Errorf("IPODate = %s, want %s", pf.IPODate.Format(time.DateOnly), want.Format(time.DateOnly))
		}
	}
	if pf.GicSector == "" {
		t.Error("GicSector is empty")
	}
	if pf.GicGroup == "" {
		t.Error("GicGroup is empty")
	}
	if pf.GicIndustry == "" {
		t.Error("GicIndustry is empty")
	}
	if pf.GicSubIndustry == "" {
		t.Error("GicSubIndustry is empty")
	}
	if pf.URL != "https://www.nvidia.com" {
		t.Errorf("URL = %q, want %q", pf.URL, "https://www.nvidia.com")
	}
	if pf.ETFURL != "" {
		t.Errorf("ETFURL = %q, want empty for stock", pf.ETFURL)
	}
	if pf.NetExpenseRatio != nil {
		t.Errorf("NetExpenseRatio should be nil for stock, got %v", *pf.NetExpenseRatio)
	}
	if pf.TotalAssets != nil {
		t.Errorf("TotalAssets should be nil for stock, got %v", *pf.TotalAssets)
	}
	if pf.ETFYield != nil {
		t.Errorf("ETFYield should be nil for stock, got %v", *pf.ETFYield)
	}

	// --- Snapshot (fact_fundamentals) ---

	snap := pf.Snapshot
	if snap.EpsTTM == nil {
		t.Error("Snapshot.EpsTTM is nil")
	}
	if snap.RevenueTTM == nil || *snap.RevenueTTM <= 0 {
		t.Error("Snapshot.RevenueTTM is nil or zero")
	}
	if snap.EBITDA == nil || *snap.EBITDA <= 0 {
		t.Error("Snapshot.EBITDA is nil or zero")
	}
	if snap.EnterpriseValue == nil || *snap.EnterpriseValue <= 0 {
		t.Error("Snapshot.EnterpriseValue is nil or zero")
	}
	if snap.PriceBookMRQ == nil {
		t.Error("Snapshot.PriceBookMRQ is nil")
	}
	if snap.SharesOutstanding == nil || *snap.SharesOutstanding <= 0 {
		t.Error("Snapshot.SharesOutstanding is nil or zero")
	}
	if snap.SharesFloat == nil || *snap.SharesFloat <= 0 {
		t.Error("Snapshot.SharesFloat is nil or zero")
	}
	if snap.SharesShort == nil || *snap.SharesShort <= 0 {
		t.Error("Snapshot.SharesShort is nil or zero")
	}
	if snap.AnalystStrongBuy == nil {
		t.Error("Snapshot.AnalystStrongBuy is nil")
	}
	if snap.MostRecentQuarter == nil {
		t.Error("Snapshot.MostRecentQuarter is nil")
	} else if snap.MostRecentQuarter.IsZero() {
		t.Error("Snapshot.MostRecentQuarter is zero time")
	}

	// --- History (fact_financials_history) ---

	if len(pf.History) == 0 {
		t.Error("History is empty")
	}
	var hasQ, hasA bool
	for _, h := range pf.History {
		if h.PeriodEnd.IsZero() {
			t.Error("History row has zero PeriodEnd")
		}
		if h.PeriodType != "Q" && h.PeriodType != "A" {
			t.Errorf("History row has unexpected PeriodType %q", h.PeriodType)
		}
		if h.PeriodType == "Q" {
			hasQ = true
		}
		if h.PeriodType == "A" {
			hasA = true
		}
	}
	if !hasQ {
		t.Error("no quarterly history rows")
	}
	if !hasA {
		t.Error("no annual history rows")
	}

	// --- Listings (dim_security_listings) ---

	if len(pf.Listings) == 0 {
		t.Error("Listings is empty")
	}
	for _, l := range pf.Listings {
		if l.TickerCode == "" {
			t.Error("Listing has empty TickerCode")
		}
		if l.ExchangeCode == "" {
			t.Error("Listing has empty ExchangeCode")
		}
	}
}

// TestParseFundamentalsSPY verifies parsing of an ETF (SPY) fundamentals response.
// EODHD returns a stripped-down General section for ETFs — no PrimaryTicker, no Listings,
// no ISIN/GIC/IPODate in General. Those fields come from ETF_Data instead.
func TestParseFundamentalsSPY(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/SPY.US.fundamentals.json")
	if err != nil {
		t.Fatalf("testdata/SPY.US.fundamentals.json not found — place the EODHD response file there: %v", err)
	}

	pf, err := eodhd.ParseFundamentalsJSON(data)
	if err != nil {
		t.Fatalf("ParseFundamentalsJSON: %v", err)
	}

	// --- Identity / dim_security fields ---

	if pf.Ticker != "SPY" {
		t.Errorf("Ticker = %q, want %q (Code fallback for ETFs)", pf.Ticker, "SPY")
	}
	if pf.Code != "SPY" {
		t.Errorf("Code = %q, want %q", pf.Code, "SPY")
	}
	if pf.ExchangeName == "" {
		t.Error("ExchangeName is empty")
	}
	// ISIN comes from ETF_Data.ISIN, not General.ISIN.
	if pf.ISIN != "US78462F1030" {
		t.Errorf("ISIN = %q, want %q (sourced from ETF_Data)", pf.ISIN, "US78462F1030")
	}
	if pf.CountryISO != "US" {
		t.Errorf("CountryISO = %q, want %q", pf.CountryISO, "US")
	}
	if pf.Description == "" {
		t.Error("Description is empty")
	}
	// Inception date comes from ETF_Data.Inception_Date, not General.IPODate.
	if pf.IPODate == nil {
		t.Error("IPODate is nil — ETF_Data.Inception_Date not parsed")
	} else {
		want := time.Date(1993, 1, 22, 0, 0, 0, 0, time.UTC)
		if !pf.IPODate.Equal(want) {
			t.Errorf("IPODate = %s, want %s (sourced from ETF_Data)", pf.IPODate.Format(time.DateOnly), want.Format(time.DateOnly))
		}
	}
	// ETF JSON omits these — confirm they stay empty rather than picking up garbage.
	if pf.CIK != "" {
		t.Errorf("CIK = %q, want empty for ETF", pf.CIK)
	}
	if pf.GicSector != "" {
		t.Errorf("GicSector = %q, want empty for ETF", pf.GicSector)
	}
	if pf.FiscalYearEnd != "" {
		t.Errorf("FiscalYearEnd = %q, want empty for ETF", pf.FiscalYearEnd)
	}
	// ETF URLs: Company_URL populates url; ETF_URL populates etf_url.
	if pf.URL != "http://www.spdrs.com" {
		t.Errorf("URL = %q, want %q (ETF_Data.Company_URL)", pf.URL, "http://www.spdrs.com")
	}
	if pf.ETFURL != "https://us.spdrs.com/en/product/fund.seam?ticker=SPY" {
		t.Errorf("ETFURL = %q, want ETF product page URL", pf.ETFURL)
	}
	// ETF financial stats.
	if pf.NetExpenseRatio == nil {
		t.Error("NetExpenseRatio is nil for ETF")
	}
	if pf.TotalAssets == nil || *pf.TotalAssets <= 0 {
		t.Error("TotalAssets is nil or zero for ETF")
	}
	if pf.ETFYield == nil {
		t.Error("ETFYield is nil for ETF")
	}

	// --- Snapshot (fact_fundamentals) ---

	snap := pf.Snapshot
	// ETF-specific: income statement, valuation, and technicals are all absent
	// in the EODHD ETF schema. The snapshot will be all-nil for ETFs — price-derived
	// fields (beta, 52w, MAs) are computed from fact_price and never stored.
	if snap.EpsTTM != nil {
		t.Errorf("Snapshot.EpsTTM should be nil for ETF, got %v", *snap.EpsTTM)
	}
	if snap.RevenueTTM != nil {
		t.Errorf("Snapshot.RevenueTTM should be nil for ETF, got %v", *snap.RevenueTTM)
	}

	// --- History (fact_financials_history) ---

	// ETFs have no EPS history rows.
	for _, h := range pf.History {
		if h.EpsActual != nil {
			t.Errorf("ETF SPY has unexpected EpsActual in history row at %s", h.PeriodEnd.Format(time.DateOnly))
		}
	}

	// --- Listings (dim_security_listings) ---

	// ETF JSON omits Listings entirely — expected, not an error.
	_ = pf.Listings
}

// TestParseFundamentalsVFIAX verifies parsing of a mutual fund (VFIAX) fundamentals response.
// EODHD returns a MutualFund_Data section instead of ETF_Data; General omits IPODate/Description.
func TestParseFundamentalsVFIAX(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/VFIAX.US.fundamentals.json")
	if err != nil {
		t.Fatalf("testdata/VFIAX.US.fundamentals.json not found: %v", err)
	}

	pf, err := eodhd.ParseFundamentalsJSON(data)
	if err != nil {
		t.Fatalf("ParseFundamentalsJSON: %v", err)
	}

	// --- Identity ---

	if pf.Ticker != "VFIAX" {
		t.Errorf("Ticker = %q, want %q", pf.Ticker, "VFIAX")
	}
	if pf.ISIN != "US9229087104" {
		t.Errorf("ISIN = %q, want %q (sourced from General)", pf.ISIN, "US9229087104")
	}
	if pf.CUSIP != "922908710" {
		t.Errorf("CUSIP = %q, want %q", pf.CUSIP, "922908710")
	}
	if pf.Description == "" {
		t.Error("Description is empty — Fund_Summary fallback not working")
	}
	if pf.FiscalYearEnd != "December" {
		t.Errorf("FiscalYearEnd = %q, want %q", pf.FiscalYearEnd, "December")
	}
	// Inception date comes from MutualFund_Data.Inception_Date, not General.IPODate.
	if pf.IPODate == nil {
		t.Error("IPODate is nil — MutualFund_Data.Inception_Date not parsed")
	} else {
		want := time.Date(1976, 8, 31, 0, 0, 0, 0, time.UTC)
		if !pf.IPODate.Equal(want) {
			t.Errorf("IPODate = %s, want %s", pf.IPODate.Format(time.DateOnly), want.Format(time.DateOnly))
		}
	}
	// Mutual funds have no GIC classification.
	if pf.GicSector != "" {
		t.Errorf("GicSector = %q, want empty for mutual fund", pf.GicSector)
	}
	// No ETF product page for mutual funds.
	if pf.ETFURL != "" {
		t.Errorf("ETFURL = %q, want empty for mutual fund", pf.ETFURL)
	}

	// --- Fund financial fields (sourced from MutualFund_Data) ---

	if pf.NetExpenseRatio == nil {
		t.Error("NetExpenseRatio is nil — MutualFund_Data.Expense_Ratio not parsed")
	}
	if pf.TotalAssets == nil || *pf.TotalAssets <= 0 {
		t.Error("TotalAssets is nil or zero — MutualFund_Data.Share_Class_Net_Assets not parsed")
	}
	if pf.ETFYield == nil {
		t.Error("ETFYield is nil — MutualFund_Data.Yield not parsed")
	}
	if pf.NAV == nil || *pf.NAV <= 0 {
		t.Error("NAV is nil or zero — MutualFund_Data.Nav not parsed")
	}

	// --- Snapshot — mutual funds have no Highlights/Valuation/Technicals sections ---

	snap := pf.Snapshot
	// Mutual funds have no income statement, valuation, or technicals sections in EODHD.
	if snap.EpsTTM != nil {
		t.Errorf("Snapshot.EpsTTM should be nil for mutual fund, got %v", *snap.EpsTTM)
	}

	// --- History — mutual funds have no earnings history ---

	if len(pf.History) != 0 {
		t.Errorf("History should be empty for mutual fund, got %d rows", len(pf.History))
	}

	// --- Listings — mutual funds have no cross-exchange listings ---

	if len(pf.Listings) != 0 {
		t.Errorf("Listings should be empty for mutual fund, got %d rows", len(pf.Listings))
	}
}

// TestStripExchangeSuffix verifies the PrimaryTicker stripping logic via ParseFundamentalsJSON
// indirectly — a minimal JSON with a dotted ticker exercises the stripping path.
func TestParseFundamentalsTickerStripping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		primaryTicker string
		want          string
	}{
		{"NVDA.US", "NVDA"},
		{"BRK.B.US", "BRK.B"},
		{"SPY.US", "SPY"},
		{"NVDA", "NVDA"}, // no suffix — unchanged
	}

	for _, tc := range cases {
		// Build minimal valid JSON with just the PrimaryTicker field.
		raw := []byte(`{"General":{"PrimaryTicker":"` + tc.primaryTicker + `","Exchange":"NASDAQ"}}`)
		pf, err := eodhd.ParseFundamentalsJSON(raw)
		if err != nil {
			t.Fatalf("ParseFundamentalsJSON(%q): %v", tc.primaryTicker, err)
		}
		if pf.Ticker != tc.want {
			t.Errorf("PrimaryTicker %q → Ticker %q, want %q", tc.primaryTicker, pf.Ticker, tc.want)
		}
	}
}

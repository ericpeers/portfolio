package tests

import (
	"context"
	"testing"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
)

// TestPreferUSListingUSDFallback verifies that PreferUSListing uses USD currency
// as a secondary tiebreaker when no country=USA listing exists.
// Mirrors the real-world EME case: listed on NYSE (no USA country tag), ASX, and LSE.
func TestPreferUSListingUSDFallback(t *testing.T) {
	makeSec := func(id int64, symbol, country, currency string) *models.SecurityWithCountry {
		return &models.SecurityWithCountry{
			Security: models.Security{ID: id, Symbol: symbol},
			Country:  country,
			Currency: currency,
		}
	}

	t.Run("USD wins when no country=USA exists", func(t *testing.T) {
		candidates := []*models.SecurityWithCountry{
			makeSec(1, "EME", "", "USD"),
			makeSec(2, "EME", "AUS", "AUD"),
			makeSec(3, "EME", "GBR", "GBP"),
		}
		got := repository.PreferUSListing(candidates)
		if got == nil {
			t.Fatal("expected non-nil result for single USD candidate")
		}
		if got.ID != 1 {
			t.Errorf("expected ID 1 (USD listing), got %d", got.ID)
		}
	})

	t.Run("country=USA beats USD when both present", func(t *testing.T) {
		candidates := []*models.SecurityWithCountry{
			makeSec(1, "EME", "USA", "USD"),
			makeSec(2, "EME", "AUS", "AUD"),
		}
		got := repository.PreferUSListing(candidates)
		if got == nil || got.ID != 1 {
			t.Errorf("expected country=USA listing (ID 1), got %v", got)
		}
	})

	t.Run("nil when multiple USD listings and no country=USA", func(t *testing.T) {
		candidates := []*models.SecurityWithCountry{
			makeSec(1, "EME", "BMU", "USD"), // Bermuda-listed, USD
			makeSec(2, "EME", "CYM", "USD"), // Cayman-listed, USD
		}
		got := repository.PreferUSListing(candidates)
		if got != nil {
			t.Errorf("expected nil for ambiguous USD listings, got ID %d", got.ID)
		}
	})

	t.Run("single non-USD non-USA candidate still returned", func(t *testing.T) {
		candidates := []*models.SecurityWithCountry{
			makeSec(1, "EME", "DEU", "EUR"),
		}
		got := repository.PreferUSListing(candidates)
		if got == nil || got.ID != 1 {
			t.Errorf("expected sole candidate returned, got %v", got)
		}
	})
}

// TestShouldPreferNonUSForETF verifies the 4-rule heuristic that decides whether
// an ETF's holdings should be resolved against local-exchange listings (non-US)
// rather than US-listed stocks or ADRs.
func TestShouldPreferNonUSForETF(t *testing.T) {
	cases := []struct {
		name     string
		etf      models.SecurityWithCountry
		wantTrue bool
	}{
		// Rule 1 (override): US index → always prefer US, even with non-USD currency / non-US exchange
		{
			name:     "US-listed S&P 500 ETF",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "SPDR S&P 500 ETF Trust"}, Country: "USA", Currency: "USD"},
			wantTrue: false,
		},
		{
			name:     "Australian iShares Core S&P 500 ETF (non-USD, US index override)",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "iShares Core S&P 500 ETF"}, Country: "AUS", Currency: "AUD"},
			wantTrue: false,
		},
		{
			name:     "European S&P 500 UCITS ETF (non-USD, US index override)",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "iShares Core S&P 500 UCITS ETF"}, Country: "DEU", Currency: "EUR"},
			wantTrue: false,
		},
		{
			name:     "Nasdaq-100 ETF",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "Invesco QQQ Trust (NASDAQ-100)"}, Country: "USA", Currency: "USD"},
			wantTrue: false,
		},
		{
			name:     "Russell 2000 ETF",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "iShares Russell 2000 ETF"}, Country: "USA", Currency: "USD"},
			wantTrue: false,
		},
		// Rule 2 (strong): non-USD currency → prefer non-US
		{
			name:     "Avantis Emerging Markets UCITS ETF (EUR, non-US exchange)",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "Avantis Emerging Markets Equity UCITS ETF"}, Country: "DEU", Currency: "EUR"},
			wantTrue: true,
		},
		{
			name:     "Amundi MSCI Emerging Markets UCITS ETF (EUR)",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "Amundi MSCI Emerging Markets UCITS ETF"}, Country: "DEU", Currency: "EUR"},
			wantTrue: true,
		},
		// Rule 3 (strong): ex-US branding → prefer non-US
		{
			name:     "Vanguard FTSE All-World ex-US ETF (USD, US-listed)",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "Vanguard FTSE All-World ex-US ETF"}, Country: "USA", Currency: "USD"},
			wantTrue: true,
		},
		{
			name:     "ex United States phrasing",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "MSCI ex United States Small Cap ETF"}, Country: "USA", Currency: "USD"},
			wantTrue: true,
		},
		// Rule 4 (medium): non-US exchange + geographic keyword → prefer non-US
		{
			name:     "non-US International ETF on foreign exchange",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "Xtrackers MSCI International ETF"}, Country: "DEU", Currency: "USD"},
			wantTrue: true,
		},
		// Rule 4: US-listed EM ETFs hold local-market shares (not ADRs) → prefer non-US
		{
			name:     "US-listed Emerging Markets ETF (holds local shares)",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "SPDR Portfolio Emerging Markets ETF"}, Country: "USA", Currency: "USD"},
			wantTrue: true,
		},
		{
			name:     "US-listed plain ETF",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "Vanguard Total Stock Market ETF"}, Country: "USA", Currency: "USD"},
			wantTrue: false,
		},
		// Empty currency should not trigger Rule 2
		{
			name:     "no currency set, US-listed ETF",
			etf:      models.SecurityWithCountry{Security: models.Security{Name: "Some Generic ETF"}, Country: "USA", Currency: ""},
			wantTrue: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := repository.ShouldPreferNonUSForETF(&tc.etf)
			if got != tc.wantTrue {
				t.Errorf("ShouldPreferNonUSForETF(%q, country=%q, currency=%q) = %v, want %v",
					tc.etf.Name, tc.etf.Country, tc.etf.Currency, got, tc.wantTrue)
			}
		})
	}
}

// TestGetMultipleBySymbolsEmpty tests that empty input returns empty map
func TestGetMultipleBySymbolsEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	repo := repository.NewSecurityRepository(pool)

	result, err := repo.GetMultipleBySymbols(context.Background(), []string{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected empty map, got %d entries", len(result))
	}
}

// TestGetMultipleBySymbolsMultipleValid tests fetching multiple existing securities
func TestGetMultipleBySymbolsMultipleValid(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test securities (on US exchange via createTestStock)
	tickers := []string{"TSTSYM1", "TSTSYM2", "TSTSYM3"}
	names := []string{"Test Symbol One", "Test Symbol Two", "Test Symbol Three"}
	createdIDs := make(map[string]int64)

	for i, ticker := range tickers {
		id, err := createTestStock(pool, ticker, names[i])
		if err != nil {
			t.Fatalf("Failed to insert test security %s: %v", ticker, err)
		}
		createdIDs[ticker] = id
	}
	defer func() {
		for _, ticker := range tickers {
			cleanupTestSecurity(pool, ticker)
		}
	}()

	// Test: Fetch all three by symbol
	repo := repository.NewSecurityRepository(pool)
	result, err := repo.GetMultipleBySymbols(ctx, tickers)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 securities, got %d", len(result))
	}

	for ticker, expectedID := range createdIDs {
		candidates, exists := result[ticker]
		if !exists {
			t.Errorf("Security %s not found in result", ticker)
			continue
		}
		sec := repository.PreferUSListing(candidates)
		if sec == nil {
			t.Errorf("Security %s: PreferUSListing returned nil", ticker)
			continue
		}
		if sec.ID != expectedID {
			t.Errorf("Security %s: expected ID %d, got %d", ticker, expectedID, sec.ID)
		}
		if sec.Symbol != ticker {
			t.Errorf("Security %s: expected symbol %s, got %s", ticker, ticker, sec.Symbol)
		}
	}
}

// TestGetMultipleBySymbolsMixedValidInvalid tests that only valid symbols are returned
func TestGetMultipleBySymbolsMixedValidInvalid(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create one test security
	ticker := "TSTMIXED1"
	id, err := createTestStock(pool, ticker, "Test Mixed One")
	if err != nil {
		t.Fatalf("Failed to insert test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	// Test: Fetch with mix of valid and invalid symbols
	repo := repository.NewSecurityRepository(pool)
	result, err := repo.GetMultipleBySymbols(ctx, []string{ticker, "NONEXISTENT123", "ALSONOTREAL456"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("Expected 1 security (only the valid one), got %d", len(result))
	}

	candidates, exists := result[ticker]
	if !exists {
		t.Errorf("Valid security %s not found in result", ticker)
	} else {
		sec := repository.PreferUSListing(candidates)
		if sec == nil {
			t.Errorf("PreferUSListing returned nil for %s", ticker)
		} else if sec.ID != id {
			t.Errorf("Expected ID %d, got %d", id, sec.ID)
		}
	}
}

// TestIsDevelopedMarket verifies the MSCI-aligned developed-market classification.
func TestIsDevelopedMarket(t *testing.T) {
	cases := []struct {
		country string
		want    bool
	}{
		// Developed markets
		{"USA", true},
		{"Canada", true},
		{"Germany", true},
		{"UK", true},
		{"France", true},
		{"Switzerland", true},
		{"Japan", true},
		{"Australia", true},
		{"Hong Kong", true},
		{"Singapore", true},
		{"Korea", true},
		{"Taiwan", true},
		{"Israel", true},
		// Emerging / frontier / unknown
		{"Zambia", false},
		{"Nigeria", false},
		{"Vietnam", false},
		{"Mexico", false},
		{"Brazil", false},
		{"Unknown", false},
		{"Unkown", false}, // typo present in dim_exchanges
		{"", false},
	}

	for _, tc := range cases {
		t.Run(tc.country, func(t *testing.T) {
			got := repository.IsDevelopedMarket(tc.country)
			if got != tc.want {
				t.Errorf("IsDevelopedMarket(%q) = %v, want %v", tc.country, got, tc.want)
			}
		})
	}
}

// TestIsEmergingMarketsETF verifies emerging/frontier keyword detection in ETF names.
func TestIsEmergingMarketsETF(t *testing.T) {
	makeSec := func(name, country, currency string) *models.SecurityWithCountry {
		return &models.SecurityWithCountry{
			Security: models.Security{Name: name},
			Country:  country,
			Currency: currency,
		}
	}

	cases := []struct {
		name string
		etf  *models.SecurityWithCountry
		want bool
	}{
		{
			name: "developed world ex-US ETF",
			etf:  makeSec("SPDR Portfolio Developed World ex-US ETF", "USA", "USD"),
			want: false,
		},
		{
			name: "Avantis emerging markets equity ETF",
			etf:  makeSec("Avantis Emerging Markets Equity ETF", "USA", "USD"),
			want: true,
		},
		{
			name: "iShares MSCI Emerging Markets ETF",
			etf:  makeSec("iShares Core MSCI Emerging Markets ETF", "USA", "USD"),
			want: true,
		},
		{
			name: "iShares MSCI Frontier 100 ETF",
			etf:  makeSec("iShares MSCI Frontier 100 ETF", "USA", "USD"),
			want: true,
		},
		{
			name: "Vanguard Total International Stock ETF",
			etf:  makeSec("Vanguard Total International Stock ETF", "USA", "USD"),
			want: false,
		},
		{
			name: "Amundi MSCI Emerging Markets UCITS ETF (non-US)",
			etf:  makeSec("Amundi MSCI Emerging Markets UCITS ETF", "Germany", "EUR"),
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := repository.IsEmergingMarketsETF(tc.etf)
			if got != tc.want {
				t.Errorf("IsEmergingMarketsETF(%q) = %v, want %v", tc.etf.Name, got, tc.want)
			}
		})
	}
}

// TestPreferDevelopedNonUSListing verifies resolution for developed-world ex-US ETFs.
func TestPreferDevelopedNonUSListing(t *testing.T) {
	mkSec := func(id int64, country, currency string) *models.SecurityWithCountry {
		return &models.SecurityWithCountry{
			Security: models.Security{ID: id, Symbol: "SHOP"},
			Country:  country,
			Currency: currency,
		}
	}

	eightGerman := make([]*models.SecurityWithCountry, 8)
	for i := range eightGerman {
		eightGerman[i] = &models.SecurityWithCountry{
			Security: models.Security{ID: int64(i + 1), Symbol: "PFE"},
			Country:  "Germany",
			Currency: "EUR",
		}
	}

	cases := []struct {
		name       string
		candidates []*models.SecurityWithCountry
		wantID     int64 // 0 means expect nil
	}{
		{
			name: "SHOP scenario: Zambia + Canada + USA → Canada wins (developed non-US)",
			candidates: []*models.SecurityWithCountry{
				mkSec(110717, "Zambia", "ZMW"),
				mkSec(134760, "Canada", "CAD"),
				mkSec(177882, "USA", "USD"),
			},
			wantID: 134760,
		},
		{
			name: "single Canada listing",
			candidates: []*models.SecurityWithCountry{
				mkSec(1, "Canada", "CAD"),
			},
			wantID: 1,
		},
		{
			name: "two developed non-US (Germany + Canada) → first (Germany)",
			candidates: []*models.SecurityWithCountry{
				mkSec(10, "Germany", "EUR"),
				mkSec(20, "Canada", "CAD"),
			},
			wantID: 10,
		},
		{
			name:       "eight German listings → never nil, returns first",
			candidates: eightGerman,
			wantID:     1,
		},
		{
			name: "only emerging non-US (Nigeria + Zambia) → first emerging (Nigeria)",
			candidates: []*models.SecurityWithCountry{
				mkSec(5, "Nigeria", "NGN"),
				mkSec(6, "Zambia", "ZMW"),
			},
			wantID: 5,
		},
		{
			name: "no non-US (USA only) → USA as last resort",
			candidates: []*models.SecurityWithCountry{
				mkSec(99, "USA", "USD"),
			},
			wantID: 99,
		},
		{
			name:       "empty candidates → nil",
			candidates: []*models.SecurityWithCountry{},
			wantID:     0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := repository.PreferDevelopedNonUSListing(tc.candidates)
			if tc.wantID == 0 {
				if got != nil {
					t.Errorf("expected nil, got ID %d", got.ID)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected ID %d, got nil", tc.wantID)
			}
			if got.ID != tc.wantID {
				t.Errorf("expected ID %d, got ID %d", tc.wantID, got.ID)
			}
		})
	}
}

// TestPreferEmergingNonUSListing verifies resolution for emerging-market ETFs.
func TestPreferEmergingNonUSListing(t *testing.T) {
	mkSec := func(id int64, country, currency string) *models.SecurityWithCountry {
		return &models.SecurityWithCountry{
			Security: models.Security{ID: id, Symbol: "TEST"},
			Country:  country,
			Currency: currency,
		}
	}

	cases := []struct {
		name       string
		candidates []*models.SecurityWithCountry
		wantID     int64 // 0 means expect nil
	}{
		{
			name: "Zambia + Canada + USA → Zambia wins (emerging non-US)",
			candidates: []*models.SecurityWithCountry{
				mkSec(1, "Zambia", "ZMW"),
				mkSec(2, "Canada", "CAD"),
				mkSec(3, "USA", "USD"),
			},
			wantID: 1,
		},
		{
			name: "Canada + Germany only (no emerging) → Canada (first developed fallback)",
			candidates: []*models.SecurityWithCountry{
				mkSec(10, "Canada", "CAD"),
				mkSec(20, "Germany", "EUR"),
			},
			wantID: 10,
		},
		{
			name: "USA only → USA as last resort",
			candidates: []*models.SecurityWithCountry{
				mkSec(99, "USA", "USD"),
			},
			wantID: 99,
		},
		{
			name:       "empty candidates → nil",
			candidates: []*models.SecurityWithCountry{},
			wantID:     0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := repository.PreferEmergingNonUSListing(tc.candidates)
			if tc.wantID == 0 {
				if got != nil {
					t.Errorf("expected nil, got ID %d", got.ID)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected ID %d, got nil", tc.wantID)
			}
			if got.ID != tc.wantID {
				t.Errorf("expected ID %d, got ID %d", tc.wantID, got.ID)
			}
		})
	}
}

// TestETFResolverRouting verifies that the ShouldPreferNonUSForETF + IsEmergingMarketsETF
// combination routes each ETF type to the correct resolver function.
func TestETFResolverRouting(t *testing.T) {
	cases := []struct {
		name         string
		etf          models.SecurityWithCountry
		wantNonUS    bool // ShouldPreferNonUSForETF result
		wantEmerging bool // IsEmergingMarketsETF result (only meaningful if wantNonUS=true)
	}{
		{
			name:         "SPY → PreferUSListing (US S&P 500 index)",
			etf:          models.SecurityWithCountry{Security: models.Security{Name: "SPDR S&P 500 ETF Trust"}, Country: "USA", Currency: "USD"},
			wantNonUS:    false,
			wantEmerging: false,
		},
		{
			name:         "SPDW (ex-US developed) → PreferDevelopedNonUSListing",
			etf:          models.SecurityWithCountry{Security: models.Security{Name: "SPDR Portfolio Developed World ex-US ETF"}, Country: "USA", Currency: "USD"},
			wantNonUS:    true,
			wantEmerging: false,
		},
		{
			name:         "AVEM (emerging markets) → PreferEmergingNonUSListing",
			etf:          models.SecurityWithCountry{Security: models.Security{Name: "Avantis Emerging Markets Equity ETF"}, Country: "USA", Currency: "USD"},
			wantNonUS:    true,
			wantEmerging: true,
		},
		{
			name:         "VWO (Vanguard FTSE Emerging) → PreferEmergingNonUSListing",
			etf:          models.SecurityWithCountry{Security: models.Security{Name: "Vanguard FTSE Emerging Markets ETF"}, Country: "USA", Currency: "USD"},
			wantNonUS:    true,
			wantEmerging: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotNonUS := repository.ShouldPreferNonUSForETF(&tc.etf)
			if gotNonUS != tc.wantNonUS {
				t.Errorf("ShouldPreferNonUSForETF = %v, want %v", gotNonUS, tc.wantNonUS)
			}
			if gotNonUS {
				gotEmerging := repository.IsEmergingMarketsETF(&tc.etf)
				if gotEmerging != tc.wantEmerging {
					t.Errorf("IsEmergingMarketsETF = %v, want %v", gotEmerging, tc.wantEmerging)
				}
			}
		})
	}
}

// TestGetMultipleBySymbolsDuplicates tests that duplicate symbols in input are handled correctly
func TestGetMultipleBySymbolsDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	// Setup: Create test security
	ticker := "TSTDUP1"
	id, err := createTestStock(pool, ticker, "Test Duplicate One")
	if err != nil {
		t.Fatalf("Failed to insert test security: %v", err)
	}
	defer cleanupTestSecurity(pool, ticker)

	// Test: Fetch with duplicate symbols in input
	repo := repository.NewSecurityRepository(pool)
	result, err := repo.GetMultipleBySymbols(ctx, []string{ticker, ticker, ticker})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should only return one map key (slice may have multiple entries if multiple exchanges)
	if len(result) != 1 {
		t.Errorf("Expected 1 map key (duplicates collapsed), got %d", len(result))
	}

	candidates, exists := result[ticker]
	if !exists {
		t.Errorf("Security %s not found in result", ticker)
	} else {
		sec := repository.PreferUSListing(candidates)
		if sec == nil {
			t.Errorf("PreferUSListing returned nil for %s", ticker)
		} else if sec.ID != id {
			t.Errorf("Expected ID %d, got %d", id, sec.ID)
		}
	}
}


package tests

import (
	"context"
	"math"
	"testing"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
)

// magsHoldings returns MAGS-like fixture data based on real AlphaVantage output.
func magsHoldings() []alphavantage.ParsedETFHolding {
	return []alphavantage.ParsedETFHolding{
		{Symbol: "n/a", Name: "NVIDIA CORP SWAP", Percentage: 0.0886},
		{Symbol: "FGXXX", Name: "FIRST AMERICAN GOVERNMENT OBLIGS X", Percentage: 0.0721},
		{Symbol: "n/a", Name: "ALPHABET INC SWAP GS", Percentage: 0.0589},
		{Symbol: "n/a", Name: "AMAZON.COM INC SWAP", Percentage: 0.0576},
		{Symbol: "AMZN", Name: "AMAZON.COM INC", Percentage: 0.0562},
		{Symbol: "n/a", Name: "ALPHABET INC-CL A SWAP", Percentage: 0.0534},
		{Symbol: "NVDA", Name: "NVIDIA CORP", Percentage: 0.0533},
		{Symbol: "n/a", Name: "MICROSOFT CORP SWAP", Percentage: 0.0532},
		{Symbol: "MSFT", Name: "MICROSOFT CORP", Percentage: 0.0526},
		{Symbol: "TSLA", Name: "TESLA INC", Percentage: 0.0522},
		{Symbol: "META", Name: "META PLATFORMS INC CLASS A", Percentage: 0.0513},
		{Symbol: "n/a", Name: "META PLATFORMS INC-CLASS A SWAP", Percentage: 0.0512},
		{Symbol: "AAPL", Name: "APPLE INC", Percentage: 0.0503},
		{Symbol: "n/a", Name: "TESLA INC SWAP", Percentage: 0.0445},
		{Symbol: "n/a", Name: "APPLE INC SWAP", Percentage: 0.0442},
		{Symbol: "n/a", Name: "TESLA INC SWAP GS", Percentage: 0.0423},
		{Symbol: "GOOGL", Name: "ALPHABET INC CLASS A", Percentage: 0.0415},
		{Symbol: "n/a", Name: "APPLE INC SWAP GS", Percentage: 0.0409},
		{Symbol: "n/a", Name: "AMAZON INC SWAP GS", Percentage: 0.0358},
		{Symbol: "n/a", Name: "MICROSOFT CORP SWAP GS", Percentage: 0.0344},
		{Symbol: "n/a", Name: "META PLATFORMS INC SWAP GS", Percentage: 0.0342},
		{Symbol: "n/a", Name: "US DOLLARS", Percentage: 0.0053},
		{Symbol: "n/a", Name: "OTHER ASSETS AND LIABILITIES", Percentage: -0.0257},
		{Symbol: "n/a", Name: "CASH OFFSET", Percentage: -0.5705},
	}
}

func TestResolveSwapHoldings_MAGSData(t *testing.T) {
	resolved, unresolved := services.ResolveSwapHoldings(magsHoldings())

	// Should have 8 resolved holdings: 7 equities + FGXXX
	// (AMAZON INC SWAP GS now matches AMAZON.COM INC because .COM is stripped)
	if len(resolved) != 8 {
		t.Fatalf("expected 8 resolved holdings, got %d", len(resolved))
	}

	// Build a map of resolved symbols → percentages
	resolvedMap := make(map[string]float64)
	for _, h := range resolved {
		resolvedMap[h.Symbol] = h.Percentage
	}

	// NVDA should have its own 0.0533 + SWAP 0.0886 = 0.1419
	assertClose(t, "NVDA", resolvedMap["NVDA"], 0.1419, 0.0001)

	// AMZN: 0.0562 + 0.0576 + 0.0358 = 0.1496
	// "AMAZON.COM INC" normalizes to "AMAZON INC" (.COM stripped)
	// "AMAZON.COM INC SWAP" → base "AMAZON INC" → matches ✓
	// "AMAZON INC SWAP GS" → base "AMAZON INC" → matches ✓ (now that .COM is stripped from real holding)
	assertClose(t, "AMZN", resolvedMap["AMZN"], 0.1496, 0.0001)

	// MSFT: 0.0526 + 0.0532 + 0.0344 = 0.1402
	assertClose(t, "MSFT", resolvedMap["MSFT"], 0.1402, 0.0001)

	// TSLA: 0.0522 + 0.0445 + 0.0423 = 0.1390
	assertClose(t, "TSLA", resolvedMap["TSLA"], 0.1390, 0.0001)

	// META: 0.0513 + 0.0512 + 0.0342 = 0.1367
	assertClose(t, "META", resolvedMap["META"], 0.1367, 0.0001)

	// AAPL: 0.0503 + 0.0442 + 0.0409 = 0.1354
	assertClose(t, "AAPL", resolvedMap["AAPL"], 0.1354, 0.0001)

	// GOOGL: 0.0415 + 0.0589 + 0.0534 = 0.1538
	// "ALPHABET INC SWAP GS" → base "ALPHABET INC" matches "ALPHABET INC CLASS A" → "ALPHABET INC" ✓
	// "ALPHABET INC-CL A SWAP" → base "ALPHABET INC" matches ✓
	assertClose(t, "GOOGL", resolvedMap["GOOGL"], 0.1538, 0.0001)

	// FGXXX stays as-is (real symbol, not n/a)
	assertClose(t, "FGXXX", resolvedMap["FGXXX"], 0.0721, 0.0001)

	// Unresolved should contain: US DOLLARS, OTHER ASSETS AND LIABILITIES, CASH OFFSET
	// Note: AMAZON INC SWAP GS now resolves because .COM is stripped from company names
	unresolvedNames := make(map[string]bool)
	for _, h := range unresolved {
		unresolvedNames[h.Name] = true
	}

	if !unresolvedNames["US DOLLARS"] {
		t.Error("expected US DOLLARS in unresolved")
	}
	if !unresolvedNames["OTHER ASSETS AND LIABILITIES"] {
		t.Error("expected OTHER ASSETS AND LIABILITIES in unresolved")
	}
	if !unresolvedNames["CASH OFFSET"] {
		t.Error("expected CASH OFFSET in unresolved")
	}
	if unresolvedNames["AMAZON INC SWAP GS"] {
		t.Error("AMAZON INC SWAP GS should NOT be in unresolved (now matches AMAZON.COM INC)")
	}
}

func TestResolveSwapHoldings_NoSwaps(t *testing.T) {
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "AAPL", Name: "APPLE INC", Percentage: 0.50},
		{Symbol: "MSFT", Name: "MICROSOFT CORP", Percentage: 0.50},
	}

	resolved, unresolved := services.ResolveSwapHoldings(holdings)
	if len(resolved) != 2 {
		t.Fatalf("expected 2 resolved, got %d", len(resolved))
	}
	if len(unresolved) != 0 {
		t.Fatalf("expected 0 unresolved, got %d", len(unresolved))
	}
}

func TestResolveSwapHoldings_AllSwapsNoEquities(t *testing.T) {
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "n/a", Name: "NVIDIA CORP SWAP", Percentage: 0.50},
		{Symbol: "n/a", Name: "APPLE INC SWAP GS", Percentage: 0.30},
	}

	resolved, unresolved := services.ResolveSwapHoldings(holdings)
	// No real holdings to match against, so all swaps go unresolved
	if len(resolved) != 0 {
		t.Fatalf("expected 0 resolved, got %d", len(resolved))
	}
	if len(unresolved) != 2 {
		t.Fatalf("expected 2 unresolved, got %d", len(unresolved))
	}
}

func TestResolveSwapHoldings_NegativeWeightSkipped(t *testing.T) {
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "AAPL", Name: "APPLE INC", Percentage: 0.50},
		{Symbol: "n/a", Name: "CASH OFFSET", Percentage: -0.5705},
	}

	resolved, unresolved := services.ResolveSwapHoldings(holdings)
	if len(resolved) != 1 {
		t.Fatalf("expected 1 resolved, got %d", len(resolved))
	}
	if len(unresolved) != 1 {
		t.Fatalf("expected 1 unresolved, got %d", len(unresolved))
	}
	if unresolved[0].Name != "CASH OFFSET" {
		t.Errorf("expected CASH OFFSET in unresolved, got %s", unresolved[0].Name)
	}
}

func TestNormalizeHoldings_ScalesUp(t *testing.T) {
	ctx, wc := services.NewWarningContext(context.Background())

	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "AAPL", Name: "APPLE INC", Percentage: 0.25},
		{Symbol: "MSFT", Name: "MICROSOFT CORP", Percentage: 0.25},
	}

	result := services.NormalizeHoldings(ctx, holdings, "TEST")

	// Sum was 0.50, should be scaled to 1.0
	var sum float64
	for _, h := range result {
		sum += h.Percentage
	}
	assertClose(t, "normalized sum", sum, 1.0, 0.0001)
	assertClose(t, "AAPL normalized", result[0].Percentage, 0.50, 0.0001)
	assertClose(t, "MSFT normalized", result[1].Percentage, 0.50, 0.0001)

	// Should have emitted a W1002 warning
	warnings := wc.GetWarnings()
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d", len(warnings))
	}
	if warnings[0].Code != models.WarnPartialETFExpansion {
		t.Errorf("expected warning code %s, got %s", models.WarnPartialETFExpansion, warnings[0].Code)
	}
}

func TestNormalizeHoldings_AlreadyNormalized(t *testing.T) {
	ctx, wc := services.NewWarningContext(context.Background())

	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "AAPL", Name: "APPLE INC", Percentage: 0.60},
		{Symbol: "MSFT", Name: "MICROSOFT CORP", Percentage: 0.40},
	}

	result := services.NormalizeHoldings(ctx, holdings, "TEST")

	// Should not be modified
	assertClose(t, "AAPL unchanged", result[0].Percentage, 0.60, 0.0001)
	assertClose(t, "MSFT unchanged", result[1].Percentage, 0.40, 0.0001)

	// No warning should be emitted
	if len(wc.GetWarnings()) != 0 {
		t.Errorf("expected no warnings, got %d", len(wc.GetWarnings()))
	}
}

func TestNormalizeHoldings_EmptySlice(t *testing.T) {
	ctx, _ := services.NewWarningContext(context.Background())

	result := services.NormalizeHoldings(ctx, nil, "TEST")
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestNormalizeHoldings_NoWarningContextSafe(t *testing.T) {
	// normalizeHoldings should not panic when ctx has no warning collector
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "AAPL", Name: "APPLE INC", Percentage: 0.25},
	}

	result := services.NormalizeHoldings(context.Background(), holdings, "TEST")

	// Should still scale correctly
	assertClose(t, "AAPL normalized", result[0].Percentage, 1.0, 0.0001)
}

func TestResolveSwapHoldings_MAGSCoverage(t *testing.T) {
	// Verify that after resolving swaps, the resolved holdings cover the expected percentage
	resolved, _ := services.ResolveSwapHoldings(magsHoldings())

	var resolvedSum float64
	for _, h := range resolved {
		if h.Percentage > 0 {
			resolvedSum += h.Percentage
		}
	}

	// The 7 equities + their swaps + FGXXX should cover most of the ETF.
	// With .COM stripping, all swaps now match, so resolved ≈ 0.9966 + FGXXX 0.0721 ≈ 1.069
	// Only US DOLLARS (0.0053) goes unresolved (negative-weight items are also unresolved).
	if resolvedSum < 0.95 {
		t.Errorf("expected resolved sum > 0.95, got %.4f", resolvedSum)
	}
	t.Logf("Resolved coverage: %.4f (%.1f%%)", resolvedSum, resolvedSum*100)
}

func makeKnown(symbols ...string) map[string][]*models.SecurityWithCountry {
	m := make(map[string][]*models.SecurityWithCountry, len(symbols))
	for _, s := range symbols {
		m[s] = []*models.SecurityWithCountry{{Security: models.Security{Symbol: s}}}
	}
	return m
}

func TestResolveSymbolVariants_DotToDash(t *testing.T) {
	known := makeKnown("BRK-B", "AAPL")
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "BRK.B", Name: "Berkshire Hathaway", Percentage: 0.10},
		{Symbol: "AAPL", Name: "Apple Inc", Percentage: 0.90},
	}
	result := services.ResolveSymbolVariants(holdings, known)
	if result[0].Symbol != "BRK-B" {
		t.Errorf("expected BRK.B → BRK-B, got %q", result[0].Symbol)
	}
	if result[1].Symbol != "AAPL" {
		t.Errorf("expected AAPL unchanged, got %q", result[1].Symbol)
	}
}

func TestResolveSymbolVariants_DashToDot(t *testing.T) {
	known := makeKnown("BRK.B")
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "BRK-B", Name: "Berkshire Hathaway", Percentage: 1.0},
	}
	result := services.ResolveSymbolVariants(holdings, known)
	if result[0].Symbol != "BRK.B" {
		t.Errorf("expected BRK-B → BRK.B, got %q", result[0].Symbol)
	}
}

func TestResolveSymbolVariants_StrippedFallback(t *testing.T) {
	known := makeKnown("BRKB")
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "BRK.B", Name: "Berkshire Hathaway", Percentage: 1.0},
	}
	result := services.ResolveSymbolVariants(holdings, known)
	if result[0].Symbol != "BRKB" {
		t.Errorf("expected BRK.B → BRKB, got %q", result[0].Symbol)
	}
}

func TestResolveSymbolVariants_NoPunctuationPassthrough(t *testing.T) {
	known := makeKnown("AAPL")
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "UNKN", Name: "Unknown Corp", Percentage: 1.0},
	}
	result := services.ResolveSymbolVariants(holdings, known)
	if result[0].Symbol != "UNKN" {
		t.Errorf("expected UNKN unchanged, got %q", result[0].Symbol)
	}
}

func TestResolveSymbolVariants_NoMatchPassthrough(t *testing.T) {
	// BRK.X has no match under any variant — should come through unchanged
	// so the validation step can emit the warning.
	known := makeKnown("AAPL")
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "BRK.X", Name: "Unknown", Percentage: 1.0},
	}
	result := services.ResolveSymbolVariants(holdings, known)
	if result[0].Symbol != "BRK.X" {
		t.Errorf("expected BRK.X unchanged when no match, got %q", result[0].Symbol)
	}
}

func TestResolveSymbolVariants_PreferDashOverStripped(t *testing.T) {
	// Both BRK-B and BRKB exist; dot-to-dash should win as it's tried first.
	known := makeKnown("BRK-B", "BRKB")
	holdings := []alphavantage.ParsedETFHolding{
		{Symbol: "BRK.B", Name: "Berkshire Hathaway", Percentage: 1.0},
	}
	result := services.ResolveSymbolVariants(holdings, known)
	if result[0].Symbol != "BRK-B" {
		t.Errorf("expected BRK-B preferred over BRKB, got %q", result[0].Symbol)
	}
}

func assertClose(t *testing.T, name string, got, want, epsilon float64) {
	t.Helper()
	if math.Abs(got-want) > epsilon {
		t.Errorf("%s: got %.6f, want %.6f (epsilon %.6f)", name, got, want, epsilon)
	}
}

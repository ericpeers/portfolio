package services

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
)

// resolveSwapHoldings merges total return swap holdings into their underlying
// equity positions. ETFs like MAGS hold both the real equity (symbol: "NVDA",
// name: "NVIDIA CORP") and one or more swap contracts (symbol: "n/a", name:
// "NVIDIA CORP SWAP" or "NVIDIA CORP SWAP GS"). This function matches swaps
// to equities by comparing the base company name and adds the swap weight to
// the equity's weight.
//
// Returns:
//   - resolved: real-symbol holdings (with swap weights merged in)
//   - unresolved: "n/a" holdings that couldn't be matched (cash, liabilities, etc.)
func ResolveSwapHoldings(holdings []alphavantage.ParsedETFHolding) (resolved, unresolved []alphavantage.ParsedETFHolding) {
	var realHoldings []alphavantage.ParsedETFHolding
	var naHoldings []alphavantage.ParsedETFHolding

	for _, h := range holdings {
		//log.Infof("Holding: %s => %s", h.Symbol, h.Name)

		if h.Symbol != "n/a" {
			realHoldings = append(realHoldings, h)
		} else {
			naHoldings = append(naHoldings, h)
		}
	}

	// Build a lookup from normalized company name → index in realHoldings
	nameIndex := make(map[string]int, len(realHoldings))
	for i, h := range realHoldings {
		nameIndex[normalizeCompanyName(h.Name)] = i
	}

	for _, na := range naHoldings {
		//log.Infof("NA Considering: %s", na.Name)
		// Skip negative-weight holdings (CASH OFFSET, liabilities)
		if na.Percentage < 0 {
			unresolved = append(unresolved, na)
			continue
		}

		baseName := extractSwapBaseName(na.Name)
		if baseName == "" {
			// Not a swap holding
			unresolved = append(unresolved, na)
			//log.Infof("Not a swap holding: %s", na.Name)
			continue
		}

		if idx, ok := nameIndex[baseName]; ok {
			realHoldings[idx].Percentage += na.Percentage
			//log.Infof("Resolved swap holding: %s, %f%%", realHoldings[idx].Symbol, na.Percentage*100.0)
		} else {
			unresolved = append(unresolved, na)
		}
	}

	return realHoldings, unresolved
}

// extractSwapBaseName returns the normalized company name from a swap description,
// or "" if the description doesn't look like a swap.
//
// Examples:
//
//	"NVIDIA CORP SWAP"       → "NVIDIA CORP"
//	"NVIDIA CORP SWAP GS"    → "NVIDIA CORP"
//	"ALPHABET INC-CL A SWAP" → "ALPHABET INC"
//	"US DOLLARS"             → ""
func extractSwapBaseName(description string) string {
	upper := strings.ToUpper(strings.TrimSpace(description))

	// Must contain " SWAP" to be treated as a swap
	idx := strings.Index(upper, " SWAP")
	if idx < 0 {
		return ""
	}

	baseName := upper[:idx]
	return normalizeCompanyName(baseName)
}

// normalizeCompanyName strips share-class suffixes and common noise so that
// "ALPHABET INC CLASS A", "ALPHABET INC-CL A", and "ALPHABET INC" all become
// "ALPHABET INC".
func normalizeCompanyName(name string) string {
	upper := strings.ToUpper(strings.TrimSpace(name))

	// Strip domain-like suffixes so "AMAZON.COM INC" → "AMAZON INC"
	domainSuffixes := []string{".COM", ".NET", ".ORG", ".CO"}
	for _, ds := range domainSuffixes {
		upper = strings.ReplaceAll(upper, ds, "")
	}

	// Remove share-class suffixes like "CLASS A", "-CL A", "CL A", "-CLASS A", "INC-CL A"
	// Order matters: try longer patterns first
	classPatterns := []string{
		"-CLASS A", "-CLASS B", "-CLASS C",
		"-CL A", "-CL B", "-CL C",
		" CLASS A", " CLASS B", " CLASS C",
		" CL A", " CL B", " CL C",
	}
	for _, p := range classPatterns {
		if strings.HasSuffix(upper, p) {
			upper = upper[:len(upper)-len(p)]
			break
		}
	}

	// Also handle "META PLATFORMS INC-CLASS A" → strip the hyphenated variant mid-string
	// Already covered by suffix stripping above.

	return strings.TrimSpace(upper)
}

// resolveSpecialSymbols maps well-known non-equity holding types to synthetic
// securities that can be priced. Intended special symbols:
//
//	MONEYMARKET — tracks 3-month US Treasury bill yield (T-bill).
//	              Price source: AV TREASURY_YIELD with maturity=3month.
//
//	USCASH      — flat cash position, 0% return. Always $1.00.
//
//	FGXXX       — First American Government Obligations fund.
//	              Could be added when a data source is found, or
//	              emulated as MONEYMARKET at a -1% offset.
//
// When implemented, this resolver would:
//  1. Scan unresolved holdings for descriptions matching known patterns
//     (e.g., "US DOLLARS" → USCASH, "FIRST AMERICAN GOVERNMENT" → FGXXX)
//  2. Replace them with the corresponding special symbol
//  3. Return remaining unresolved holdings
func ResolveSpecialSymbols(holdings []alphavantage.ParsedETFHolding) (resolved, unresolved []alphavantage.ParsedETFHolding) {
	// TODO: implement when special symbol securities are added to dim_security
	return nil, holdings
}

// normalizeHoldings scales resolved holdings so their percentages sum to 1.0.
// If the pre-normalization sum is already ~1.0 (within epsilon), no scaling or
// warning is emitted. Otherwise a W1002 warning is added to ctx with the
// coverage percentage.
func NormalizeHoldings(ctx context.Context, holdings []alphavantage.ParsedETFHolding, etfSymbol string) []alphavantage.ParsedETFHolding {
	if len(holdings) == 0 {
		return holdings
	}

	var sum float64
	for _, h := range holdings {
		if h.Percentage > 0 {
			sum += h.Percentage
		}
	}

	const epsilon = 0.001
	if math.Abs(sum-1.0) <= epsilon {
		return holdings
	}

	if sum <= 0 {
		return holdings
	}

	// Emit warning about partial coverage
	AddWarning(ctx, models.Warning{
		Code: models.WarnPartialETFExpansion,
		Message: fmt.Sprintf("ETF %s: holdings covered %.1f%% before normalization, %.1f%% was unresolvable",
			etfSymbol, sum*100, (1.0-sum)*100),
	})

	scale := 1.0 / sum
	result := make([]alphavantage.ParsedETFHolding, len(holdings))
	for i, h := range holdings {
		result[i] = h
		if h.Percentage > 0 {
			result[i].Percentage = h.Percentage * scale
		}
	}
	return result
}

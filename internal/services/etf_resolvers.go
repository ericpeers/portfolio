package services

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/epeers/portfolio/internal/alphavantage"
	"github.com/epeers/portfolio/internal/models"
	log "github.com/sirupsen/logrus"
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
		// Skip negative-weight holdings (CASH OFFSET, liabilities)
		if na.Percentage < 0 {
			unresolved = append(unresolved, na)
			continue
		}

		baseName := extractSwapBaseName(na.Name)
		if baseName == "" {
			// Not a swap holding
			unresolved = append(unresolved, na)
			continue
		}

		if idx, ok := nameIndex[baseName]; ok {
			realHoldings[idx].Percentage += na.Percentage
		} else {
			log.Errorf("ResolveSwapHoldings: swap %q (base %q) has no matching equity position", na.Name, baseName)
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

// CheckSourceSum inspects the raw holdings before any resolution and emits
// W1003 if the source data does not add up to 100%. Integer arithmetic is
// used to avoid floating-point accumulation: each percentage is multiplied
// by 10000 and truncated to int64, giving 2-decimal-place precision (e.g.
// 7.83% → 783, 0.01% → 1). The expected sum is 10000 (= 100.00%).
func CheckSourceSum(ctx context.Context, holdings []alphavantage.ParsedETFHolding, etfSymbol string) {
	var sum int64
	for _, h := range holdings {
		sum += int64(h.Percentage * 10000)
	}
	if sum != 10000 {
		AddWarning(ctx, models.Warning{
			Code:    models.WarnETFSourceIncomplete,
			Message: fmt.Sprintf("ETF %s: source data sums to %.2f%%, expected 100%%", etfSymbol, float64(sum)/100.0),
		})
	}
}

// ResolveSymbolVariants rewrites symbols that aren't found verbatim in
// knownSecurities by trying punctuation variants. Fidelity CSVs use "." as a
// separator (e.g. BRK.B) while the database may store the same security with
// "-" (BRK-B) or no separator (BRKB), and vice versa.
//
// For each holding whose symbol is not in knownSecurities, the following
// candidates are tried in order and the first match wins:
//  1. Replace all "." with "-"  (BRK.B  → BRK-B)
//  2. Replace all "-" with "."  (BRK-B  → BRK.B)
//  3. Strip all "." and "-"     (BRK.B  → BRKB, BRK-B → BRKB)
//
// Holdings whose symbol is already known, or that contain neither "." nor "-",
// are returned unchanged. Holdings that still don't match after all variants
// are also returned unchanged — they will be dropped by the validation step.
func ResolveSymbolVariants(holdings []alphavantage.ParsedETFHolding, knownSecurities map[string][]*models.SecurityWithCountry) []alphavantage.ParsedETFHolding {
	result := make([]alphavantage.ParsedETFHolding, len(holdings))
	for i, h := range holdings {
		if len(knownSecurities[h.Symbol]) > 0 {
			result[i] = h
			continue
		}
		if !strings.ContainsAny(h.Symbol, ".-") {
			result[i] = h
			continue
		}
		candidates := []string{
			strings.ReplaceAll(h.Symbol, ".", "-"),
			strings.ReplaceAll(h.Symbol, "-", "."),
			strings.NewReplacer(".", "", "-", "").Replace(h.Symbol),
		}
		resolved := false
		for _, candidate := range candidates {
			if candidate == h.Symbol {
				continue
			}
			if len(knownSecurities[candidate]) > 0 {
				//log.Debugf("ResolveSymbolVariants: %q → %q", h.Symbol, candidate)
				h.Symbol = candidate
				resolved = true
				break
			}
		}
		if !resolved {
			log.Warnf("ResolveSymbolVariants: no variant found for %q", h.Symbol)
		}
		result[i] = h
	}
	return result
}

// ResolveSpecialSymbols maps well-known cash/collateral holding names to their
// corresponding securities in the database. Matched holdings have their
// percentages accumulated into a single resolved holding.
//
// Handled patterns (case-insensitive):
//   - "USD CASH"                   → symbol "US DOLLAR"
//   - "CASH COLLATERAL USD ..."    → symbol "US DOLLAR" (prefix match)
//
// Multiple matched holdings are merged by summing their percentages, mirroring
// how ResolveSwapHoldings accumulates swap weights into equity positions.
func ResolveSpecialSymbols(holdings []alphavantage.ParsedETFHolding) (resolved, unresolved []alphavantage.ParsedETFHolding) {
	var usdCashTotal float64
	usdCashFound := false

	for _, h := range holdings {
		upper := strings.ToUpper(strings.TrimSpace(h.Name))
		if upper == "USD CASH" || strings.HasPrefix(upper, "CASH COLLATERAL USD") {
			usdCashTotal += h.Percentage
			usdCashFound = true
		} else {
			unresolved = append(unresolved, h)
		}
	}

	if usdCashFound {
		resolved = append(resolved, alphavantage.ParsedETFHolding{
			Symbol:     "US DOLLAR",
			Name:       "USD CASH",
			Percentage: usdCashTotal,
		})
	}

	return resolved, unresolved
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

	// Emit warning that resolved weights were scaled. The gap may be from dropped
	// unresolvable holdings (each emits its own W1001), from the source data not
	// summing to 100% (emits W1003 earlier), or both.
	AddWarning(ctx, models.Warning{
		Code:    models.WarnPartialETFExpansion,
		Message: fmt.Sprintf("ETF %s: holdings summed to %.1f%% before normalization, scaled to 100%%", etfSymbol, sum*100),
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

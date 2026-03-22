package services

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	log "github.com/sirupsen/logrus"
)

// ResolveSwapHoldings merges total return swap holdings into their underlying
// equity positions. ETFs like MAGS hold both the real equity (symbol: "NVDA",
// name: "NVIDIA CORP") and one or more swap contracts (symbol: "n/a", name:
// "NVIDIA CORP SWAP" or "NVIDIA CORP SWAP GS"). This function matches swaps
// to equities by comparing the base company name and adds the swap weight to
// the equity's weight.
//
// Returns:
//   - resolved: real-symbol holdings (with swap weights merged in)
//   - unresolved: "n/a" holdings that couldn't be matched (cash, liabilities, etc.)
func ResolveSwapHoldings(holdings []providers.ParsedETFHolding) (resolved, unresolved []providers.ParsedETFHolding) {
	var realHoldings []providers.ParsedETFHolding
	var naHoldings []providers.ParsedETFHolding

	for _, h := range holdings {
		//log.Infof("Holding: %s => %s", h.Ticker, h.Name)

		if h.Ticker != "n/a" {
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
func CheckSourceSum(ctx context.Context, holdings []providers.ParsedETFHolding, etfTicker string) {
	var sum int64
	for _, h := range holdings {
		sum += int64(h.Percentage * 10000)
	}
	if sum != 10000 {
		AddWarning(ctx, models.Warning{
			Code:    models.WarnETFSourceIncomplete,
			Message: fmt.Sprintf("ETF %s: source data sums to %.2f%%, expected 100%%", etfTicker, float64(sum)/100.0),
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
//  0. "-F" or "-R" suffix: Fidelity foreign OTC listing — strip suffix and
//     accept the base only if it has a non-US listing.
//     ("BH-F" → "BH" on Thai SET, not "BHF" US stock)
//     ("KTB-R" → "KTB" on Thai SET, not a rights offering)
//  1. Replace all "." with "-"  (BRK.B  → BRK-B)
//  2. Replace all "-" with "."  (BRK-B  → BRK.B)
//  3. Strip all "." and "-"     (BRK.B  → BRKB, BRK-B → BRKB)
//
// Holdings whose symbol is already known, or that contain neither "." nor "-",
// are returned unchanged. Holdings that still don't match after all variants
// are also returned unchanged — they will be dropped by the validation step.
func ResolveSymbolVariants(holdings []providers.ParsedETFHolding, knownSecurities map[string][]*models.SecurityWithCountry) []providers.ParsedETFHolding {
	result := make([]providers.ParsedETFHolding, len(holdings))
	for i, h := range holdings {
		if len(knownSecurities[h.Ticker]) > 0 {
			result[i] = h
			continue
		}
		if !strings.ContainsAny(h.Ticker, ".-") {
			result[i] = h
			continue
		}

		resolved := false

		// Step 0: Fidelity foreign OTC suffixes. Must run BEFORE the generic strip-all
		// step to prevent collisions with real US tickers (e.g. "BH-F" → "BHF").
		//
		// "-F" is Fidelity's marker for a foreign OTC listing (e.g. "BH-F" = Bumrungrad
		// Hospital on Thai SET). This matches the Nasdaq fifth-character suffix spec.
		//
		// "-R" is also used by Fidelity for foreign OTC listings (e.g. "KTB-R", "KBANK-R",
		// "LH-R" on Thai SET). This does NOT follow the Nasdaq spec, where "-R" denotes
		// a rights offering. Fidelity appears to use "-R" for foreign registrar shares.
		// Ref: https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/nasdaqfifthcharactersuffixlist.pdf
		//
		// Only accept the base symbol if it has at least one non-US listing.
		if strings.HasSuffix(h.Ticker, "-F") || strings.HasSuffix(h.Ticker, "-R") {
			base := h.Ticker[:len(h.Ticker)-2]
			for _, s := range knownSecurities[base] {
				if s.Country != "USA" {
					h.Ticker = base
					resolved = true
					break
				}
			}
			if !resolved {
				log.Warnf("ResolveSymbolVariants: no variant found for %q", h.Ticker)
			}
			result[i] = h
			continue
		}

		candidates := []string{
			strings.ReplaceAll(h.Ticker, ".", "-"),
			strings.ReplaceAll(h.Ticker, "-", "."),
			strings.NewReplacer(".", "", "-", "").Replace(h.Ticker),
		}
		for _, candidate := range candidates {
			if candidate == h.Ticker {
				continue
			}
			if len(knownSecurities[candidate]) > 0 {
				h.Ticker = candidate
				resolved = true
				break
			}
		}
		if !resolved {
			log.Warnf("ResolveSymbolVariants: no variant found for %q", h.Ticker)
		}
		result[i] = h
	}
	return result
}

// ResolveByName selects the candidate from candidates whose Name field best
// matches holdingName using word-overlap scoring. Returns nil if no candidate
// shares any meaningful words with holdingName, allowing the caller to fall
// back to exchange-context resolvers.
func ResolveByName(holdingName string, candidates []*models.SecurityWithCountry) *models.SecurityWithCountry {
	holdingWords := nameWords(holdingName)
	if len(holdingWords) == 0 {
		return nil
	}
	var best *models.SecurityWithCountry
	bestScore := 0
	for _, c := range candidates {
		score := 0
		for w := range nameWords(c.Name) {
			if holdingWords[w] {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			best = c
		}
	}
	if bestScore == 0 {
		return nil
	}
	return best
}

// nameWords normalizes a company name to a set of meaningful lowercase words,
// stripping punctuation and dropping tokens of two characters or fewer to avoid
// noise from abbreviations ("of", "co", "plc", etc.).
func nameWords(s string) map[string]bool {
	normalized := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == ' ' {
			return r
		}
		return ' '
	}, s)
	words := strings.Fields(strings.ToLower(normalized))
	result := make(map[string]bool, len(words))
	for _, w := range words {
		if len(w) > 2 {
			result[w] = true
		}
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
func ResolveSpecialSymbols(holdings []providers.ParsedETFHolding) (resolved, unresolved []providers.ParsedETFHolding) {
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
		resolved = append(resolved, providers.ParsedETFHolding{
			Ticker:     "US DOLLAR",
			Name:       "USD CASH",
			Percentage: usdCashTotal,
		})
	}

	return resolved, unresolved
}

// ResolveHoldingsToTickers runs the full ticker-normalization pipeline on raw CSV
// holdings and returns validated, normalized holdings ready for security-ID selection.
//
// Pipeline:
//  1. CheckSourceSum       — warn W1003 if source doesn't sum to 100%
//  2. ResolveSwapHoldings  — merge swap contracts into equity weights
//  3. ResolveSpecialSymbols — map "USD CASH" / "CASH COLLATERAL USD ..." → "US DOLLAR"
//  4. Warn W1001           — for unresolvable n/a holdings
//  5. ResolveSymbolVariants — normalize punctuation (BRK.B→BRK-B) and -F suffixes (BH-F→BH)
//  6. Validate             — drop holdings whose ticker isn't in knownSecurities; warn W1001 per drop
//  7. NormalizeHoldings    — scale to 1.0; warn W1002 if scaling was needed
func ResolveHoldingsToTickers(
	ctx context.Context,
	rawHoldings []providers.ParsedETFHolding,
	etfTicker string,
	knownSecurities map[string][]*models.SecurityWithCountry,
) []providers.ParsedETFHolding {
	CheckSourceSum(ctx, rawHoldings, etfTicker)

	resolved, unresolved := ResolveSwapHoldings(rawHoldings)
	resolved2, unresolved2 := ResolveSpecialSymbols(unresolved)
	resolved = append(resolved, resolved2...)

	for _, uh := range unresolved2 {
		AddWarning(ctx, models.Warning{
			Code:    models.WarnUnresolvedETFHolding,
			Message: fmt.Sprintf("ETF %s: unresolved holding %q (weight %.4f)", etfTicker, uh.Name, uh.Percentage),
		})
	}

	resolved = ResolveSymbolVariants(resolved, knownSecurities)

	var validated []providers.ParsedETFHolding
	for _, h := range resolved {
		if len(knownSecurities[h.Ticker]) > 0 {
			validated = append(validated, h)
		} else {
			AddWarning(ctx, models.Warning{
				Code:    models.WarnUnresolvedETFHolding,
				Message: fmt.Sprintf("ETF %s: symbol %q / Name: %s not found in database (weight %.4f)", etfTicker, h.Ticker, h.Name, h.Percentage),
			})
		}
	}

	return NormalizeHoldings(ctx, validated, etfTicker)
}

// NormalizeHoldings scales resolved holdings so their percentages sum to 1.0.
// If the pre-normalization sum is already ~1.0 (within epsilon), no scaling or
// warning is emitted. Otherwise a W1002 warning is added to ctx with the
// coverage percentage.
func NormalizeHoldings(ctx context.Context, holdings []providers.ParsedETFHolding, etfTicker string) []providers.ParsedETFHolding {
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
		Message: fmt.Sprintf("ETF %s: holdings summed to %.1f%% before normalization, scaled to 100%%", etfTicker, sum*100),
	})

	scale := 1.0 / sum
	result := make([]providers.ParsedETFHolding, len(holdings))
	for i, h := range holdings {
		result[i] = h
		if h.Percentage > 0 {
			result[i].Percentage = h.Percentage * scale
		}
	}
	return result
}

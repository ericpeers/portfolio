package tests

import (
	"math"
	"testing"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
)

// TestSimilarityIdenticalPortfolios tests that identical expanded memberships have 100% similarity
func TestSimilarityIdenticalPortfolios(t *testing.T) {
	// Create a comparison service (we only need the ComputeSimilarity method)
	svc := &services.ComparisonService{}

	// Both portfolios have the same expanded memberships
	// Simulating an ETF that holds TSTA(40%), TSTB(30%), TSTC(30%)
	membershipA := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.40},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.30},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.30},
	}

	membershipB := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.40},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.30},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.30},
	}

	similarity := svc.ComputeSimilarity(membershipA, membershipB)

	if similarity != 1.0 {
		t.Errorf("Expected similarity score 1.0, got %.4f", similarity)
	}

	t.Logf("Identical portfolios similarity: %.4f", similarity)
}

// TestSimilarityDisjointPortfolios tests that portfolios with no overlap have 0% similarity
func TestSimilarityDisjointPortfolios(t *testing.T) {
	svc := &services.ComparisonService{}

	// Portfolio A holds TSTA, TSTB, TSTC
	membershipA := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.40},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.30},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.30},
	}

	// Portfolio B holds TSTD, TSTE - completely different securities
	membershipB := []models.ExpandedMembership{
		{SecurityID: 4, Symbol: "TSTD", Allocation: 0.50},
		{SecurityID: 5, Symbol: "TSTE", Allocation: 0.50},
	}

	similarity := svc.ComputeSimilarity(membershipA, membershipB)

	if similarity != 0.0 {
		t.Errorf("Expected similarity score 0.0, got %.4f", similarity)
	}

	t.Logf("Disjoint portfolios similarity: %.4f", similarity)
}

// TestSimilarityETFvsSimilarStocks tests ETF vs individual stocks with partial match
func TestSimilarityETFvsSimilarStocks(t *testing.T) {
	svc := &services.ComparisonService{}

	// Portfolio A: ETF expanded to TSTA(40%), TSTB(30%), TSTC(30%)
	membershipA := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.40},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.30},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.30},
	}

	// Portfolio B: Direct stocks TSTA(40%), TSTB(30%), TSTC(25%) - missing 5% of TSTC
	membershipB := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.40},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.30},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.25},
	}

	similarity := svc.ComputeSimilarity(membershipA, membershipB)

	// Expected: min(0.40,0.40) + min(0.30,0.30) + min(0.30,0.25) = 0.40 + 0.30 + 0.25 = 0.95
	expected := 0.95
	if similarity != expected {
		t.Errorf("Expected similarity score %.4f, got %.4f", expected, similarity)
	}

	t.Logf("ETF vs similar stocks similarity: %.4f (expected %.4f)", similarity, expected)
}

// TestSimilarityIdealVsCombination tests multi-ETF portfolio vs combination with overlap
func TestSimilarityIdealVsCombination(t *testing.T) {
	svc := &services.ComparisonService{}

	// Portfolio A (Ideal): 50% TSTETF1 + 50% TSTETF2 expanded to:
	// TSTA(20%), TSTB(15%), TSTC(15%), TSTD(25%), TSTE(25%)
	membershipA := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.20},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.15},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.15},
		{SecurityID: 4, Symbol: "TSTD", Allocation: 0.25},
		{SecurityID: 5, Symbol: "TSTE", Allocation: 0.25},
	}

	// Portfolio B (Combination): 60% TSTETF3 + 20% TSTE + 20% TSTC expanded to:
	// TSTA(30%), TSTB(15%), TSTD(15%), TSTE(20%), TSTC(20%)
	membershipB := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.30},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.15},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.20},
		{SecurityID: 4, Symbol: "TSTD", Allocation: 0.15},
		{SecurityID: 5, Symbol: "TSTE", Allocation: 0.20},
	}

	similarity := svc.ComputeSimilarity(membershipA, membershipB)

	// Similarity calculation:
	// TSTA: min(0.20, 0.30) = 0.20
	// TSTB: min(0.15, 0.15) = 0.15
	// TSTC: min(0.15, 0.20) = 0.15
	// TSTD: min(0.25, 0.15) = 0.15
	// TSTE: min(0.25, 0.20) = 0.20
	// Total = 0.20 + 0.15 + 0.15 + 0.15 + 0.20 = 0.85
	expected := 0.85
	if math.Abs(similarity-expected) > 0.0001 {
		t.Errorf("Expected similarity score %.4f, got %.4f", expected, similarity)
	}

	t.Logf("Ideal vs combination similarity: %.4f (expected %.4f)", similarity, expected)
}

// TestSimilarityPartialOverlap tests portfolios with some overlapping securities
func TestSimilarityPartialOverlap(t *testing.T) {
	svc := &services.ComparisonService{}

	// Portfolio A: TSTA(50%), TSTB(50%)
	membershipA := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.50},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.50},
	}

	// Portfolio B: TSTB(30%), TSTC(70%) - only TSTB overlaps
	membershipB := []models.ExpandedMembership{
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.30},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.70},
	}

	similarity := svc.ComputeSimilarity(membershipA, membershipB)

	// Expected: min(0.50,0.30) = 0.30 (only TSTB overlaps)
	expected := 0.30
	if similarity != expected {
		t.Errorf("Expected similarity score %.4f, got %.4f", expected, similarity)
	}

	t.Logf("Partial overlap similarity: %.4f (expected %.4f)", similarity, expected)
}

// TestSimilarityEmptyPortfolios tests that empty portfolios have 0% similarity
func TestSimilarityEmptyPortfolios(t *testing.T) {
	svc := &services.ComparisonService{}

	membershipA := []models.ExpandedMembership{}
	membershipB := []models.ExpandedMembership{}

	similarity := svc.ComputeSimilarity(membershipA, membershipB)

	if similarity != 0.0 {
		t.Errorf("Expected similarity score 0.0, got %.2f", similarity)
	}

	t.Logf("Empty portfolios similarity: %.2f%%", similarity)
}

// TestSimilarityOneEmpty tests that one empty portfolio results in 0% similarity
func TestSimilarityOneEmpty(t *testing.T) {
	svc := &services.ComparisonService{}

	membershipA := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 1.0},
	}
	membershipB := []models.ExpandedMembership{}

	similarity := svc.ComputeSimilarity(membershipA, membershipB)

	if similarity != 0.0 {
		t.Errorf("Expected similarity score 0.0, got %.4f", similarity)
	}

	t.Logf("One empty portfolio similarity: %.4f", similarity)
}

// TestSimilarityClampTo100 tests that floating point errors are clamped to 100%
func TestSimilarityClampTo100(t *testing.T) {
	svc := &services.ComparisonService{}

	// Create memberships that might result in > 1.0 due to floating point issues
	// Using values that add up to exactly 1.0 to verify clamping works
	membershipA := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.33333333333333336},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.33333333333333336},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.33333333333333336},
	}

	membershipB := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.33333333333333336},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.33333333333333336},
		{SecurityID: 3, Symbol: "TSTC", Allocation: 0.33333333333333336},
	}

	similarity := svc.ComputeSimilarity(membershipA, membershipB)

	// The sum might be slightly over 1.0 due to floating point, but should be clamped
	if similarity > 1.0 {
		t.Errorf("Similarity should be clamped to 1.0, got %.15f", similarity)
	}

	t.Logf("Floating point test similarity: %.15f (should be <= 1.0)", similarity)
}

// TestSimilarityAsymmetric verifies the algorithm is symmetric (A vs B == B vs A)
func TestSimilarityAsymmetric(t *testing.T) {
	svc := &services.ComparisonService{}

	membershipA := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.60},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.40},
	}

	membershipB := []models.ExpandedMembership{
		{SecurityID: 1, Symbol: "TSTA", Allocation: 0.30},
		{SecurityID: 2, Symbol: "TSTB", Allocation: 0.70},
	}

	similarityAB := svc.ComputeSimilarity(membershipA, membershipB)
	similarityBA := svc.ComputeSimilarity(membershipB, membershipA)

	if similarityAB != similarityBA {
		t.Errorf("Similarity should be symmetric: A vs B = %.4f, B vs A = %.4f", similarityAB, similarityBA)
	}

	// Expected: min(0.60,0.30) + min(0.40,0.70) = 0.30 + 0.40 = 0.70
	expected := 0.70
	if similarityAB != expected {
		t.Errorf("Expected similarity score %.4f, got %.4f", expected, similarityAB)
	}

	t.Logf("Asymmetric allocation similarity: %.4f (symmetric: %v)", similarityAB, similarityAB == similarityBA)
}

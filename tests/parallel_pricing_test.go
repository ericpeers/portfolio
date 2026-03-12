package tests

import (
	"context"
	"testing"
	"time"

	"github.com/epeers/portfolio/config"
	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers/eodhd"
	"github.com/epeers/portfolio/internal/providers/fred"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

// TestParallelPricingBenchmark measures the speedup from concurrent DB reads in
// ComputeDailyValues when all securities are already cached (no EODHD calls).
// Compares concurrency=1 (sequential) vs concurrency=20 (parallel).
// Run with: go test ./tests/ -run TestParallelPricingBenchmark -v -timeout 300s
func TestParallelPricingBenchmark(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()

	// Use a date range that should be fully covered by cached securities.
	endDate := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	startDate := endDate.AddDate(0, 0, -30)

	// Find securities that have cached price data covering the entire date range.
	rows, err := pool.Query(ctx, `
		SELECT fpr.security_id
		FROM fact_price_range fpr
		WHERE fpr.start_date <= $1 AND fpr.end_date >= $2
		LIMIT 50
	`, startDate, endDate)
	if err != nil {
		t.Fatalf("failed to query fact_price_range: %v", err)
	}

	var memberships []models.PortfolioMembership
	for rows.Next() {
		var secID int64
		if err := rows.Scan(&secID); err != nil {
			t.Fatalf("failed to scan security_id: %v", err)
		}
		memberships = append(memberships, models.PortfolioMembership{
			SecurityID:         secID,
			PercentageOrShares: 1.0,
		})
	}
	rows.Close()

	if len(memberships) < 10 {
		t.Skipf("not enough cached securities (%d < 10), skipping benchmark", len(memberships))
	}
	t.Logf("Benchmark: %d cached securities, date range %s to %s",
		len(memberships), startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

	portfolio := &models.PortfolioWithMemberships{
		Portfolio: models.Portfolio{
			ID:            -999,
			PortfolioType: models.PortfolioTypeActive,
		},
		Memberships: memberships,
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	priceRepo := repository.NewPriceRepository(pool)
	securityRepo := repository.NewSecurityRepository(pool)
	portfolioRepo := repository.NewPortfolioRepository(pool)
	eohdClient := eodhd.NewClient(cfg.EODHDKey)
	fredClient := fred.NewClient(cfg.FREDKey)

	pricingSvc := services.NewPricingService(priceRepo, securityRepo, eohdClient, eohdClient, fredClient, eohdClient).
		WithConcurrency(10)

	// Sequential: priceConcurrency=1
	perfSvcSeq := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo, 1)
	start1 := time.Now()
	_, err = perfSvcSeq.ComputeDailyValues(ctx, portfolio, startDate, endDate)
	elapsed1 := time.Since(start1)
	if err != nil {
		t.Fatalf("sequential ComputeDailyValues failed: %v", err)
	}
	t.Logf("Sequential (priceConcurrency=1):  %v", elapsed1)

	// Parallel: priceConcurrency=20
	perfSvcPar := services.NewPerformanceService(pricingSvc, portfolioRepo, securityRepo, 20)
	start2 := time.Now()
	_, err = perfSvcPar.ComputeDailyValues(ctx, portfolio, startDate, endDate)
	elapsed2 := time.Since(start2)
	if err != nil {
		t.Fatalf("parallel ComputeDailyValues failed: %v", err)
	}
	t.Logf("Parallel   (priceConcurrency=20): %v", elapsed2)

	t.Logf("Speedup: %.1fx", float64(elapsed1)/float64(elapsed2))
}

package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/providers"
	"github.com/epeers/portfolio/internal/providers/alphavantage"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/epeers/portfolio/internal/services"
)

// mockBulkFetcher is a configurable BulkFetcher for testing.
type mockBulkFetcher struct {
	eodRecords   []providers.BulkEODRecord
	eventRecords []providers.BulkEventRecord
}

func (m *mockBulkFetcher) GetBulkEOD(_ context.Context, _ string, _ time.Time) ([]providers.BulkEODRecord, error) {
	return m.eodRecords, nil
}
func (m *mockBulkFetcher) GetBulkEvents(_ context.Context, _ string, _ time.Time) ([]providers.BulkEventRecord, error) {
	return m.eventRecords, nil
}
func (m *mockBulkFetcher) GetBulkSplits(_ context.Context, _ string, _ time.Time) ([]providers.BulkEventRecord, error) {
	return nil, nil
}
func (m *mockBulkFetcher) GetBulkDividends(_ context.Context, _ string, _ time.Time) ([]providers.BulkEventRecord, error) {
	return nil, nil
}

// ---- Singleton: sparse data (lightly traded — no data on start date) ----

// TestSingletonNoRefetch_SparseData verifies that when a provider returns data starting
// from startDate+1 (lightly traded security with no tick on startDate), the second request
// for the same range does NOT trigger a re-fetch.
//
// Bug scenario: fetchAndStore stores minDate (Jan 3) as range start. On the second call,
// DetermineFetch sees startDT (Jan 2) < priceRange.StartDate (Jan 3) → re-fetch again.
func TestSingletonNoRefetch_SparseData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := setupPricingTestSecurity(pool, "TSTSPRSE1", "Test Sparse Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTSPRSE1")

	// startDate = Jan 2, 2025 (valid trading day). Mock returns data from Jan 3 only.
	startDate := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC) // Thursday
	dataStart := time.Date(2025, 1, 3, 0, 0, 0, 0, time.UTC) // Friday — first actual data
	endDate := time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC)

	prices1 := generatePriceData(dataStart, endDate)

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avDummy := alphavantage.NewClient("test-key", "http://localhost:9999")

	// --- svc1: initial fetch ---
	var callCount1 int32
	mock1 := createMockPriceServer(prices1, &callCount1)
	defer mock1.Close()

	svc1 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock1.URL),
		Treasury: avDummy,
	})

	_, _, err = svc1.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("First GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Error("Expected provider to be called on first request")
	}

	// The range must start at startDate (Jan 2), NOT minDate (Jan 3).
	var rangeStart time.Time
	err = pool.QueryRow(ctx,
		`SELECT start_date FROM fact_price_range WHERE security_id = $1`, secID,
	).Scan(&rangeStart)
	if err != nil {
		t.Fatalf("Failed to query fact_price_range: %v", err)
	}
	if !rangeStart.Equal(startDate) {
		t.Errorf("fact_price_range.start_date = %s, want %s (startDate, not minDate)",
			rangeStart.Format("2006-01-02"), startDate.Format("2006-01-02"))
	}

	// --- svc2: second request — must NOT call the provider ---
	var callCount2 int32
	mock2 := createMockPriceServer(prices1, &callCount2)
	defer mock2.Close()

	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock2.URL),
		Treasury: avDummy,
	})

	_, _, err = svc2.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("Second GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount2) > 0 {
		t.Errorf("Second request: expected NO provider call (cache sufficient), got %d", callCount2)
	}
}

// ---- Singleton: holiday on start date ----

// TestSingletonNoRefetch_HolidayStart verifies no re-fetch when startDate is an NYSE
// holiday (MLK Day 2025 = Jan 20, Monday). Provider returns data from Jan 21.
func TestSingletonNoRefetch_HolidayStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := setupPricingTestSecurity(pool, "TSTHOLIDAY", "Test Holiday Start Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTHOLIDAY")

	// MLK Day 2025 = Jan 20 (Monday, NYSE holiday). First trading day = Jan 21.
	startDate := time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC) // MLK Day — no market data
	dataStart := time.Date(2025, 1, 21, 0, 0, 0, 0, time.UTC) // first actual data
	endDate := time.Date(2025, 2, 7, 0, 0, 0, 0, time.UTC)

	prices1 := generatePriceData(dataStart, endDate)

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avDummy := alphavantage.NewClient("test-key", "http://localhost:9999")

	var callCount1 int32
	mock1 := createMockPriceServer(prices1, &callCount1)
	defer mock1.Close()

	svc1 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock1.URL),
		Treasury: avDummy,
	})

	_, _, err = svc1.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("First GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Error("Expected provider call on first request")
	}

	var rangeStart time.Time
	err = pool.QueryRow(ctx,
		`SELECT start_date FROM fact_price_range WHERE security_id = $1`, secID,
	).Scan(&rangeStart)
	if err != nil {
		t.Fatalf("Failed to query fact_price_range: %v", err)
	}
	if !rangeStart.Equal(startDate) {
		t.Errorf("fact_price_range.start_date = %s, want %s (holiday startDate, not first-data date)",
			rangeStart.Format("2006-01-02"), startDate.Format("2006-01-02"))
	}

	var callCount2 int32
	mock2 := createMockPriceServer(prices1, &callCount2)
	defer mock2.Close()

	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock2.URL),
		Treasury: avDummy,
	})

	_, _, err = svc2.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("Second GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount2) > 0 {
		t.Errorf("Second request (holiday start): expected NO provider call, got %d", callCount2)
	}
}

// ---- Singleton: weekend on start date ----

// TestSingletonNoRefetch_WeekendStart verifies no re-fetch when startDate is a Saturday.
// Jan 18, 2025 (Sat) → Jan 19 (Sun) → Jan 20 (MLK Day Mon) → data from Jan 21.
func TestSingletonNoRefetch_WeekendStart(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := setupPricingTestSecurity(pool, "TSTWEEKEND", "Test Weekend Start Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTWEEKEND")

	// Saturday Jan 18, 2025. Next trading day is Jan 21 (MLK Day intervenes).
	startDate := time.Date(2025, 1, 18, 0, 0, 0, 0, time.UTC) // Saturday
	dataStart := time.Date(2025, 1, 21, 0, 0, 0, 0, time.UTC) // Tuesday (after Sat+Sun+MLK)
	endDate := time.Date(2025, 2, 7, 0, 0, 0, 0, time.UTC)

	prices1 := generatePriceData(dataStart, endDate)

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avDummy := alphavantage.NewClient("test-key", "http://localhost:9999")

	var callCount1 int32
	mock1 := createMockPriceServer(prices1, &callCount1)
	defer mock1.Close()

	svc1 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock1.URL),
		Treasury: avDummy,
	})

	_, _, err = svc1.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("First GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Error("Expected provider call on first request")
	}

	var rangeStart time.Time
	err = pool.QueryRow(ctx,
		`SELECT start_date FROM fact_price_range WHERE security_id = $1`, secID,
	).Scan(&rangeStart)
	if err != nil {
		t.Fatalf("Failed to query fact_price_range: %v", err)
	}
	if !rangeStart.Equal(startDate) {
		t.Errorf("fact_price_range.start_date = %s, want %s (Saturday startDate)",
			rangeStart.Format("2006-01-02"), startDate.Format("2006-01-02"))
	}

	var callCount2 int32
	mock2 := createMockPriceServer(prices1, &callCount2)
	defer mock2.Close()

	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock2.URL),
		Treasury: avDummy,
	})

	_, _, err = svc2.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("Second GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount2) > 0 {
		t.Errorf("Second request (weekend start): expected NO provider call, got %d", callCount2)
	}
}

// ---- Singleton: holiday Monday creating long weekend ----

// TestSingletonNoRefetch_LongWeekend verifies no re-fetch when startDate is a holiday
// Monday (Labor Day 2025 = Sep 1). Provider returns data from Sep 2.
func TestSingletonNoRefetch_LongWeekend(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := setupPricingTestSecurity(pool, "TSTLONGWKD", "Test Long Weekend Security", &inception)
	if err != nil {
		t.Fatalf("Failed to setup security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTLONGWKD")

	// Labor Day 2025 = Sep 1 (Monday, NYSE holiday). First trading day = Sep 2.
	startDate := time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC) // Labor Day
	dataStart := time.Date(2025, 9, 2, 0, 0, 0, 0, time.UTC) // Tuesday
	endDate := time.Date(2025, 9, 30, 0, 0, 0, 0, time.UTC)

	prices1 := generatePriceData(dataStart, endDate)

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avDummy := alphavantage.NewClient("test-key", "http://localhost:9999")

	var callCount1 int32
	mock1 := createMockPriceServer(prices1, &callCount1)
	defer mock1.Close()

	svc1 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock1.URL),
		Treasury: avDummy,
	})

	_, _, err = svc1.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("First GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Error("Expected provider call on first request")
	}

	var rangeStart time.Time
	err = pool.QueryRow(ctx,
		`SELECT start_date FROM fact_price_range WHERE security_id = $1`, secID,
	).Scan(&rangeStart)
	if err != nil {
		t.Fatalf("Failed to query fact_price_range: %v", err)
	}
	if !rangeStart.Equal(startDate) {
		t.Errorf("fact_price_range.start_date = %s, want %s (Labor Day startDate)",
			rangeStart.Format("2006-01-02"), startDate.Format("2006-01-02"))
	}

	var callCount2 int32
	mock2 := createMockPriceServer(prices1, &callCount2)
	defer mock2.Close()

	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock2.URL),
		Treasury: avDummy,
	})

	_, _, err = svc2.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("Second GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount2) > 0 {
		t.Errorf("Second request (long weekend): expected NO provider call, got %d", callCount2)
	}
}

// ---- Singleton: holiday on Friday that backs into a weekend ----

// TestSingletonNoRefetch_HolidayBacking_Weekend tests the case where the start date is a
// Friday that is an observed NYSE holiday, making Friday+Saturday+Sunday all non-trading.
// Christmas 2021: Dec 25 is Saturday, observed Dec 24 (Friday). First trading day = Dec 27 (Mon).
func TestSingletonNoRefetch_HolidayBacking_Weekend(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	secID, err := setupPricingTestSecurity(pool, "TSTXMAS21", "Test Holiday Backing Weekend", &inception)
	if err != nil {
		t.Fatalf("Failed to setup security: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTXMAS21")

	// Christmas 2021: Dec 25 = Saturday, observed Dec 24 = Friday (NYSE closed).
	// Dec 24 (holiday), Dec 25 (Sat), Dec 26 (Sun) → all non-trading. First trading = Dec 27.
	startDate := time.Date(2021, 12, 24, 0, 0, 0, 0, time.UTC) // observed Christmas (Friday holiday)
	dataStart := time.Date(2021, 12, 27, 0, 0, 0, 0, time.UTC) // Monday after the 3-day gap
	endDate := time.Date(2022, 1, 14, 0, 0, 0, 0, time.UTC)

	prices1 := generatePriceData(dataStart, endDate)

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avDummy := alphavantage.NewClient("test-key", "http://localhost:9999")

	var callCount1 int32
	mock1 := createMockPriceServer(prices1, &callCount1)
	defer mock1.Close()

	svc1 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock1.URL),
		Treasury: avDummy,
	})

	_, _, err = svc1.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("First GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount1) == 0 {
		t.Error("Expected provider call on first request")
	}

	var rangeStart time.Time
	err = pool.QueryRow(ctx,
		`SELECT start_date FROM fact_price_range WHERE security_id = $1`, secID,
	).Scan(&rangeStart)
	if err != nil {
		t.Fatalf("Failed to query fact_price_range: %v", err)
	}
	if !rangeStart.Equal(startDate) {
		t.Errorf("fact_price_range.start_date = %s, want %s (observed Christmas Friday)",
			rangeStart.Format("2006-01-02"), startDate.Format("2006-01-02"))
	}

	var callCount2 int32
	mock2 := createMockPriceServer(prices1, &callCount2)
	defer mock2.Close()

	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock2.URL),
		Treasury: avDummy,
	})

	_, _, err = svc2.GetDailyPrices(ctx, secID, startDate, endDate)
	if err != nil {
		t.Fatalf("Second GetDailyPrices failed: %v", err)
	}
	if atomic.LoadInt32(&callCount2) > 0 {
		t.Errorf("Second request (holiday backing weekend): expected NO provider call, got %d", callCount2)
	}
}

// ---- Bulk: security absent from EOD response ----

// TestBulkNoRefetch_SparseData verifies that when BulkFetchPrices is called and one security
// has no EOD record in the response (lightly traded), that security still gets a
// fact_price_range entry. A subsequent singleton GetDailyPrices for that security must NOT
// trigger a re-fetch.
//
// Bug scenario: ranges slice is built only from the `prices` slice. Securities absent from the
// EOD response never appear in `prices` and therefore get no range update.
func TestBulkNoRefetch_SparseData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	secIDA, err := setupPricingTestSecurity(pool, "TSTBLKA1", "Test Bulk Security A", &inception)
	if err != nil {
		t.Fatalf("Failed to setup secA: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTBLKA1")

	secIDB, err := setupPricingTestSecurity(pool, "TSTBLKB1", "Test Bulk Security B", &inception)
	if err != nil {
		t.Fatalf("Failed to setup secB: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTBLKB1")

	bulkDate := time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC) // Friday Jan 9, 2026 (trading day)

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avDummy := alphavantage.NewClient("test-key", "http://localhost:9999")

	// EOD response only includes secA — secB is "lightly traded" and absent.
	bulkMock := &mockBulkFetcher{
		eodRecords: []providers.BulkEODRecord{
			{Code: "TSTBLKA1", Date: bulkDate, Open: 100, High: 105, Low: 99, AdjClose: 102, Volume: 500000},
		},
	}

	secsByTicker := map[string]*models.Security{
		"TSTBLKA1": {ID: secIDA, Ticker: "TSTBLKA1"},
		"TSTBLKB1": {ID: secIDB, Ticker: "TSTBLKB1"},
	}

	svc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    avDummy,
		Treasury: avDummy,
		Bulk:     bulkMock,
	})

	_, err = svc.BulkFetchPrices(ctx, "US", bulkDate, secsByTicker)
	if err != nil {
		t.Fatalf("BulkFetchPrices failed: %v", err)
	}

	// Both securities must have a fact_price_range row.
	for _, secID := range []int64{secIDA, secIDB} {
		var count int
		if err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM fact_price_range WHERE security_id = $1`, secID,
		).Scan(&count); err != nil {
			t.Fatalf("Failed to query fact_price_range for secID %d: %v", secID, err)
		}
		if count == 0 {
			t.Errorf("secID %d: expected fact_price_range row after bulk fetch (even with no data)", secID)
		}
	}

	// A singleton GetDailyPrices for secB must NOT call the price provider.
	var callCount2 int32
	mock2 := createMockPriceServer(nil, &callCount2)
	defer mock2.Close()

	svc2 := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    alphavantage.NewClient("test-key", mock2.URL),
		Treasury: avDummy,
	})

	_, _, err = svc2.GetDailyPrices(ctx, secIDB, bulkDate, bulkDate)
	if err != nil {
		t.Fatalf("Singleton GetDailyPrices for secB failed: %v", err)
	}
	if atomic.LoadInt32(&callCount2) > 0 {
		t.Errorf("Singleton fetch after bulk: expected NO provider call for secB (range covered), got %d", callCount2)
	}
}

// ---- Bulk: completely empty EOD response ----

// TestBulkNoRefetch_EmptyResponse verifies that when BulkFetchPrices receives an empty EOD
// response (e.g., called for a holiday date via service directly), all known securities in
// secsByTicker still get a fact_price_range entry covering that date.
func TestBulkNoRefetch_EmptyResponse(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool := getTestPool(t)
	ctx := context.Background()

	inception := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	secIDA, err := setupPricingTestSecurity(pool, "TSTBLKEA", "Test Bulk Empty A", &inception)
	if err != nil {
		t.Fatalf("Failed to setup secA: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTBLKEA")

	secIDB, err := setupPricingTestSecurity(pool, "TSTBLKEB", "Test Bulk Empty B", &inception)
	if err != nil {
		t.Fatalf("Failed to setup secB: %v", err)
	}
	defer cleanupTestSecurity(pool, "TSTBLKEB")

	// Use a trading day; an empty response simulates all-lightly-traded or a provider issue.
	bulkDate := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC) // Thursday Jan 8, 2026

	priceRepo := repository.NewPriceRepository(pool)
	secRepo := repository.NewSecurityRepository(pool)
	avDummy := alphavantage.NewClient("test-key", "http://localhost:9999")

	bulkMock := &mockBulkFetcher{} // empty eodRecords

	secsByTicker := map[string]*models.Security{
		"TSTBLKEA": {ID: secIDA, Ticker: "TSTBLKEA"},
		"TSTBLKEB": {ID: secIDB, Ticker: "TSTBLKEB"},
	}

	svc := services.NewPricingService(priceRepo, secRepo, services.PricingClients{
		Price:    avDummy,
		Treasury: avDummy,
		Bulk:     bulkMock,
	})

	_, err = svc.BulkFetchPrices(ctx, "US", bulkDate, secsByTicker)
	if err != nil {
		t.Fatalf("BulkFetchPrices (empty) failed: %v", err)
	}

	// Both securities must have a fact_price_range row even with empty EOD response.
	for _, secID := range []int64{secIDA, secIDB} {
		var count int
		if err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM fact_price_range WHERE security_id = $1`, secID,
		).Scan(&count); err != nil {
			t.Fatalf("Failed to query fact_price_range for secID %d: %v", secID, err)
		}
		if count == 0 {
			t.Errorf("secID %d: expected fact_price_range row even with empty bulk response", secID)
		}
	}
}

package cache

import (
	"sync"
	"time"

	"github.com/epeers/portfolio/internal/models"
)

// MemoryCache provides an in-memory L1 cache for prices and quotes
type MemoryCache struct {
	prices    map[string]priceEntry
	quotes    map[int64]quoteEntry
	priceMu   sync.RWMutex
	quoteMu   sync.RWMutex
	quoteTTL  time.Duration
}

type priceEntry struct {
	data      []models.PriceData
	fetchedAt time.Time
}

type quoteEntry struct {
	quote     *models.Quote
	fetchedAt time.Time
}

// NewMemoryCache creates a new in-memory cache
func NewMemoryCache(quoteTTL time.Duration) *MemoryCache {
	return &MemoryCache{
		prices:   make(map[string]priceEntry),
		quotes:   make(map[int64]quoteEntry),
		quoteTTL: quoteTTL,
	}
}

// priceCacheKey generates a cache key for price data
func priceCacheKey(securityID int64, startDate, endDate time.Time) string {
	return string(rune(securityID)) + startDate.Format("2006-01-02") + endDate.Format("2006-01-02")
}

// GetPrices retrieves cached prices if available
func (c *MemoryCache) GetPrices(securityID int64, startDate, endDate time.Time) ([]models.PriceData, bool) {
	c.priceMu.RLock()
	defer c.priceMu.RUnlock()

	key := priceCacheKey(securityID, startDate, endDate)
	entry, exists := c.prices[key]
	if !exists {
		return nil, false
	}
	return entry.data, true
}

// SetPrices caches price data
func (c *MemoryCache) SetPrices(securityID int64, startDate, endDate time.Time, data []models.PriceData) {
	c.priceMu.Lock()
	defer c.priceMu.Unlock()

	key := priceCacheKey(securityID, startDate, endDate)
	c.prices[key] = priceEntry{
		data:      data,
		fetchedAt: time.Now(),
	}
}

// GetQuote retrieves a cached quote if fresh
func (c *MemoryCache) GetQuote(securityID int64) (*models.Quote, bool) {
	c.quoteMu.RLock()
	defer c.quoteMu.RUnlock()

	entry, exists := c.quotes[securityID]
	if !exists {
		return nil, false
	}
	if time.Since(entry.fetchedAt) > c.quoteTTL {
		return nil, false
	}
	return entry.quote, true
}

// SetQuote caches a quote
func (c *MemoryCache) SetQuote(securityID int64, quote *models.Quote) {
	c.quoteMu.Lock()
	defer c.quoteMu.Unlock()

	c.quotes[securityID] = quoteEntry{
		quote:     quote,
		fetchedAt: time.Now(),
	}
}

// InvalidateQuote removes a quote from the cache
func (c *MemoryCache) InvalidateQuote(securityID int64) {
	c.quoteMu.Lock()
	defer c.quoteMu.Unlock()

	delete(c.quotes, securityID)
}

// Clear removes all cached data
func (c *MemoryCache) Clear() {
	c.priceMu.Lock()
	c.prices = make(map[string]priceEntry)
	c.priceMu.Unlock()

	c.quoteMu.Lock()
	c.quotes = make(map[int64]quoteEntry)
	c.quoteMu.Unlock()
}

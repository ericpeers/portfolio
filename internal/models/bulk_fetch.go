package models

// MinBulkFetchPrices is the minimum matched-price count required for BulkFetchPrices
// to treat a response as complete. A full US-market bulk fetch yields 40,000–48,000
// matched prices; values below this threshold indicate EODHD hasn't finished publishing
// (e.g. fetched minutes after market close). Pass 0 to skip the check entirely (e.g.
// for manual backfills of older dates that may legitimately yield fewer records).
//
// This constant lives in models (rather than services) so that price_repo.go can use it
// in GetLastBulkFetchDate without creating a circular import (repos cannot import services).
const MinBulkFetchPrices = 30000

// BulkFetchResult contains the results of a bulk EODHD price fetch operation.
type BulkFetchResult struct {
	Exchange string `json:"exchange"`
	Date     string `json:"date"`
	Fetched  int    `json:"fetched"`
	Stored   int    `json:"stored"`
	Skipped  int    `json:"skipped"`
}

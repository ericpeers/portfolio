package models

// BulkFetchResult contains the results of a bulk EODHD price fetch operation.
type BulkFetchResult struct {
	Exchange string `json:"exchange"`
	Date     string `json:"date"`
	Fetched  int    `json:"fetched"`
	Stored   int    `json:"stored"`
	Skipped  int    `json:"skipped"`
}

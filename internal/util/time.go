package util

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// NextMarketDate predicts the date of the next stock market update.
// It handles timezone conversion, business day logic.
// It returns the next valid market date (a weekday) at 4:30 PM New York time, in UTC.
func NextMarketDate(input time.Time) time.Time {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("Failed to load location 'America/New_York': %v. Falling back to UTC.", err)
		loc = time.UTC
	}
	nowET := input.In(loc)

	// Start with today at 4:30 PM ET
	next := time.Date(nowET.Year(), nowET.Month(), nowET.Day(), 16, 30, 0, 0, loc)

	// If it's already past 4:30 PM, move to the next day
	if nowET.After(next) {
		next = next.AddDate(0, 0, 1)
	}

	// Skip weekends to find the next business day
	for next.Weekday() == time.Saturday || next.Weekday() == time.Sunday {
		next = next.AddDate(0, 0, 1)
	}

	return next.UTC()
}

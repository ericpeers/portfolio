// Package services contains the business logic layer of the portfolio server.
//
// Each service is responsible for a single domain:
//   - PricingService:     fetch, cache, and serve OHLCV price data and treasury rates
//   - PortfolioService:   CRUD for portfolio records and membership validation
//   - MembershipService:  expand portfolio memberships (ETF → constituent stocks)
//   - ComparisonService:  side-by-side portfolio comparison and similarity scoring
//   - PerformanceService: compute daily values, returns, Sharpe ratio, and dividends
//   - AdminService:       sync securities from external providers, bulk price imports
//   - PrefetchService:    background goroutines that keep the price cache warm
//
// Services call into internal/repository for database I/O and
// internal/providers for external API calls. They must not import handlers.
package services

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// adHocClosures covers NYSE market closures that are not recurring statutory holidays.
// These are one-off closures for national emergencies, presidential mourning periods,
// and natural disasters. Add new entries here as they occur.
var adHocClosures = map[time.Time]bool{
	time.Date(2001, 9, 11, 0, 0, 0, 0, time.UTC): true, // 9/11 terrorist attacks
	time.Date(2001, 9, 12, 0, 0, 0, 0, time.UTC): true,
	time.Date(2001, 9, 13, 0, 0, 0, 0, time.UTC): true,
	time.Date(2001, 9, 14, 0, 0, 0, 0, time.UTC): true,
	time.Date(2004, 6, 11, 0, 0, 0, 0, time.UTC): true, // President Reagan mourning
	time.Date(2007, 1, 2, 0, 0, 0, 0, time.UTC):  true, // President Ford mourning
	time.Date(2012, 10, 29, 0, 0, 0, 0, time.UTC): true, // Hurricane Sandy
	time.Date(2012, 10, 30, 0, 0, 0, 0, time.UTC): true,
	time.Date(2018, 12, 5, 0, 0, 0, 0, time.UTC): true, // President Bush Sr. mourning
	time.Date(2025, 1, 9, 0, 0, 0, 0, time.UTC):  true, // President Carter mourning
}

// easterSundays maps year → Easter Sunday date (UTC midnight) for 1990–2060.
// Good Friday = Easter Sunday − 2 days (always a Friday).
// Dates sourced from the US Naval Observatory / ecclesiastical calendar.
// For years outside this table, buildHolidaySet falls back to the Meeus/Jones/Butcher algorithm.
var easterSundays = map[int]time.Time{
	1990: time.Date(1990, 4, 15, 0, 0, 0, 0, time.UTC),
	1991: time.Date(1991, 3, 31, 0, 0, 0, 0, time.UTC),
	1992: time.Date(1992, 4, 19, 0, 0, 0, 0, time.UTC),
	1993: time.Date(1993, 4, 11, 0, 0, 0, 0, time.UTC),
	1994: time.Date(1994, 4, 3, 0, 0, 0, 0, time.UTC),
	1995: time.Date(1995, 4, 16, 0, 0, 0, 0, time.UTC),
	1996: time.Date(1996, 4, 7, 0, 0, 0, 0, time.UTC),
	1997: time.Date(1997, 3, 30, 0, 0, 0, 0, time.UTC),
	1998: time.Date(1998, 4, 12, 0, 0, 0, 0, time.UTC),
	1999: time.Date(1999, 4, 4, 0, 0, 0, 0, time.UTC),
	2000: time.Date(2000, 4, 23, 0, 0, 0, 0, time.UTC),
	2001: time.Date(2001, 4, 15, 0, 0, 0, 0, time.UTC),
	2002: time.Date(2002, 3, 31, 0, 0, 0, 0, time.UTC),
	2003: time.Date(2003, 4, 20, 0, 0, 0, 0, time.UTC),
	2004: time.Date(2004, 4, 11, 0, 0, 0, 0, time.UTC),
	2005: time.Date(2005, 3, 27, 0, 0, 0, 0, time.UTC),
	2006: time.Date(2006, 4, 16, 0, 0, 0, 0, time.UTC),
	2007: time.Date(2007, 4, 8, 0, 0, 0, 0, time.UTC),
	2008: time.Date(2008, 3, 23, 0, 0, 0, 0, time.UTC),
	2009: time.Date(2009, 4, 12, 0, 0, 0, 0, time.UTC),
	2010: time.Date(2010, 4, 4, 0, 0, 0, 0, time.UTC),
	2011: time.Date(2011, 4, 24, 0, 0, 0, 0, time.UTC),
	2012: time.Date(2012, 4, 8, 0, 0, 0, 0, time.UTC),
	2013: time.Date(2013, 3, 31, 0, 0, 0, 0, time.UTC),
	2014: time.Date(2014, 4, 20, 0, 0, 0, 0, time.UTC),
	2015: time.Date(2015, 4, 5, 0, 0, 0, 0, time.UTC),
	2016: time.Date(2016, 3, 27, 0, 0, 0, 0, time.UTC),
	2017: time.Date(2017, 4, 16, 0, 0, 0, 0, time.UTC),
	2018: time.Date(2018, 4, 1, 0, 0, 0, 0, time.UTC),
	2019: time.Date(2019, 4, 21, 0, 0, 0, 0, time.UTC),
	2020: time.Date(2020, 4, 12, 0, 0, 0, 0, time.UTC),
	2021: time.Date(2021, 4, 4, 0, 0, 0, 0, time.UTC),
	2022: time.Date(2022, 4, 17, 0, 0, 0, 0, time.UTC),
	2023: time.Date(2023, 4, 9, 0, 0, 0, 0, time.UTC),
	2024: time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC),
	2025: time.Date(2025, 4, 20, 0, 0, 0, 0, time.UTC),
	2026: time.Date(2026, 4, 5, 0, 0, 0, 0, time.UTC),
	2027: time.Date(2027, 3, 28, 0, 0, 0, 0, time.UTC),
	2028: time.Date(2028, 4, 16, 0, 0, 0, 0, time.UTC),
	2029: time.Date(2029, 4, 1, 0, 0, 0, 0, time.UTC),
	2030: time.Date(2030, 4, 21, 0, 0, 0, 0, time.UTC),
	2031: time.Date(2031, 4, 13, 0, 0, 0, 0, time.UTC),
	2032: time.Date(2032, 3, 28, 0, 0, 0, 0, time.UTC),
	2033: time.Date(2033, 4, 17, 0, 0, 0, 0, time.UTC),
	2034: time.Date(2034, 4, 9, 0, 0, 0, 0, time.UTC),
	2035: time.Date(2035, 3, 25, 0, 0, 0, 0, time.UTC),
	2036: time.Date(2036, 4, 13, 0, 0, 0, 0, time.UTC),
	2037: time.Date(2037, 4, 5, 0, 0, 0, 0, time.UTC),
	2038: time.Date(2038, 4, 25, 0, 0, 0, 0, time.UTC),
	2039: time.Date(2039, 4, 10, 0, 0, 0, 0, time.UTC),
	2040: time.Date(2040, 4, 1, 0, 0, 0, 0, time.UTC),
	2041: time.Date(2041, 4, 21, 0, 0, 0, 0, time.UTC),
	2042: time.Date(2042, 4, 6, 0, 0, 0, 0, time.UTC),
	2043: time.Date(2043, 3, 29, 0, 0, 0, 0, time.UTC),
	2044: time.Date(2044, 4, 17, 0, 0, 0, 0, time.UTC),
	2045: time.Date(2045, 4, 9, 0, 0, 0, 0, time.UTC),
	2046: time.Date(2046, 3, 25, 0, 0, 0, 0, time.UTC),
	2047: time.Date(2047, 4, 14, 0, 0, 0, 0, time.UTC),
	2048: time.Date(2048, 4, 5, 0, 0, 0, 0, time.UTC),
	2049: time.Date(2049, 4, 18, 0, 0, 0, 0, time.UTC),
	2050: time.Date(2050, 4, 10, 0, 0, 0, 0, time.UTC),
	2051: time.Date(2051, 4, 2, 0, 0, 0, 0, time.UTC),
	2052: time.Date(2052, 4, 21, 0, 0, 0, 0, time.UTC),
	2053: time.Date(2053, 4, 6, 0, 0, 0, 0, time.UTC),
	2054: time.Date(2054, 3, 29, 0, 0, 0, 0, time.UTC),
	2055: time.Date(2055, 4, 18, 0, 0, 0, 0, time.UTC),
	2056: time.Date(2056, 4, 2, 0, 0, 0, 0, time.UTC),
	2057: time.Date(2057, 4, 22, 0, 0, 0, 0, time.UTC),
	2058: time.Date(2058, 4, 14, 0, 0, 0, 0, time.UTC),
	2059: time.Date(2059, 3, 30, 0, 0, 0, 0, time.UTC),
	2060: time.Date(2060, 4, 18, 0, 0, 0, 0, time.UTC),
}

// holidayCache stores per-year holiday sets so they are only computed once.
// Keys are int (year), values are map[time.Time]struct{}.
var holidayCache sync.Map

// IsUSMarketHoliday reports whether d is an NYSE market holiday or ad-hoc closure.
// NYSE observes: New Year's Day, MLK Day, Presidents' Day, Good Friday,
// Memorial Day, Juneteenth (since 2022), Independence Day, Labor Day,
// Thanksgiving, and Christmas — with fixed holidays shifted to the nearest
// weekday when they fall on a weekend (Saturday → Friday, Sunday → Monday).
// Ad-hoc closures (9/11, Hurricane Sandy, presidential mourning days) are also covered.
//
// Holiday sets are computed once per year and cached; repeated calls within the same
// year (e.g. from generateMoneyMarketPrices) pay only an O(1) map lookup.
func IsUSMarketHoliday(d time.Time) bool {
	target := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)

	if adHocClosures[target] {
		return true
	}

	year := d.Year()
	val, ok := holidayCache.Load(year)
	if !ok {
		val, _ = holidayCache.LoadOrStore(year, buildHolidaySet(year))
	}
	set := val.(map[time.Time]struct{})
	_, found := set[target]
	return found
}

// buildHolidaySet computes the full set of NYSE statutory holidays for the given year.
// Called at most once per year; result is stored in holidayCache.
func buildHolidaySet(year int) map[time.Time]struct{} {
	// observedDate returns the weekday a fixed holiday is observed on.
	// Saturday holidays are observed the preceding Friday; Sunday holidays the following Monday.
	observedDate := func(y int, month time.Month, day int) time.Time {
		h := time.Date(y, month, day, 0, 0, 0, 0, time.UTC)
		switch h.Weekday() {
		case time.Saturday:
			return h.AddDate(0, 0, -1)
		case time.Sunday:
			return h.AddDate(0, 0, 1)
		}
		return h
	}

	// nthWeekday returns the nth occurrence of weekday in month/year.
	nthWeekday := func(y int, month time.Month, weekday time.Weekday, n int) time.Time {
		t := time.Date(y, month, 1, 0, 0, 0, 0, time.UTC)
		for t.Weekday() != weekday {
			t = t.AddDate(0, 0, 1)
		}
		return t.AddDate(0, 0, 7*(n-1))
	}

	// lastWeekday returns the last occurrence of weekday in month/year.
	lastWeekday := func(y int, month time.Month, weekday time.Weekday) time.Time {
		t := time.Date(y, month+1, 0, 0, 0, 0, 0, time.UTC)
		for t.Weekday() != weekday {
			t = t.AddDate(0, 0, -1)
		}
		return t
	}

	// goodFriday returns Good Friday for the year. Easter Sunday dates are looked up
	// from the easterSundays table (1990–2060). For out-of-range years the
	// Meeus/Jones/Butcher algorithm is used as a fallback.
	goodFriday := func(y int) time.Time {
		if easter, ok := easterSundays[y]; ok {
			return easter.AddDate(0, 0, -2)
		}
		// Meeus/Jones/Butcher algorithm (fallback for years outside the table)
		a := y % 19
		b := y / 100
		c := y % 100
		d := b / 4
		e := b % 4
		f := (b + 8) / 25
		g := (b - f + 1) / 3
		h := (19*a + b - d - g + 15) % 30
		i := c / 4
		k := c % 4
		l := (32 + 2*e + 2*i - h - k) % 7
		m := (a + 11*h + 22*l) / 451
		month := time.Month((h + l - 7*m + 114) / 31)
		day := ((h + l - 7*m + 114) % 31) + 1
		easter := time.Date(y, month, day, 0, 0, 0, 0, time.UTC)
		return easter.AddDate(0, 0, -2)
	}

	holidays := []time.Time{
		observedDate(year, time.January, 1),             // New Year's Day
		observedDate(year+1, time.January, 1),           // New Year's Day (observed Dec 31 when Jan 1 is Saturday)
		nthWeekday(year, time.January, time.Monday, 3),  // MLK Day
		nthWeekday(year, time.February, time.Monday, 3), // Presidents' Day
		goodFriday(year),                                 // Good Friday
		lastWeekday(year, time.May, time.Monday),         // Memorial Day
		observedDate(year, time.July, 4),                 // Independence Day
		nthWeekday(year, time.September, time.Monday, 1), // Labor Day
		nthWeekday(year, time.November, time.Thursday, 4), // Thanksgiving
		observedDate(year, time.December, 25),            // Christmas
	}
	if year >= 2022 {
		holidays = append(holidays, observedDate(year, time.June, 19)) // Juneteenth
	}

	set := make(map[time.Time]struct{}, len(holidays))
	for _, h := range holidays {
		set[h] = struct{}{}
	}
	return set
}

// NextTradingDay advances t by one calendar day, then keeps advancing until it lands on
// a weekday that is not a NYSE holiday. Returns midnight NY on that day.
//
// The input's Year/Month/Day fields encode the intended calendar date regardless of
// whether t is midnight UTC (as returned by pgx for Postgres DATE columns) or midnight NY.
// Re-anchoring to midnight NY before iterating ensures AddDate advances by one calendar
// day in the NY timezone, avoiding the off-by-one that occurs when midnight UTC is
// converted to NY (EDT = UTC-4) and shows as the previous calendar day's evening.
func NextTradingDay(t time.Time) time.Time {
	nyLoc, _ := time.LoadLocation("America/New_York")
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, nyLoc).AddDate(0, 0, 1)
	for {
		if d.Weekday() != time.Saturday && d.Weekday() != time.Sunday && !IsUSMarketHoliday(d) {
			return d
		}
		d = d.AddDate(0, 0, 1)
	}
}

// countTradingDays counts the number of trading days strictly after from up to and including to.
func countTradingDays(from, to time.Time) int {
	count := 0
	for d := NextTradingDay(from); !d.After(to); d = NextTradingDay(d) {
		count++
	}
	return count
}

// NextMarketDate predicts the date of the next stock market update.
// It handles timezone conversion, business day logic, and NYSE holidays.
// It returns the next target date, in New York time, 4:30pm.
func NextMarketDate(input time.Time) time.Time {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("Failed to load location: %v", err)
		return input.AddDate(0, 0, 1)
	}

	nyTime := input.In(nyLoc)
	cutoffHour, cutoffMinute := 16, 30

	// Create target at 4:30 PM today
	target := time.Date(nyTime.Year(), nyTime.Month(), nyTime.Day(),
		cutoffHour, cutoffMinute, 0, 0, nyLoc)

	isWeekday := nyTime.Weekday() >= time.Monday && nyTime.Weekday() <= time.Friday
	isBeforeCutoff := nyTime.Before(target)
	isTradingDay := isWeekday && !IsUSMarketHoliday(nyTime)

	if !(isTradingDay && isBeforeCutoff) {
		// Roll forward to next trading day
		target = target.AddDate(0, 0, 1)
		for target.Weekday() == time.Saturday || target.Weekday() == time.Sunday || IsUSMarketHoliday(target) {
			target = target.AddDate(0, 0, 1)
		}
	}

	return target
}

// LastMarketClose returns the most recent time end-of-day market data was available.
// If now is on a trading day at or after 4:30 PM ET, that is the last close.
// Otherwise it rolls back to the previous trading day at 4:30 PM ET.
// Use this instead of time.Now() when selecting an endDate for price queries.
func LastMarketClose(now time.Time) time.Time {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("Failed to load location: %v", err)
		return now.AddDate(0, 0, -1)
	}

	nyTime := now.In(nyLoc)
	target := time.Date(nyTime.Year(), nyTime.Month(), nyTime.Day(), 16, 30, 0, 0, nyLoc)

	isWeekday := nyTime.Weekday() >= time.Monday && nyTime.Weekday() <= time.Friday
	isTradingDay := isWeekday && !IsUSMarketHoliday(nyTime)
	isAfterCutoff := !nyTime.Before(target)

	if isTradingDay && isAfterCutoff {
		return target
	}

	// Roll back to previous trading day
	target = target.AddDate(0, 0, -1)
	for target.Weekday() == time.Saturday || target.Weekday() == time.Sunday || IsUSMarketHoliday(target) {
		target = target.AddDate(0, 0, -1)
	}
	return target
}

// PreviousMarketDay returns the most recent trading-day close strictly before today.
// Use in /glance to avoid requesting data that EODHD hasn't bulk-published yet
// (EODHD publishes day-D data at ~4am ET on D+1).
func PreviousMarketDay(now time.Time) time.Time {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("Failed to load location: %v", err)
		return now.AddDate(0, 0, -2)
	}
	nyTime := now.In(nyLoc)
	// Set to yesterday at 23:59 ET. LastMarketClose then returns yesterday's
	// trading-day close (if it was a trading day) or rolls back further — never today.
	yesterday := time.Date(nyTime.Year(), nyTime.Month(), nyTime.Day()-1, 23, 59, 0, 0, nyLoc)
	return LastMarketClose(yesterday)
}

// NextTreasuryUpdateDate predicts the next time FRED DGS10 data will be updated.
// FRED publishes Friday treasury data on the following Monday at 4:30 PM ET,
// so Fridays are always treated as "after cutoff" regardless of the time of day.
func NextTreasuryUpdateDate(input time.Time) time.Time {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Errorf("Failed to load location: %v", err)
		return input.AddDate(0, 0, 1)
	}

	nyTime := input.In(nyLoc)
	target := time.Date(nyTime.Year(), nyTime.Month(), nyTime.Day(), 16, 30, 0, 0, nyLoc)

	// Monday–Thursday before 4:30 PM ET → return today at 4:30 PM
	// Friday (any time), weekends, or after 4:30 PM → roll to next business day
	isWeekdayNotFriday := nyTime.Weekday() >= time.Monday && nyTime.Weekday() <= time.Thursday
	isBeforeCutoff := nyTime.Before(target)

	if !(isWeekdayNotFriday && isBeforeCutoff) {
		target = target.AddDate(0, 0, 1)
		for target.Weekday() == time.Saturday || target.Weekday() == time.Sunday || IsUSMarketHoliday(target) {
			target = target.AddDate(0, 0, 1)
		}
	}

	return target
}

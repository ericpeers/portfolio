package tests

import (
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/services"
)

func date(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func TestIsUSMarketHoliday_KnownHolidays(t *testing.T) {
	t.Parallel()
	holidays := []struct {
		name string
		d    time.Time
	}{
		// 2024
		{"New Year's Day 2024", date(2024, time.January, 1)},
		{"MLK Day 2024", date(2024, time.January, 15)},
		{"Presidents' Day 2024", date(2024, time.February, 19)},
		{"Good Friday 2024", date(2024, time.March, 29)},
		{"Memorial Day 2024", date(2024, time.May, 27)},
		{"Juneteenth 2024", date(2024, time.June, 19)},
		{"Independence Day 2024", date(2024, time.July, 4)},
		{"Labor Day 2024", date(2024, time.September, 2)},
		{"Thanksgiving 2024", date(2024, time.November, 28)},
		{"Christmas 2024", date(2024, time.December, 25)},

		// Weekend observations
		// New Year's 2022 falls on Saturday → observed Dec 31, 2021 (Friday)
		{"New Year's 2022 observed (Dec 31 2021)", date(2021, time.December, 31)},
		// Juneteenth 2023 falls on Monday (no shift needed)
		{"Juneteenth 2023", date(2023, time.June, 19)},
		// Christmas 2021 falls on Saturday → observed Dec 24, 2021 (Friday)
		{"Christmas 2021 observed (Dec 24)", date(2021, time.December, 24)},
		// Independence Day 2021 falls on Sunday → observed Jul 5
		{"Independence Day 2021 observed (Jul 5)", date(2021, time.July, 5)},
	}

	for _, tc := range holidays {
		t.Run(tc.name, func(t *testing.T) {
			if !services.IsUSMarketHoliday(tc.d) {
				t.Errorf("expected %s (%s) to be a market holiday", tc.name, tc.d.Format("2006-01-02"))
			}
		})
	}
}

func TestIsUSMarketHoliday_NotHolidays(t *testing.T) {
	t.Parallel()
	notHolidays := []struct {
		name string
		d    time.Time
	}{
		{"Regular Monday 2024-01-08", date(2024, time.January, 8)},
		{"Regular Wednesday 2024-03-20", date(2024, time.March, 20)},
		{"Day after Thanksgiving 2024", date(2024, time.November, 29)},
		{"Christmas Eve 2024 (not a holiday)", date(2024, time.December, 24)},
		// Juneteenth before it was a holiday
		{"Juneteenth 2021 (not yet a holiday)", date(2021, time.June, 19)},
	}

	for _, tc := range notHolidays {
		t.Run(tc.name, func(t *testing.T) {
			if services.IsUSMarketHoliday(tc.d) {
				t.Errorf("expected %s (%s) NOT to be a market holiday", tc.name, tc.d.Format("2006-01-02"))
			}
		})
	}
}

func TestTradingDaySequenceAroundThanksgiving(t *testing.T) {
	t.Parallel()
	// Verify that IsUSMarketHoliday + weekend logic covers the two-week span
	// around Thanksgiving 2024: Nov 25 (Mon) through Dec 6 (Fri).
	// Expected trading days: Nov 25, 26, 27, Nov 29, Dec 2, 3, 4, 5, 6
	// Excluded: Nov 28 (Thanksgiving), Nov 30 (Sat), Dec 1 (Sun)
	// Note: day-after-Thanksgiving (Nov 29) IS a normal trading day.
	expectedTradingDays := []time.Time{
		date(2024, time.November, 25), // Mon
		date(2024, time.November, 26), // Tue
		date(2024, time.November, 27), // Wed
		// Nov 28 Thanksgiving — holiday
		date(2024, time.November, 29), // Fri — normal trading day
		// Nov 30 Sat, Dec 1 Sun — weekends
		date(2024, time.December, 2),  // Mon
		date(2024, time.December, 3),  // Tue
		date(2024, time.December, 4),  // Wed
		date(2024, time.December, 5),  // Thu
		date(2024, time.December, 6),  // Fri
	}

	start := date(2024, time.November, 25)
	end := date(2024, time.December, 6)

	var got []time.Time
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		if services.IsUSMarketHoliday(d) {
			continue
		}
		got = append(got, d)
	}

	if len(got) != len(expectedTradingDays) {
		t.Fatalf("expected %d trading days, got %d: %v", len(expectedTradingDays), len(got), got)
	}
	for i, d := range got {
		if !d.Equal(expectedTradingDays[i]) {
			t.Errorf("day %d: expected %s, got %s", i, expectedTradingDays[i].Format("2006-01-02"), d.Format("2006-01-02"))
		}
	}
}

func TestIsUSMarketHoliday_AdHocClosures(t *testing.T) {
	t.Parallel()
	closures := []struct {
		name string
		d    time.Time
	}{
		{"9/11 Day 1", date(2001, time.September, 11)},
		{"9/11 Day 2", date(2001, time.September, 12)},
		{"9/11 Day 3", date(2001, time.September, 13)},
		{"9/11 Day 4", date(2001, time.September, 14)},
		{"Reagan mourning 2004", date(2004, time.June, 11)},
		{"Ford mourning 2007", date(2007, time.January, 2)},
		{"Hurricane Sandy Day 1 2012", date(2012, time.October, 29)},
		{"Hurricane Sandy Day 2 2012", date(2012, time.October, 30)},
		{"Bush Sr. mourning 2018", date(2018, time.December, 5)},
		{"Carter mourning 2025", date(2025, time.January, 9)},
	}

	for _, tc := range closures {
		t.Run(tc.name, func(t *testing.T) {
			if !services.IsUSMarketHoliday(tc.d) {
				t.Errorf("expected %s (%s) to be an ad-hoc market closure", tc.name, tc.d.Format("2006-01-02"))
			}
		})
	}
}

func TestIsUSMarketHoliday_AdjacentDaysNotClosed(t *testing.T) {
	t.Parallel()
	// Verify the days immediately before and after ad-hoc multi-day closures
	// are NOT flagged as holidays (assuming they are weekdays).
	notClosed := []struct {
		name string
		d    time.Time
	}{
		{"Day before 9/11 closure (Mon Sep 10 2001)", date(2001, time.September, 10)},
		{"Day after 9/11 closure (Mon Sep 17 2001)", date(2001, time.September, 17)},
		{"Day after Reagan mourning (Fri Jun 12 2004)", date(2004, time.June, 12)},
		{"Day after Sandy (Mon Oct 31 2012)", date(2012, time.October, 31)},
	}

	for _, tc := range notClosed {
		t.Run(tc.name, func(t *testing.T) {
			if services.IsUSMarketHoliday(tc.d) {
				t.Errorf("expected %s (%s) NOT to be a market closure", tc.name, tc.d.Format("2006-01-02"))
			}
		})
	}
}

package tests

import (
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/services"
)

// TestPreviousMarketDay verifies that PreviousMarketDay always returns the most recent
// trading-day close strictly before today, regardless of current time-of-day.
// This prevents /glance from requesting EODHD data that hasn't been bulk-published yet
// (EODHD publishes D+1 data at ~4am ET the following day).
func TestPreviousMarketDay(t *testing.T) {
	nyLoc, _ := time.LoadLocation("America/New_York")

	cases := []struct {
		name string
		now  time.Time
		want string // calendar date "2006-01-02"
	}{
		{
			// Monday morning — yesterday was Sunday, so rolls back to Friday.
			name: "Monday 9am ET → Friday",
			now:  time.Date(2026, time.April, 13, 9, 0, 0, 0, nyLoc), // Monday
			want: "2026-04-10",                                         // Friday
		},
		{
			// Tuesday morning — yesterday was Monday (trading day).
			name: "Tuesday 9am ET → Monday",
			now:  time.Date(2026, time.April, 14, 9, 0, 0, 0, nyLoc),
			want: "2026-04-13",
		},
		{
			// Wednesday morning — yesterday was Tuesday (trading day).
			name: "Wednesday 9am ET → Tuesday",
			now:  time.Date(2026, time.April, 15, 9, 0, 0, 0, nyLoc),
			want: "2026-04-14",
		},
		{
			// Tuesday after market close (5pm ET): LastMarketClose(now) would return Tuesday,
			// but PreviousMarketDay must return Monday — never today.
			name: "Tuesday 5pm ET (after close) → Monday, not Tuesday",
			now:  time.Date(2026, time.April, 14, 17, 0, 0, 0, nyLoc),
			want: "2026-04-13",
		},
		{
			// Saturday — yesterday was Friday (trading day).
			name: "Saturday → Friday",
			now:  time.Date(2026, time.April, 11, 12, 0, 0, 0, nyLoc),
			want: "2026-04-10",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := services.PreviousMarketDay(tc.now)
			if got.Format("2006-01-02") != tc.want {
				t.Errorf("PreviousMarketDay(%s) = %s (%s), want %s",
					tc.now.Format("2006-01-02 15:04:05 MST"),
					got.Format("2006-01-02 15:04:05 MST"), got.Weekday(),
					tc.want)
			}
		})
	}
}

// TestNextTradingDayFromMidnightUTC is a regression test for the Saturday-fetch bug.
//
// GetLastBulkFetchDate returns a Postgres DATE column scanned by pgx, which arrives
// as midnight UTC (e.g. 2026-03-27 00:00:00 UTC for the date 2026-03-27).
//
// Bug: NextTradingDay converted midnight UTC to NY time (EDT = UTC-4), which shifted the
// clock back to 8 PM on the previous calendar day. So 2026-03-28 00:00 UTC became
// 2026-03-27 20:00 EDT — still "Friday" — and passed the weekend check, causing the
// function to return 2026-03-28 (Saturday) instead of 2026-03-30 (Monday).
//
// Fix: anchor the input's Year/Month/Day to midnight NY before iterating, so the
// calendar date is preserved regardless of whether the input is midnight UTC or midnight NY.
// The function now returns midnight NY, so comparisons use Format("2006-01-02").
func TestNextTradingDayFromMidnightUTC(t *testing.T) {
	cases := []struct {
		name string
		from time.Time
		want string // calendar date "2006-01-02"
	}{
		{
			// The exact failure from the production log: server offline since
			// 2026-03-27 (Friday), restarted 2026-04-02; bulk fetch triggered
			// for 2026-03-28 (Saturday) instead of 2026-03-30 (Monday).
			name: "Friday midnight UTC → Monday (EDT offset shifts check to previous day)",
			from: time.Date(2026, time.March, 27, 0, 0, 0, 0, time.UTC),
			want: "2026-03-30",
		},
		{
			// Any Friday midnight UTC should advance to Monday, not Saturday.
			name: "Friday midnight UTC (2025-01-03) → Monday 2025-01-06",
			from: time.Date(2025, time.January, 3, 0, 0, 0, 0, time.UTC),
			want: "2025-01-06",
		},
		{
			// Monday midnight UTC → Tuesday (baseline sanity check).
			name: "Monday midnight UTC → Tuesday",
			from: time.Date(2025, time.January, 6, 0, 0, 0, 0, time.UTC),
			want: "2025-01-07",
		},
		{
			// Thursday midnight UTC → Friday (no weekend skip needed).
			name: "Thursday midnight UTC → Friday",
			from: time.Date(2025, time.January, 16, 0, 0, 0, 0, time.UTC),
			want: "2025-01-17",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := services.NextTradingDay(tc.from)
			if got.Format("2006-01-02") != tc.want {
				t.Errorf("NextTradingDay(%s) = %s (%s), want %s",
					tc.from.Format("2006-01-02 15:04:05 MST"),
					got.Format("2006-01-02 15:04:05 MST"), got.Weekday(),
					tc.want)
			}
		})
	}
}

package tests

import (
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/services"
)

// TestNextTreasuryUpdateDate covers the FRED DGS10 publication schedule:
// Friday data is not published until the following Monday at 4:30 PM ET.
// Monday–Thursday: if before 4:30 PM ET, returns today at 4:30 PM ET;
//                  otherwise rolls forward to the next business day.
func TestNextTreasuryUpdateDate(t *testing.T) {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("Failed to load NY timezone: %v", err)
	}

	tests := []struct {
		name        string
		input       time.Time
		wantWeekday time.Weekday
		wantHour    int
		wantMinute  int
	}{
		{
			// Monday 9 AM ET — before cutoff → same Monday 4:30 PM ET
			name:        "Monday 9 AM ET — returns same Monday 4:30 PM ET",
			input:       time.Date(2025, 1, 6, 9, 0, 0, 0, nyLoc), // Monday
			wantWeekday: time.Monday,
			wantHour:    16,
			wantMinute:  30,
		},
		{
			// Monday 4:29 PM ET — still before cutoff → same Monday 4:30 PM ET
			name:        "Monday 4:29 PM ET — returns same Monday 4:30 PM ET",
			input:       time.Date(2025, 1, 6, 16, 29, 0, 0, nyLoc),
			wantWeekday: time.Monday,
			wantHour:    16,
			wantMinute:  30,
		},
		{
			// Monday 5 PM ET — after cutoff → Tuesday 4:30 PM ET
			name:        "Monday 5 PM ET — rolls to Tuesday 4:30 PM ET",
			input:       time.Date(2025, 1, 6, 17, 0, 0, 0, nyLoc),
			wantWeekday: time.Tuesday,
			wantHour:    16,
			wantMinute:  30,
		},
		{
			// Wednesday noon ET — before cutoff → same Wednesday 4:30 PM ET
			name:        "Wednesday noon ET — returns same Wednesday 4:30 PM ET",
			input:       time.Date(2025, 1, 8, 12, 0, 0, 0, nyLoc),
			wantWeekday: time.Wednesday,
			wantHour:    16,
			wantMinute:  30,
		},
		{
			// Thursday 4:31 PM ET — after cutoff → rolls to next business day = Friday.
			// Note: the function only skips Fridays as an *input* day (any time on Friday rolls
			// to Monday). A roll that *lands* on Friday is not skipped further. This means
			// Thursday-after-cutoff → Friday 4:30 PM, even though FRED doesn't publish DGS10
			// data on Friday (it publishes Friday data on the following Monday). This is an
			// acknowledged limitation; callers that need Monday behaviour can check the result day.
			name:        "Thursday 4:31 PM ET — rolls to Friday 4:30 PM ET (next business day)",
			input:       time.Date(2025, 1, 9, 16, 31, 0, 0, nyLoc), // Thursday
			wantWeekday: time.Friday,
			wantHour:    16,
			wantMinute:  30,
		},
		{
			// Friday 9 AM ET — Fridays are always treated as after-cutoff → rolls to Monday
			name:        "Friday 9 AM ET — always rolls to Monday 4:30 PM ET",
			input:       time.Date(2025, 1, 10, 9, 0, 0, 0, nyLoc), // Friday
			wantWeekday: time.Monday,
			wantHour:    16,
			wantMinute:  30,
		},
		{
			// Saturday — rolls to Monday
			name:        "Saturday — rolls to Monday 4:30 PM ET",
			input:       time.Date(2025, 1, 11, 12, 0, 0, 0, nyLoc), // Saturday
			wantWeekday: time.Monday,
			wantHour:    16,
			wantMinute:  30,
		},
		{
			// Sunday — rolls to Monday
			name:        "Sunday — rolls to Monday 4:30 PM ET",
			input:       time.Date(2025, 1, 12, 12, 0, 0, 0, nyLoc), // Sunday
			wantWeekday: time.Monday,
			wantHour:    16,
			wantMinute:  30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := services.NextTreasuryUpdateDate(tt.input)
			resultNY := result.In(nyLoc)

			if resultNY.Weekday() != tt.wantWeekday {
				t.Errorf("got weekday %v, want %v (result: %s)",
					resultNY.Weekday(), tt.wantWeekday,
					resultNY.Format("2006-01-02 15:04 MST"))
			}
			if resultNY.Hour() != tt.wantHour || resultNY.Minute() != tt.wantMinute {
				t.Errorf("got time %02d:%02d ET, want %02d:%02d ET (result: %s)",
					resultNY.Hour(), resultNY.Minute(),
					tt.wantHour, tt.wantMinute,
					resultNY.Format("2006-01-02 15:04 MST"))
			}
		})
	}
}

func TestNextMarketDate(t *testing.T) {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("Failed to load NY timezone: %v", err)
	}

	tests := []struct {
		name     string
		input    time.Time
		expected time.Time
	}{
		{
			name:     "Monday 10:00 AM NY - same day",
			input:    time.Date(2025, 1, 6, 10, 0, 0, 0, nyLoc),
			expected: time.Date(2025, 1, 6, 16, 30, 0, 0, nyLoc),
		},
		{
			name:     "Monday 4:29 PM NY - same day",
			input:    time.Date(2025, 1, 6, 16, 29, 0, 0, nyLoc),
			expected: time.Date(2025, 1, 6, 16, 30, 0, 0, nyLoc),
		},
		{
			name:     "Monday 4:30 PM NY - next day",
			input:    time.Date(2025, 1, 6, 16, 30, 0, 0, nyLoc),
			expected: time.Date(2025, 1, 7, 16, 30, 0, 0, nyLoc),
		},
		{
			name:     "Monday 5:00 PM NY - next day",
			input:    time.Date(2025, 1, 6, 17, 0, 0, 0, nyLoc),
			expected: time.Date(2025, 1, 7, 16, 30, 0, 0, nyLoc),
		},
		{
			name:     "Friday 5:00 PM NY - rolls to Monday",
			input:    time.Date(2025, 1, 3, 17, 0, 0, 0, nyLoc),
			expected: time.Date(2025, 1, 6, 16, 30, 0, 0, nyLoc),
		},
		{
			name:     "Saturday noon NY - rolls to Monday",
			input:    time.Date(2025, 1, 4, 12, 0, 0, 0, nyLoc),
			expected: time.Date(2025, 1, 6, 16, 30, 0, 0, nyLoc),
		},
		{
			name:     "Sunday noon NY - rolls to Monday",
			input:    time.Date(2025, 1, 5, 12, 0, 0, 0, nyLoc),
			expected: time.Date(2025, 1, 6, 16, 30, 0, 0, nyLoc),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := services.NextMarketDate(tt.input)

			// Compare in NY timezone
			resultNY := result.In(nyLoc)
			expectedNY := tt.expected.In(nyLoc)

			if !resultNY.Equal(expectedNY) {
				t.Errorf("NextMarketDate(%v) = %v, want %v",
					tt.input.Format("2006-01-02 15:04 MST"),
					resultNY.Format("2006-01-02 15:04 MST"),
					expectedNY.Format("2006-01-02 15:04 MST"))
			}
		})
	}
}

func TestNextMarketDateTimezoneConversion(t *testing.T) {
	nyLoc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("Failed to load NY timezone: %v", err)
	}

	denverLoc, err := time.LoadLocation("America/Denver")
	if err != nil {
		t.Fatalf("Failed to load Denver timezone: %v", err)
	}

	londonLoc, err := time.LoadLocation("Europe/London")
	if err != nil {
		t.Fatalf("Failed to load London timezone: %v", err)
	}

	tests := []struct {
		name     string
		input    time.Time
		expected time.Time
	}{
		{
			// 8 AM MST = 10 AM EST (before 4:30 PM cutoff)
			name:     "Denver/Mountain to NY - before cutoff",
			input:    time.Date(2025, 1, 6, 8, 0, 0, 0, denverLoc),
			expected: time.Date(2025, 1, 6, 16, 30, 0, 0, nyLoc),
		},
		{
			// 3 PM MST = 5 PM EST (after 4:30 PM cutoff)
			name:     "Denver/Mountain to NY - after cutoff",
			input:    time.Date(2025, 1, 6, 15, 0, 0, 0, denverLoc),
			expected: time.Date(2025, 1, 7, 16, 30, 0, 0, nyLoc),
		},
		{
			// DST mismatch: US on DST (EDT), UK not yet on DST (GMT)
			// March 11, 2025: US switched to EDT on March 9, UK switches March 30
			// 7 PM GMT = 3 PM EDT (before 4:30 PM cutoff)
			name:     "DST mismatch - US on DST, UK not yet",
			input:    time.Date(2025, 3, 11, 19, 0, 0, 0, londonLoc),
			expected: time.Date(2025, 3, 11, 16, 30, 0, 0, nyLoc),
		},
		{
			// DST mismatch: US on DST (EDT), UK not yet on DST (GMT)
			// March 11, 2025: 10 PM GMT = 6 PM EDT (after 4:30 PM cutoff)
			name:     "DST mismatch - US on DST, UK not yet - after cutoff",
			input:    time.Date(2025, 3, 11, 22, 0, 0, 0, londonLoc),
			expected: time.Date(2025, 3, 12, 16, 30, 0, 0, nyLoc),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := services.NextMarketDate(tt.input)

			// Compare in NY timezone
			resultNY := result.In(nyLoc)
			expectedNY := tt.expected.In(nyLoc)

			if !resultNY.Equal(expectedNY) {
				t.Errorf("NextMarketDate(%v) = %v, want %v",
					tt.input.Format("2006-01-02 15:04 MST"),
					resultNY.Format("2006-01-02 15:04 MST"),
					expectedNY.Format("2006-01-02 15:04 MST"))
			}
		})
	}
}

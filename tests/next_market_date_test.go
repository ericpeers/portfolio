package tests

import (
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/services"
)

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

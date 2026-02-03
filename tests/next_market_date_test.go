package tests

import (
	"testing"
	"time"

	"github.com/epeers/portfolio/internal/util"
	"github.com/stretchr/testify/assert"
)

func TestNextMarketDate(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("should have loaded timezone America/New_York: %v", err)
	}

	testCases := []struct {
		name     string
		input    time.Time
		expected time.Time
	}{
		{
			name:     "Weekday before 4:30 PM",
			input:    time.Date(2024, 7, 23, 10, 0, 0, 0, ny), // Tuesday 10:00 AM
			expected: time.Date(2024, 7, 23, 16, 30, 0, 0, ny), // Tuesday 4:30 PM
		},
		{
			name:     "Weekday after 4:30 PM",
			input:    time.Date(2024, 7, 23, 17, 0, 0, 0, ny), // Tuesday 5:00 PM
			expected: time.Date(2024, 7, 24, 16, 30, 0, 0, ny), // Wednesday 4:30 PM
		},
		{
			name:     "Friday before 4:30 PM",
			input:    time.Date(2024, 7, 26, 12, 0, 0, 0, ny), // Friday 12:00 PM
			expected: time.Date(2024, 7, 26, 16, 30, 0, 0, ny), // Friday 4:30 PM
		},
		{
			name:     "Friday after 4:30 PM",
			input:    time.Date(2024, 7, 26, 18, 0, 0, 0, ny), // Friday 6:00 PM
			expected: time.Date(2024, 7, 29, 16, 30, 0, 0, ny), // Monday 4:30 PM
		},
		{
			name:     "Saturday",
			input:    time.Date(2024, 7, 27, 12, 0, 0, 0, ny), // Saturday 12:00 PM
			expected: time.Date(2024, 7, 29, 16, 30, 0, 0, ny), // Monday 4:30 PM
		},
		{
			name:     "Sunday",
			input:    time.Date(2024, 7, 28, 12, 0, 0, 0, ny), // Sunday 12:00 PM
			expected: time.Date(2024, 7, 29, 16, 30, 0, 0, ny), // Monday 4:30 PM
		},
		{
			name:     "Weekday at exactly 4:30 PM",
			input:    time.Date(2024, 7, 23, 16, 30, 0, 0, ny), // Tuesday 4:30 PM
			expected: time.Date(2024, 7, 23, 16, 30, 0, 0, ny), // Tuesday 4:30 PM
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := util.NextMarketDate(tc.input)
			assert.Equal(t, tc.expected.UTC(), actual, "The expected date should be %v but was %v", tc.expected.UTC(), actual)
		})
	}
}

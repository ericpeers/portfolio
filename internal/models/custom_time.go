package models

import (
	"encoding/json"
	"strings"
	"time"
)

// FlexibleDate is a custom time type that can unmarshal both RFC3339 and "YYYY-MM-DD" formats
type FlexibleDate struct {
	time.Time
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (f *FlexibleDate) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)

	// Try parsing as RFC3339 full timestamp first
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		f.Time = t
		return nil
	}

	// If that fails, try parsing as a date-only string
	t, err = time.Parse("2006-01-02", s)
	if err != nil {
		return err
	}
	f.Time = t
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (f FlexibleDate) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.Time)
}

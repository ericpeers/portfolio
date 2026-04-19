package apperrors

import (
	"sync/atomic"
	"time"
)

var (
	errorCount int64
	startTime  = time.Now()
)

// Record increments the runtime error count.
func Record() {
	atomic.AddInt64(&errorCount, 1)
}

// Count returns the number of errors recorded since startup.
func Count() int64 {
	return atomic.LoadInt64(&errorCount)
}

// UptimeSeconds returns seconds elapsed since process start.
func UptimeSeconds() int64 {
	return int64(time.Since(startTime).Seconds())
}

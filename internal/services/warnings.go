package services

import (
	"context"
	"sync"

	"github.com/epeers/portfolio/internal/models"
)

type warningContextKey struct{}

// WarningCollector accumulates warnings during a service call chain.
type WarningCollector struct {
	mu       sync.Mutex
	warnings []models.Warning
}

// NewWarningContext returns a context carrying a fresh WarningCollector,
// plus a reference to the collector so the handler can retrieve warnings later.
func NewWarningContext(ctx context.Context) (context.Context, *WarningCollector) {
	wc := &WarningCollector{}
	return context.WithValue(ctx, warningContextKey{}, wc), wc
}

// AddWarning appends a warning to the collector in ctx.
// If ctx has no collector, the call is a no-op.
func AddWarning(ctx context.Context, w models.Warning) {
	wc, ok := ctx.Value(warningContextKey{}).(*WarningCollector)
	if !ok || wc == nil {
		return
	}
	wc.mu.Lock()
	defer wc.mu.Unlock()
	wc.warnings = append(wc.warnings, w)
}

// GetWarnings returns all collected warnings.
func (wc *WarningCollector) GetWarnings() []models.Warning {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	return wc.warnings
}

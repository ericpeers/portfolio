package tests

import (
	"context"
	"sync"
	"testing"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
)

func TestWarningCollector_BasicUsage(t *testing.T) {
	ctx, wc := services.NewWarningContext(context.Background())

	services.AddWarning(ctx, models.Warning{
		Code:    models.WarnUnresolvedETFHolding,
		Message: "test warning 1",
	})
	services.AddWarning(ctx, models.Warning{
		Code:    models.WarnPartialETFExpansion,
		Message: "test warning 2",
	})

	warnings := wc.GetWarnings()
	if len(warnings) != 2 {
		t.Fatalf("expected 2 warnings, got %d", len(warnings))
	}

	if warnings[0].Code != models.WarnUnresolvedETFHolding {
		t.Errorf("expected code %s, got %s", models.WarnUnresolvedETFHolding, warnings[0].Code)
	}
	if warnings[1].Code != models.WarnPartialETFExpansion {
		t.Errorf("expected code %s, got %s", models.WarnPartialETFExpansion, warnings[1].Code)
	}
}

func TestWarningCollector_NoCollectorNoPanic(t *testing.T) {
	// AddWarning with a plain context should not panic
	ctx := context.Background()
	services.AddWarning(ctx, models.Warning{
		Code:    models.WarnUnresolvedETFHolding,
		Message: "this should be silently dropped",
	})
	// No assertion needed — just verifying no panic
}

func TestWarningCollector_EmptyByDefault(t *testing.T) {
	_, wc := services.NewWarningContext(context.Background())
	warnings := wc.GetWarnings()
	if len(warnings) != 0 {
		t.Errorf("expected 0 warnings, got %d", len(warnings))
	}
}

func TestWarningCollector_ConcurrentSafe(t *testing.T) {
	ctx, wc := services.NewWarningContext(context.Background())

	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			services.AddWarning(ctx, models.Warning{
				Code:    models.WarnUnresolvedETFHolding,
				Message: "concurrent warning",
			})
		}()
	}
	wg.Wait()

	warnings := wc.GetWarnings()
	if len(warnings) != n {
		t.Errorf("expected %d warnings, got %d", n, len(warnings))
	}
}

func TestWarningCollector_ContextPropagation(t *testing.T) {
	// Warnings added in a child context value-propagation chain should still collect
	ctx, wc := services.NewWarningContext(context.Background())

	// Simulate passing ctx through function layers
	innerFunc := func(ctx context.Context) {
		services.AddWarning(ctx, models.Warning{
			Code:    models.WarnUnresolvedETFHolding,
			Message: "from inner function",
		})
	}

	middleFunc := func(ctx context.Context) {
		innerFunc(ctx)
		services.AddWarning(ctx, models.Warning{
			Code:    models.WarnPartialETFExpansion,
			Message: "from middle function",
		})
	}

	middleFunc(ctx)

	warnings := wc.GetWarnings()
	if len(warnings) != 2 {
		t.Fatalf("expected 2 warnings from propagation, got %d", len(warnings))
	}
}

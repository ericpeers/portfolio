package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/epeers/portfolio/internal/middleware"
)

// TestWarmingMiddleware_ChannelAlreadyClosed verifies that when warmingDone is already
// closed the request passes through immediately and the next handler is called.
func TestWarmingMiddleware_ChannelAlreadyClosed(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	done := make(chan struct{})
	close(done)

	nextCalled := false
	router := gin.New()
	router.GET("/test", middleware.WarmingMiddleware(done), func(c *gin.Context) {
		nextCalled = true
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if !nextCalled {
		t.Error("next handler was not called when warmingDone was already closed")
	}
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// TestWarmingMiddleware_ContextCancelled verifies that when the request context is
// cancelled before warmingDone closes, the middleware aborts with 410 Gone and the
// next handler is not called.
func TestWarmingMiddleware_ContextCancelled(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	// Channel that will never be closed within this test.
	done := make(chan struct{})

	nextCalled := false
	router := gin.New()
	router.GET("/test", middleware.WarmingMiddleware(done), func(c *gin.Context) {
		nextCalled = true
		c.Status(http.StatusOK)
	})

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the request is served

	req := httptest.NewRequest(http.MethodGet, "/test", nil).WithContext(cancelCtx)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if nextCalled {
		t.Error("next handler should not be called when request context is cancelled")
	}
	if w.Code != http.StatusGone {
		t.Errorf("expected 410 Gone, got %d", w.Code)
	}
}

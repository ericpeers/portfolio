package tests

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/epeers/portfolio/internal/apperrors"
	"github.com/epeers/portfolio/internal/middleware"
)

// Not parallel — tests measure deltas against a shared monotonic counter and must
// not interleave with other tests that trigger apperrors.Record().

func TestErrorCounter_RecordAndCount(t *testing.T) {
	before := apperrors.Count()

	apperrors.Record()
	apperrors.Record()
	apperrors.Record()

	if got := apperrors.Count() - before; got != 3 {
		t.Errorf("expected delta 3, got %d", got)
	}
}

func TestErrorCounter_UptimeNonNegative(t *testing.T) {
	if got := apperrors.UptimeSeconds(); got < 0 {
		t.Errorf("expected non-negative uptime, got %d", got)
	}
}

func TestErrorCounter_ConcurrentRecord(t *testing.T) {
	before := apperrors.Count()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			apperrors.Record()
		}()
	}
	wg.Wait()

	if got := apperrors.Count() - before; got != goroutines {
		t.Errorf("expected delta %d after concurrent records, got %d", goroutines, got)
	}
}

func TestErrorCounterMiddleware_5xxIncrementsCounter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	before := apperrors.Count()

	router := gin.New()
	router.Use(middleware.ErrorCounter())
	router.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusInternalServerError)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	router.ServeHTTP(httptest.NewRecorder(), req)

	if got := apperrors.Count() - before; got != 1 {
		t.Errorf("expected delta 1 after 500 response, got %d", got)
	}
}

func TestErrorCounterMiddleware_2xxDoesNotIncrement(t *testing.T) {
	gin.SetMode(gin.TestMode)
	before := apperrors.Count()

	router := gin.New()
	router.Use(middleware.ErrorCounter())
	router.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	router.ServeHTTP(httptest.NewRecorder(), req)

	if got := apperrors.Count() - before; got != 0 {
		t.Errorf("expected delta 0 after 200 response, got %d", got)
	}
}

func TestErrorCounterMiddleware_4xxDoesNotIncrement(t *testing.T) {
	gin.SetMode(gin.TestMode)
	before := apperrors.Count()

	router := gin.New()
	router.Use(middleware.ErrorCounter())
	router.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusNotFound)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	router.ServeHTTP(httptest.NewRecorder(), req)

	if got := apperrors.Count() - before; got != 0 {
		t.Errorf("expected delta 0 after 404 response, got %d", got)
	}
}

func TestErrorCounterMiddleware_MultipleRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	before := apperrors.Count()

	router := gin.New()
	router.Use(middleware.ErrorCounter())
	router.GET("/ok", func(c *gin.Context) { c.Status(http.StatusOK) })
	router.GET("/err", func(c *gin.Context) { c.Status(http.StatusInternalServerError) })
	router.GET("/bad", func(c *gin.Context) { c.Status(http.StatusBadGateway) })

	for _, path := range []string{"/ok", "/err", "/bad", "/err", "/ok"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		router.ServeHTTP(httptest.NewRecorder(), req)
	}

	// /err × 2 (500) + /bad × 1 (502) = 3
	if got := apperrors.Count() - before; got != 3 {
		t.Errorf("expected delta 3 (two 500s + one 502), got %d", got)
	}
}

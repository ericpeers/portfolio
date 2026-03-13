package middleware

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// WarmingMiddleware blocks incoming requests until the warmingDone channel is closed
// (signalling that the startup price cache catch-up has finished), or until a 30-second
// timeout elapses. After the timeout the request proceeds normally — the on-demand
// singleton fetch path serves as a fallback if the bulk fetch is still running.
//
// Apply only to routes that trigger price computation (compare, glance). Health checks,
// admin routes, and CRUD routes should not be gated.
func WarmingMiddleware(warmingDone <-chan struct{}) gin.HandlerFunc {
	return func(c *gin.Context) {
		select {
		case <-warmingDone:
			// Cache is warm — proceed immediately.
		case <-time.After(30 * time.Second):
			// Timeout — proceed anyway; singleton fallback will handle missing data.
		case <-c.Request.Context().Done():
			c.AbortWithStatus(http.StatusGone) // 410 or use 499 convention
			return
		}
		c.Next()
	}
}

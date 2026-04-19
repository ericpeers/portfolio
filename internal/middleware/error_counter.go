package middleware

import (
	"github.com/epeers/portfolio/internal/apperrors"
	"github.com/gin-gonic/gin"
)

// ErrorCounter increments the apperrors counter for any 5xx response.
func ErrorCounter() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if c.Writer.Status() >= 500 {
			apperrors.Record()
		}
	}
}

package middleware

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

const UserIDKey = "user_id"

// ValidateUser is a stubbed authentication middleware that extracts user ID from X-User-ID header
func ValidateUser() gin.HandlerFunc {
	return func(c *gin.Context) {
		userIDStr := c.GetHeader("X-User-ID")
		if userIDStr == "" {
			c.Next()
			return
		}

		userID, err := strconv.ParseInt(userIDStr, 10, 64)
		if err != nil {
			c.Next()
			return
		}

		c.Set(UserIDKey, userID)
		c.Next()
	}
}

// GetUserID retrieves the user ID from the context
func GetUserID(c *gin.Context) (int64, bool) {
	userID, exists := c.Get(UserIDKey)
	if !exists {
		return 0, false
	}
	return userID.(int64), true
}

// RequireAuth ensures a user is authenticated
func RequireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		if _, exists := GetUserID(c); !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			c.Abort()
			return
		}
		c.Next()
	}
}

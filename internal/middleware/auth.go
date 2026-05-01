package middleware

import (
	"net/http"
	"strings"

	"github.com/epeers/portfolio/internal/models"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

const UserIDKey = "user_id"
const RoleKey = "role"

// ValidateUser parses "Authorization: Bearer <token>" and populates UserIDKey and RoleKey
// in the Gin context. Permissive: calls c.Next() if the header is missing or token is invalid.
func ValidateUser(jwtSecret []byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.Next()
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		token, err := jwt.ParseWithClaims(tokenStr, &models.JWTClaims{}, func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return jwtSecret, nil
		})
		if err != nil || !token.Valid {
			c.Next()
			return
		}

		claims, ok := token.Claims.(*models.JWTClaims)
		if !ok {
			c.Next()
			return
		}

		c.Set(UserIDKey, claims.UserID)
		c.Set(RoleKey, claims.Role)
		c.Next()
	}
}

// RequireAuth aborts with 401 if no authenticated user is in the context.
func RequireAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		if _, exists := c.Get(UserIDKey); !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, models.ErrorResponse{
				Error:   "unauthorized",
				Message: "authentication required",
			})
			return
		}
		c.Next()
	}
}

// RequireAdmin aborts with 403 if the authenticated user does not have the ADMIN role.
func RequireAdmin() gin.HandlerFunc {
	return func(c *gin.Context) {
		role, _ := c.Get(RoleKey)
		if role != "ADMIN" {
			c.AbortWithStatusJSON(http.StatusForbidden, models.ErrorResponse{
				Error:   "forbidden",
				Message: "admin access required",
			})
			return
		}
		c.Next()
	}
}

// GetUserID retrieves the authenticated user's ID from the Gin context.
func GetUserID(c *gin.Context) (int64, bool) {
	v, exists := c.Get(UserIDKey)
	if !exists {
		return 0, false
	}
	id, ok := v.(int64)
	return id, ok
}

// GetRole retrieves the authenticated user's role from the Gin context.
func GetRole(c *gin.Context) (string, bool) {
	v, exists := c.Get(RoleKey)
	if !exists {
		return "", false
	}
	role, ok := v.(string)
	return role, ok
}

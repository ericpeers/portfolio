package handlers

import (
	"net/http"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/services"
	"github.com/gin-gonic/gin"
)

// AdminHandler handles admin endpoints
type AdminHandler struct {
	adminSvc *services.AdminService
}

// NewAdminHandler creates a new AdminHandler
func NewAdminHandler(adminSvc *services.AdminService) *AdminHandler {
	return &AdminHandler{
		adminSvc: adminSvc,
	}
}

// SyncSecurities handles POST /admin/sync-securities
func (h *AdminHandler) SyncSecurities(c *gin.Context) {
	result, err := h.adminSvc.SyncSecurities(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:   "internal_error",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

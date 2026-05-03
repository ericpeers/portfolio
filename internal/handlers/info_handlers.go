package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// InfoHandler serves static informational endpoints.
type InfoHandler struct {
	licensesText []byte
}

// NewInfoHandler creates an InfoHandler. licensesText is the content of
// LICENSES.TXT, embedded at build time by the root package.
func NewInfoHandler(licensesText []byte) *InfoHandler {
	return &InfoHandler{licensesText: licensesText}
}

// GetLicenses godoc
// @Summary      Third-party software licenses
// @Description  Returns the complete license text for all open source dependencies used by this service.
// @Tags         info
// @Produce      plain
// @Success      200  {string}  string  "License notices"
// @Router       /licenses [get]
func (h *InfoHandler) GetLicenses(c *gin.Context) {
	c.Data(http.StatusOK, "text/plain; charset=utf-8", h.licensesText)
}

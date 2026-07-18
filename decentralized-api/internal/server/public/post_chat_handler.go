package public

import (
	"net/http"
	"time"

	"decentralized-api/apiconfig"

	"github.com/labstack/echo/v4"
)

const (
	// MaxRequestBodySize is the maximum allowed size for request bodies (10 MiB)
	MaxRequestBodySize = 10 * 1024 * 1024
	// MaxRequestBodyLimit is the Echo body-limit middleware value that matches MaxRequestBodySize exactly.
	MaxRequestBodyLimit = "10485760"

	chatCompletionsPath = "/v1/chat/completions"

	executorCompletionsUnsupportedMsg = "selected executor does not support /v1/completions; upgrade required"
)

// configManagerRef references the config manager for accessing validation parameters.
var configManagerRef *apiconfig.ConfigManager

// NewNoRedirectClient returns an HTTP client that surfaces redirects to the
// caller instead of following them (SSRF hardening for outbound fetches).
func NewNoRedirectClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (s *Server) getAllowedPubKeys(ctx echo.Context, granterAddress string) ([]string, error) {
	return s.authzCache.GetPubKeys(ctx.Request().Context(), granterAddress, "/inference.inference.MsgStartInference")
}

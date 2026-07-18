package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestAdminBearerAuth(t *testing.T) {
	e := echo.New()
	handler := adminBearerAuth("s3cret")(func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})

	cases := []struct {
		name       string
		authHeader string
		wantOK     bool
	}{
		{"valid token", "Bearer s3cret", true},
		{"wrong token", "Bearer nope", false},
		{"missing header", "", false},
		{"no bearer prefix", "s3cret", false},
		{"empty bearer", "Bearer ", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/v1/nodes", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			c := e.NewContext(req, httptest.NewRecorder())
			err := handler(c)
			if tc.wantOK {
				require.NoError(t, err)
				return
			}
			he, ok := err.(*echo.HTTPError)
			require.True(t, ok, "expected an echo.HTTPError")
			require.Equal(t, http.StatusUnauthorized, he.Code)
		})
	}
}

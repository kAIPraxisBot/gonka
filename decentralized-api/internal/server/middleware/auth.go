package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/labstack/echo/v4"
)

// BearerAuth returns middleware requiring "Authorization: Bearer <token>" on
// every request, compared in constant time. Used to gate internal admin/ML APIs
// that would otherwise be protected only by network topology.
func BearerAuth(token string) echo.MiddlewareFunc {
	want := []byte("Bearer " + token)
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			got := []byte(c.Request().Header.Get("Authorization"))
			if subtle.ConstantTimeCompare(got, want) != 1 {
				return echo.NewHTTPError(http.StatusUnauthorized, "authorization required")
			}
			return next(c)
		}
	}
}

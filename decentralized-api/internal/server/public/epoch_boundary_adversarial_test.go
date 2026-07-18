package public

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
)

// Adversarial inputs modelling the H1 DoS claim: malformed epoch/participant
// path and query values sent to public, unauthenticated endpoints. Go's test
// runner fails the test on any panic, so a clean pass is empirical proof that
// the parse layer rejects garbage with an error instead of crashing. The full
// set (malformed + boundary-valid) is used for the no-panic sweep.
var adversarialParams = append([]string{
	"18446744073709551615", // math.MaxUint64 (valid uint64)
	"9223372036854775808",  // math.MaxInt64 + 1 (valid uint64)
	"01",                   // leading zero (parses to 1)
	"0",                    // zero
}, malformedParams...)

// malformedParams are inputs that MUST be rejected with an error by a uint64
// path/query parser (they are not valid uint64 values).
var malformedParams = []string{
	"999999999999999999999",                   // the exact H1 repro value (uint64 overflow)
	"18446744073709551616",                    // math.MaxUint64 + 1
	"-1",                                       // negative
	"-99999999999999999999999999999999999999", // huge negative
	"0x1F",                                     // hex
	"1e9",                                      // scientific notation
	"NaN",                                      // not a number
	"",                                         // empty
	" ",                                        // whitespace
	"1 ",                                       // trailing space
	" 1",                                       // leading space
	"1.5",                                      // float
	"abc",                                      // alpha
	"٤٢",                                       // arabic-indic digits (unicode)
	"４２",                                        // fullwidth digits
	"\x00\x01\x02",                             // control bytes
	"'; DROP TABLE participants;--",            // sql-ish injection
	"../../etc/passwd",                         // path traversal
	"%2e%2e%2f",                                // url-encoded traversal
	"+1",                                       // signed positive (ParseUint rejects sign)
}

func newParamContext(name, value string) echo.Context {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetParamNames(name)
	c.SetParamValues(value)
	return c
}

// TestResolveEpochAdversarial fires the H1 attack values at the exact function
// that parses the {epoch} path segment of /v1/epochs/:epoch(/participants).
// The numeric branch never touches the chain client, so a nil-recorder Server
// is sufficient; any input that isn't a valid uint64 must return an error.
func TestResolveEpochAdversarial(t *testing.T) {
	s := &Server{}
	// No input may panic (test-runner enforced across the full set).
	for _, in := range adversarialParams {
		c := newParamContext("epoch", in)
		_, _ = s.resolveEpochFromContext(c)
	}
	// Every genuinely-malformed value must be rejected with an error, not a panic.
	for _, in := range malformedParams {
		c := newParamContext("epoch", in)
		if _, err := s.resolveEpochFromContext(c); err == nil {
			t.Errorf("resolveEpochFromContext(%q) accepted malformed input (expected error)", in)
		}
	}
}

// TestParseStatsTimeRangeAdversarial exercises the stats time_from/time_to query
// parser with garbage on both parameters.
func TestParseStatsTimeRangeAdversarial(t *testing.T) {
	for _, from := range adversarialParams {
		for _, to := range adversarialParams {
			// Must not panic; error is expected for malformed input.
			_, _, _ = parseStatsTimeRange(from, to)
		}
	}
}

// TestParseEpochsNAdversarial exercises the epochs_n query parser. The only
// hard requirement is no panic (test-runner enforced) across the full set.
func TestParseEpochsNAdversarial(t *testing.T) {
	for _, in := range adversarialParams {
		_, _ = parseEpochsN(in)
	}
}

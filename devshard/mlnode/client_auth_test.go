package mlnode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBearerPerRPC(t *testing.T) {
	creds := bearerPerRPC{token: "s3cret"}

	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer s3cret", md["authorization"])

	// Must allow the insecure internal transport (token is the access control).
	require.False(t, creds.RequireTransportSecurity())
}

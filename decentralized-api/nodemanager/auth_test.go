package nodemanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthUnaryInterceptor(t *testing.T) {
	interceptor := AuthUnaryInterceptor("s3cret")
	handler := func(ctx context.Context, req interface{}) (interface{}, error) { return "ok", nil }

	authCtx := func(v string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", v))
	}

	cases := []struct {
		name    string
		ctx     context.Context
		wantErr bool
	}{
		{"valid", authCtx("Bearer s3cret"), false},
		{"wrong token", authCtx("Bearer nope"), true},
		{"no bearer prefix", authCtx("s3cret"), true},
		{"empty", authCtx(""), true},
		{"no metadata", context.Background(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := interceptor(tc.ctx, nil, nil, handler)
			if tc.wantErr {
				require.Error(t, err)
				require.Equal(t, codes.Unauthenticated, status.Code(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, "ok", resp)
		})
	}
}

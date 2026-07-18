package nodemanager

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthUnaryInterceptor requires an "authorization: Bearer <token>" gRPC metadata
// header on every unary call, compared in constant time. It gates the
// NodeManager gRPC — which hands out internal vLLM endpoints and can lock/exhaust
// nodes — that is otherwise protected only by network topology.
func AuthUnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	want := "Bearer " + token
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := checkBearerMetadata(ctx, want); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// AuthStreamInterceptor is the streaming counterpart of AuthUnaryInterceptor.
func AuthStreamInterceptor(token string) grpc.StreamServerInterceptor {
	want := "Bearer " + token
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkBearerMetadata(ss.Context(), want); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func checkBearerMetadata(ctx context.Context, want string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	vals := md.Get("authorization")
	if len(vals) != 1 || subtle.ConstantTimeCompare([]byte(vals[0]), []byte(want)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid authorization token")
	}
	return nil
}

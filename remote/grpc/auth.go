package remotegrpc

import (
	"context"
	"crypto/x509"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/xraph/fabriq/core/tenant"
)

// Authenticator verifies a call's credentials and returns a context enriched
// with the caller's identity. It MUST stamp a tenant (use WithTenant or
// tenant.WithTenant) — the binding rejects a call whose returned context carries
// none — and MAY add any app-specific principal / roles / claims under the app's
// own context keys, which fabriq's authz hooks (subscribe authz, live authz,
// document authz) then read server-side. fabriq core defines no principal type;
// identity beyond the tenant is entirely the app's vocabulary.
//
// The identity comes from the verified credential — the call metadata or mTLS
// peer reached via the helpers below — never from a field in the request body
// (ADR 0009 §Security). A non-nil error rejects the call (codes.Unauthenticated).
type Authenticator func(ctx context.Context) (context.Context, error)

// TenantResolver maps a call's credentials to just a tenant id — the common
// case with no principal. Adapt it to an Authenticator with TenantOnly.
type TenantResolver func(ctx context.Context) (tenantID string, err error)

// TenantOnly adapts a tenant-id resolver into an Authenticator that stamps only
// the tenant.
func TenantOnly(resolve TenantResolver) Authenticator {
	return func(ctx context.Context) (context.Context, error) {
		tid, err := resolve(ctx)
		if err != nil {
			return nil, err
		}
		return tenant.WithTenant(ctx, tid)
	}
}

// WithTenant stamps (and validates) the tenant id; Authenticators call it to
// build the enriched context without importing core/tenant directly.
func WithTenant(ctx context.Context, tenantID string) (context.Context, error) {
	return tenant.WithTenant(ctx, tenantID)
}

// ServerOptions wires auth into both unary and streaming calls. Combine with the
// caller's transport credentials for mTLS:
//
//	creds := credentials.NewTLS(&tls.Config{
//		Certificates: []tls.Certificate{serverCert},
//		ClientCAs:    caPool,
//		ClientAuth:   tls.RequireAndVerifyClientCert,
//	})
//	srv := grpc.NewServer(append(remotegrpc.ServerOptions(auth), grpc.Creds(creds))...)
//	remotegrpc.Register(srv, handler)
func ServerOptions(auth Authenticator) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryInterceptor(auth)),
		grpc.ChainStreamInterceptor(StreamInterceptor(auth)),
	}
}

// UnaryInterceptor authenticates a unary call and enriches its context before
// the handler runs.
func UnaryInterceptor(auth Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := authenticate(ctx, auth)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor authenticates a streaming call and enriches the stream's
// context.
func StreamInterceptor(auth Authenticator) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(ss.Context(), auth)
		if err != nil {
			return err
		}
		return handler(srv, &authStream{ServerStream: ss, ctx: ctx})
	}
}

func authenticate(ctx context.Context, auth Authenticator) (context.Context, error) {
	if auth == nil {
		return nil, status.Error(codes.Internal, "remotegrpc: nil Authenticator")
	}
	out, err := auth(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	if out == nil {
		return nil, status.Error(codes.Internal, "remotegrpc: authenticator returned a nil context")
	}
	// Enforce the one invariant the facade depends on: every call reaches a
	// store tenant-stamped. An authenticator that forgets is a bug, caught here
	// rather than as a confusing ErrNoTenant deep in the executor.
	if _, terr := tenant.Require(out); terr != nil {
		return nil, status.Error(codes.Unauthenticated, "remotegrpc: authenticator stamped no tenant")
	}
	return out, nil
}

// authStream carries the authenticated, enriched context down to the stream
// handler (grpc.ServerStream.Context is otherwise immutable).
type authStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authStream) Context() context.Context { return s.ctx }

// --- credential helpers for writing an Authenticator ---

// BearerToken returns the token from a single "authorization: Bearer <token>"
// metadata entry.
func BearerToken(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", false
	}
	const prefix = "Bearer "
	if len(vals[0]) <= len(prefix) || !strings.EqualFold(vals[0][:len(prefix)], prefix) {
		return "", false
	}
	return vals[0][len(prefix):], true
}

// ClientCertificate returns the verified leaf certificate of the mTLS peer, if
// the connection presented one. Use it to map a client cert (CN, SAN, …) to a
// tenant and principal inside an Authenticator.
func ClientCertificate(ctx context.Context) (*x509.Certificate, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, false
	}
	chains := tlsInfo.State.VerifiedChains
	if len(chains) == 0 || len(chains[0]) == 0 {
		return nil, false
	}
	return chains[0][0], true
}

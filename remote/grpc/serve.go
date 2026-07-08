package remotegrpc

import (
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/xraph/fabriq/remote"
)

// NewServer builds a *grpc.Server that serves the fabriq remote protocol backed
// by h, authenticating every call with auth (which MUST stamp a tenant). It is
// the server-role counterpart to Dial: forgeext.Extension.RemoteHandler() yields
// h from an embedded facade, this wraps it, and the caller runs srv.Serve(lis).
//
// Supply WithServerTLS for mTLS — the strong, backend-only transport ADR 0009
// assumes. Without it the server is plaintext (dev/localhost only). The caller
// owns the returned server: interceptors it adds via WithGRPCServerOptions chain
// after the auth interceptor, and it owns Stop/GracefulStop and the listener.
func NewServer(h *remote.Handler, auth Authenticator, opts ...ServerOption) *grpc.Server {
	cfg := serverConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	grpcOpts := ServerOptions(auth)
	if cfg.tlsConfig != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(cfg.tlsConfig)))
	}
	grpcOpts = append(grpcOpts, cfg.extra...)

	srv := grpc.NewServer(grpcOpts...)
	Register(srv, h)
	return srv
}

// ServerOption customizes NewServer.
type ServerOption func(*serverConfig)

type serverConfig struct {
	tlsConfig *tls.Config
	extra     []grpc.ServerOption
}

// WithServerTLS installs the server's transport credentials from cfg. Set
// ClientAuth: tls.RequireAndVerifyClientCert (plus ClientCAs) for mTLS.
func WithServerTLS(cfg *tls.Config) ServerOption {
	return func(c *serverConfig) { c.tlsConfig = cfg }
}

// WithGRPCServerOptions passes extra grpc.ServerOptions through (extra
// interceptors, keepalive/limits, etc.). They apply after the auth interceptor.
func WithGRPCServerOptions(opts ...grpc.ServerOption) ServerOption {
	return func(c *serverConfig) { c.extra = append(c.extra, opts...) }
}

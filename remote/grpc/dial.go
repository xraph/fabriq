package remotegrpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xraph/fabriq/client"
	"github.com/xraph/fabriq/remote"
)

// Dial parses a fabriq+grpc:// DSN, opens a gRPC channel to the host and returns
// a *remote.Fabric — a query.Fabric with the same call sites as the embedded
// engine (ADR 0009). It is the client-side entry point the fabriq+grpc:// scheme
// reserves in client.ParseDSN: client.Connect handles HTTP; grpc lives here so
// google.golang.org/grpc stays out of the core module.
//
// The DSN key is sent as an "authorization: Bearer <key>" credential on every
// call; the server's Authenticator maps it (or the mTLS peer cert) to the
// tenant — identity comes from the verified credential, never a request field.
// The channel is lazy (grpc.NewClient); the first RPC establishes it.
func Dial(ctx context.Context, dsn string, opts ...DialOption) (*remote.Fabric, error) {
	_ = ctx // reserved for a future connection-time handshake/ping

	d, err := client.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if d.Transport != "grpc" {
		return nil, fmt.Errorf("remotegrpc: dsn is not a grpc transport (want fabriq+grpc://): %s", dsn)
	}

	cfg := dialConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	var creds credentials.TransportCredentials
	if d.TLS {
		creds = credentials.NewTLS(cfg.tlsConfig)
	} else {
		creds = insecure.NewCredentials()
	}

	cc, err := grpc.NewClient(
		net.JoinHostPort(d.Host, d.Port),
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(bearerCreds{token: d.Key, requireSecurity: d.TLS}),
	)
	if err != nil {
		return nil, fmt.Errorf("remotegrpc: dial %s: %w", dsn, err)
	}
	return remote.New(NewClient(cc)), nil
}

// DialOption customizes a Dial. TLS material can't live in the DSN, so the mTLS
// client config is supplied here.
type DialOption func(*dialConfig)

type dialConfig struct {
	tlsConfig *tls.Config
}

// WithTLSConfig supplies the client TLS config used when the DSN selects TLS
// (tls=true, or any non-localhost host). Use it to present a client certificate
// for mTLS and to pin the CA / ServerName. Without it, a TLS DSN uses gRPC's
// default TLS (system roots, ServerName from the host).
func WithTLSConfig(c *tls.Config) DialOption {
	return func(cfg *dialConfig) { cfg.tlsConfig = c }
}

// bearerCreds sends the DSN key as an "authorization: Bearer <token>" header on
// every RPC. It permits insecure transport only when the DSN is non-TLS (a plain
// fabriq+grpc:// against localhost in dev); over TLS gRPC's own check applies.
type bearerCreds struct {
	token           string
	requireSecurity bool
}

func (b bearerCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b bearerCreds) RequireTransportSecurity() bool { return b.requireSecurity }

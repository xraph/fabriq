package remotegrpc_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/remote"
	remotegrpc "github.com/xraph/fabriq/remote/grpc"
)

// acmeBearerAuth maps the bearer token "s3cret" to tenant "acme" — the server
// edge's identity resolution for the round-trip tests.
func acmeBearerAuth() remotegrpc.Authenticator {
	return func(ctx context.Context) (context.Context, error) {
		tok, ok := remotegrpc.BearerToken(ctx)
		if !ok || tok != "s3cret" {
			return nil, errors.New("bad token")
		}
		return remotegrpc.WithTenant(ctx, "acme")
	}
}

// serveOnLoopback runs srv on a fresh 127.0.0.1 socket and returns its address,
// tearing the server down at test end.
func serveOnLoopback(t *testing.T, srv *grpc.Server) (addr string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// serveTenant stands up a real gRPC server on a loopback socket whose
// Authenticator maps the bearer token "s3cret" to tenant "acme", and returns the
// listener address plus the facade it delegates to. The caller Dials it by DSN.
func serveTenant(t *testing.T, opts ...grpc.ServerOption) (addr string, fab *fakeFabric) {
	t.Helper()
	fab = &fakeFabric{result: command.Result{AggID: "asset-1", Version: 1, EventID: "evt-1"}}
	srv := grpc.NewServer(append(remotegrpc.ServerOptions(acmeBearerAuth()), opts...)...)
	remotegrpc.Register(srv, remote.NewHandler(fab, assetRegistry(t)))
	return serveOnLoopback(t, srv), fab
}

// TestDial_BearerFromDSNKeyAuthenticatesTenant proves the whole client bridge:
// Dial parses a fabriq+grpc:// DSN, dials the host:port, sends the DSN key as a
// bearer credential, and returns a query.Fabric whose Exec crosses real gRPC —
// the server's Authenticator maps that bearer token to the tenant the facade
// sees.
func TestDial_BearerFromDSNKeyAuthenticatesTenant(t *testing.T) {
	addr, fab := serveTenant(t)

	dsn := fmt.Sprintf("fabriq+grpc://s3cret@%s/acme?tls=false", addr)
	f, err := remotegrpc.Dial(context.Background(), dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	res, err := f.Exec(context.Background(), command.Command{
		Entity: "asset", Op: command.OpCreate,
		Payload: &asset{ID: "asset-1", TenantID: "acme", Name: "Pump A"},
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.AggID != "asset-1" {
		t.Fatalf("res.AggID = %q, want asset-1", res.AggID)
	}
	if fab.gotTenant != "acme" {
		t.Fatalf("facade saw tenant %q, want acme (from DSN key -> bearer -> authenticator)", fab.gotTenant)
	}
}

// TestDial_OverMTLS proves Dial secures the channel with the caller-supplied
// client TLS config (WithTLSConfig) and still delivers the bearer credential:
// the DSN's tls=true selects TLS transport, the client presents its cert to a
// RequireAndVerifyClientCert server, and Exec crosses the encrypted channel.
func TestDial_OverMTLS(t *testing.T) {
	cert, pool := genTestCert(t)
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	addr, fab := serveTenant(t, grpc.Creds(credentials.NewTLS(serverTLS)))

	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   "bufnet", // matches the cert SAN; TCP still goes to 127.0.0.1
		MinVersion:   tls.VersionTLS13,
	}
	dsn := fmt.Sprintf("fabriq+grpc://s3cret@%s/acme?tls=true", addr)
	f, err := remotegrpc.Dial(context.Background(), dsn, remotegrpc.WithTLSConfig(clientTLS))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	if _, err := f.Exec(context.Background(), command.Command{
		Entity: "asset", Op: command.OpCreate,
		Payload: &asset{ID: "asset-1", TenantID: "acme", Name: "Pump A"},
	}); err != nil {
		t.Fatalf("Exec over mTLS: %v", err)
	}
	if fab.gotTenant != "acme" {
		t.Fatalf("facade saw tenant %q, want acme", fab.gotTenant)
	}
}

// TestDial_RejectsNonGrpcDSN guards the transport check: an http-scheme DSN
// belongs to client.Connect, not here.
func TestDial_RejectsNonGrpcDSN(t *testing.T) {
	_, err := remotegrpc.Dial(context.Background(), "fabriq://fq_key@example.com/acme")
	if err == nil {
		t.Fatal("Dial(fabriq://...) = nil error, want a non-grpc-transport rejection")
	}
}

// TestDial_PropagatesParseError guards that a malformed DSN surfaces the parser's
// error rather than a nil client.
func TestDial_PropagatesParseError(t *testing.T) {
	_, err := remotegrpc.Dial(context.Background(), "fabriq+grpc://") // no key, no host
	if err == nil {
		t.Fatal("Dial(malformed) = nil error, want a parse error")
	}
}

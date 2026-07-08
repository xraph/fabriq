package remotegrpc_test

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/remote"
	remotegrpc "github.com/xraph/fabriq/remote/grpc"
)

func execCreate(t *testing.T, f *remote.Fabric) {
	t.Helper()
	if _, err := f.Exec(context.Background(), command.Command{
		Entity: "asset", Op: command.OpCreate,
		Payload: &asset{ID: "asset-1", TenantID: "acme", Name: "Pump A"},
	}); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

// TestNewServer_ServesEmbeddedHandler proves the server-role one-liner: NewServer
// wraps a remote.Handler (built from an embedded facade) into an auth-enforcing
// *grpc.Server that a Dial'd client round-trips against.
func TestNewServer_ServesEmbeddedHandler(t *testing.T) {
	fab := &fakeFabric{result: command.Result{AggID: "asset-1", Version: 1, EventID: "evt-1"}}
	srv := remotegrpc.NewServer(remote.NewHandler(fab, assetRegistry(t)), acmeBearerAuth())
	addr := serveOnLoopback(t, srv)

	f, err := remotegrpc.Dial(context.Background(), "fabriq+grpc://s3cret@"+addr+"/acme?tls=false")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	execCreate(t, f)
	if fab.gotTenant != "acme" {
		t.Fatalf("facade saw tenant %q, want acme", fab.gotTenant)
	}
}

// TestNewServer_WithServerTLS proves WithServerTLS installs the server transport
// credentials (mTLS) so a matching Dial(...WithTLSConfig) connects.
func TestNewServer_WithServerTLS(t *testing.T) {
	cert, pool := genTestCert(t)
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	fab := &fakeFabric{result: command.Result{AggID: "asset-1", Version: 1, EventID: "evt-1"}}
	srv := remotegrpc.NewServer(remote.NewHandler(fab, assetRegistry(t)), acmeBearerAuth(),
		remotegrpc.WithServerTLS(serverTLS))
	addr := serveOnLoopback(t, srv)

	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   "bufnet",
		MinVersion:   tls.VersionTLS13,
	}
	f, err := remotegrpc.Dial(context.Background(), "fabriq+grpc://s3cret@"+addr+"/acme?tls=true",
		remotegrpc.WithTLSConfig(clientTLS))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	execCreate(t, f)
	if fab.gotTenant != "acme" {
		t.Fatalf("facade saw tenant %q, want acme", fab.gotTenant)
	}
}

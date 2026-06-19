package remotegrpc_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/remote"
	remotegrpc "github.com/xraph/fabriq/remote/grpc"
)

// bearerResolver maps the bearer token straight to a tenant id (the token IS
// the tenant, for the test).
func bearerResolver(ctx context.Context) (string, error) {
	tok, ok := remotegrpc.BearerToken(ctx)
	if !ok {
		return "", errors.New("missing bearer token")
	}
	return tok, nil
}

// TestGRPC_AuthStampsTenantFromBearer proves the edge-authenticated identity —
// not any request field — reaches the facade: a Bearer token resolves to a
// tenant that tenant.Require sees inside Exec.
func TestGRPC_AuthStampsTenantFromBearer(t *testing.T) {
	fab := &fakeFabric{result: command.Result{AggID: "a1", Version: 1}}
	client := dial(t, fab, remotegrpc.ServerOptions(remotegrpc.TenantOnly(bearerResolver))...)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer acme")
	if _, err := client.Exec(ctx, command.Command{
		Entity: "asset", Op: command.OpCreate, Payload: &asset{ID: "a1", TenantID: "acme"},
	}); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if fab.gotTenant != "acme" {
		t.Fatalf("facade saw tenant %q, want acme", fab.gotTenant)
	}
}

// TestGRPC_AuthRejectsMissingCredentialUnary proves an unauthenticated unary
// call is rejected before the handler runs.
func TestGRPC_AuthRejectsMissingCredentialUnary(t *testing.T) {
	client := dial(t, &fakeFabric{}, remotegrpc.ServerOptions(remotegrpc.TenantOnly(bearerResolver))...)

	_, err := client.Exec(context.Background(), command.Command{
		Entity: "asset", Op: command.OpCreate, Payload: &asset{ID: "x"},
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("err code = %v (%v), want Unauthenticated", status.Code(err), err)
	}
}

// TestGRPC_AuthRejectsMissingCredentialStream proves the stream interceptor
// rejects too — the rejection surfaces on the client's first Recv.
func TestGRPC_AuthRejectsMissingCredentialStream(t *testing.T) {
	client := dial(t, &fakeFabric{}, remotegrpc.ServerOptions(remotegrpc.TenantOnly(bearerResolver))...)

	_, err := client.Subscribe(context.Background(), query.SubscribeScope{Entity: "asset"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("err code = %v (%v), want Unauthenticated", status.Code(err), err)
	}
}

// TestGRPC_MTLSStampsTenantFromClientCert proves the full mTLS path: a client
// presenting a verified certificate has its CN mapped to a tenant by the
// resolver via ClientCertificate, and that tenant reaches the facade.
func TestGRPC_MTLSStampsTenantFromClientCert(t *testing.T) {
	fab := &fakeFabric{result: command.Result{AggID: "a1", Version: 1}}
	resolve := func(ctx context.Context) (string, error) {
		cert, ok := remotegrpc.ClientCertificate(ctx)
		if !ok {
			return "", errors.New("no client certificate")
		}
		return cert.Subject.CommonName, nil
	}
	client := dialMTLS(t, fab, remotegrpc.TenantOnly(resolve))

	if _, err := client.Exec(context.Background(), command.Command{
		Entity: "asset", Op: command.OpCreate, Payload: &asset{ID: "a1"},
	}); err != nil {
		t.Fatalf("Exec over mTLS: %v", err)
	}
	if fab.gotTenant != "acme" {
		t.Fatalf("facade saw tenant %q, want acme (from client cert CN)", fab.gotTenant)
	}
}

// dialMTLS stands up a TLS-credentialed server (RequireAndVerifyClientCert) and
// a client presenting the same cert, both over bufconn — a real mTLS handshake.
func dialMTLS(t *testing.T, fab query.Fabric, auth remotegrpc.Authenticator) *remote.RemoteFabric {
	t.Helper()
	cert, pool := genTestCert(t)
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   "bufnet",
		MinVersion:   tls.VersionTLS13,
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(append(remotegrpc.ServerOptions(auth), grpc.Creds(credentials.NewTLS(serverTLS)))...)
	remotegrpc.Register(srv, remote.NewHandler(fab, assetRegistry(t)))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return remote.New(remotegrpc.NewClient(cc))
}

// genTestCert builds one self-signed ECDSA certificate (CN "acme", SAN
// "bufnet") usable as CA, server leaf and client leaf — enough for a self-
// contained mTLS handshake in-test.
func genTestCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "acme"},
		DNSNames:              []string{"bufnet"},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Unix(1<<31-1, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

// principalKey is a test stand-in for an app's own identity context key; the
// app's authz hooks read whatever the Authenticator stamps under it.
type principalKey struct{}

func withPrincipal(ctx context.Context, p string) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

func principalFrom(ctx context.Context) string {
	p, _ := ctx.Value(principalKey{}).(string)
	return p
}

// TestGRPC_AuthEnrichesContextWithPrincipal proves an Authenticator can carry an
// app-defined principal alongside the tenant, and that BOTH reach the facade —
// where the authz hooks read them.
func TestGRPC_AuthEnrichesContextWithPrincipal(t *testing.T) {
	fab := &fakeFabric{result: command.Result{AggID: "a1", Version: 1}}
	auth := func(ctx context.Context) (context.Context, error) {
		tok, ok := remotegrpc.BearerToken(ctx)
		if !ok {
			return nil, errors.New("missing bearer token")
		}
		ctx, err := remotegrpc.WithTenant(ctx, "acme")
		if err != nil {
			return nil, err
		}
		return withPrincipal(ctx, tok), nil // token → principal
	}
	client := dial(t, fab, remotegrpc.ServerOptions(auth)...)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer user-1")
	if _, err := client.Exec(ctx, command.Command{
		Entity: "asset", Op: command.OpCreate, Payload: &asset{ID: "a1"},
	}); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if fab.gotTenant != "acme" || fab.gotPrincipal != "user-1" {
		t.Fatalf("facade saw tenant=%q principal=%q, want acme / user-1", fab.gotTenant, fab.gotPrincipal)
	}
}

// TestGRPC_AuthRejectsWhenNoTenantStamped proves the binding's invariant: an
// Authenticator that verifies a credential but forgets to stamp a tenant is
// rejected, not let through to fail confusingly deeper in.
func TestGRPC_AuthRejectsWhenNoTenantStamped(t *testing.T) {
	auth := func(ctx context.Context) (context.Context, error) {
		if _, ok := remotegrpc.BearerToken(ctx); !ok {
			return nil, errors.New("missing bearer token")
		}
		return ctx, nil // BUG: no tenant stamped
	}
	client := dial(t, &fakeFabric{}, remotegrpc.ServerOptions(auth)...)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer x")
	_, err := client.Exec(ctx, command.Command{
		Entity: "asset", Op: command.OpCreate, Payload: &asset{ID: "x"},
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("err code = %v (%v), want Unauthenticated", status.Code(err), err)
	}
}

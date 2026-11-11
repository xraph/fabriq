package elastic_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/elastic"
	"github.com/xraph/fabriq/core/registry"
)

func TestElasticsearchTransport(t *testing.T) {
	for _, tc := range []struct {
		name, product string
		withTLS       bool
		trust         string
		wrongHost     bool
		wantRequests  int32
		wantOpen      bool
	}{
		{name: "legacy plaintext", product: "Elasticsearch", wantRequests: 1, wantOpen: true},
		{name: "runtime HTTPS trust", product: "Elasticsearch", withTLS: true, trust: "runtime", wantRequests: 1, wantOpen: true},
		{name: "private CA with native product", product: "Elasticsearch", withTLS: true, trust: "inline", wantRequests: 1, wantOpen: true},
		{name: "wrong product", product: "OpenSearch", withTLS: true, trust: "inline", wantRequests: 1},
		{name: "wrong trust root", product: "Elasticsearch", withTLS: true, trust: "wrong"},
		{name: "wrong host", product: "Elasticsearch", withTLS: true, trust: "inline", wrongHost: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if user, pass, ok := r.BasicAuth(); !ok || user != "reader" || pass != "fixture-password" {
					t.Error("expected the configured Basic auth credentials")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				w.Header().Set("X-Elastic-Product", tc.product)
				_, _ = io.WriteString(w, `{"version":{"number":"9.4.1"}}`)
			}))
			t.Cleanup(server.Close)
			if tc.withTLS {
				server.StartTLS()
			} else {
				server.Start()
			}

			var ca []byte
			switch tc.trust {
			case "inline":
				ca = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			case "wrong":
				ca = otherElasticCA(t)
			case "runtime":
				roots := x509.NewCertPool()
				roots.AddCert(server.Certificate())
				useTestHTTPTransport(t, &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, nil)
			}
			if tc.wrongHost {
				// Keep the actual socket local while presenting a different URL hostname.
				dialer := &net.Dialer{}
				useTestHTTPTransport(t, nil, func(ctx context.Context, network, _ string) (net.Conn, error) {
					return dialer.DialContext(ctx, network, server.Listener.Addr().String())
				})
			}
			addr := server.URL
			if tc.wrongHost {
				addr = "https://elastic.invalid"
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			a, err := elastic.Open(ctx, elasticConfig(t, []string{addr}, ca), registry.New())
			if a != nil {
				defer func() { _ = a.Close() }()
			}
			if (err == nil) != tc.wantOpen {
				t.Fatalf("Open = %v, want success %v", err, tc.wantOpen)
			}
			if got := requests.Load(); got != tc.wantRequests {
				t.Fatalf("HTTP requests = %d, want %d", got, tc.wantRequests)
			}
		})
	}
}

func TestElasticsearchRejectsTrustBeforeDial(t *testing.T) {
	var dials atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			dials.Add(1)
		}
	}
	server.StartTLS()
	t.Cleanup(server.Close)

	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	privateKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("fixture-secret")})
	otherBlock := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("fixture-secret")})
	for _, tc := range []struct {
		name  string
		addrs []string
		ca    []byte
	}{
		{name: "missing addresses", ca: ca},
		{name: "plaintext address", addrs: []string{strings.Replace(server.URL, "https", "http", 1)}, ca: ca},
		{name: "mixed schemes", addrs: []string{server.URL, "http://localhost:1"}, ca: ca},
		{name: "address credentials", addrs: []string{strings.Replace(server.URL, "://", "://fixture-secret@", 1)}, ca: ca},
		{name: "address missing host", addrs: []string{"https://:9200"}, ca: ca},
		{name: "whitespace bundle", addrs: []string{server.URL}, ca: []byte(" \n\t ")},
		{name: "arbitrary prefix", addrs: []string{server.URL}, ca: append([]byte("fixture-secret\n"), ca...)},
		{name: "malformed skipped prefix", addrs: []string{server.URL}, ca: append([]byte("-----BEGIN CERTIFICATE-----\nbroken\n"), ca...)},
		{name: "malformed certificate", addrs: []string{server.URL}, ca: []byte("-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n")},
		{name: "private key", addrs: []string{server.URL}, ca: append(privateKey, ca...)},
		{name: "other PEM type", addrs: []string{server.URL}, ca: append(otherBlock, ca...)},
		{name: "too many certificates", addrs: []string{server.URL}, ca: []byte(strings.Repeat(string(ca), 9))},
		{name: "oversized bundle", addrs: []string{server.URL}, ca: append([]byte(strings.Repeat(" ", 64<<10)), ca...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			a, err := elastic.Open(ctx, elasticConfig(t, tc.addrs, tc.ca), registry.New())
			if a != nil {
				_ = a.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "elasticsearch") || strings.Contains(err.Error(), "ping") {
				t.Fatalf("expected Elasticsearch config error, got %v", err)
			}
			for _, forbidden := range []string{server.URL, "fixture-password", "fixture-secret", "BEGIN CERTIFICATE"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatal("public error exposed a config value")
				}
			}
			if dials.Load() != 0 {
				t.Fatal("invalid trust settings reached the socket")
			}
		})
	}
}

func elasticConfig(t *testing.T, addrs []string, ca []byte) elastic.Config {
	t.Helper()
	var cfg elastic.Config
	raw, err := json.Marshal(map[string]any{"Addrs": addrs, "Username": "reader", "Password": "fixture-password", "CACert": ca})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func useTestHTTPTransport(t *testing.T, cfg *tls.Config, dial func(context.Context, string, string) (net.Conn, error)) {
	t.Helper()
	previous := http.DefaultTransport
	transport := &http.Transport{TLSClientConfig: cfg, DialContext: dial}
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = previous
		transport.CloseIdleConnections()
	})
}

func otherElasticCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

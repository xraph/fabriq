package opensearch_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/opensearch"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
)

func TestOpenIdentityAuthAndTLS(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		wantError  bool
	}{
		{"native", `{"version":{"distribution":"opensearch","number":"3.9.0"}}`, 200, false},
		{"wrong engine", `{"version":{"number":"9.4.1"},"tagline":"You Know, for Search"}`, 200, true},
		{"unauthorized", `{"error":{"reason":"secret"}}`, 401, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if u, p, ok := r.BasicAuth(); !ok || u != "admin" || p != "secret" {
					t.Error("missing native basic authentication")
				}
				if strings.Contains(r.Header.Get("Accept"), "elasticsearch") || r.Header.Get("X-Elastic-Client-Meta") != "" {
					t.Error("elastic transport leaked into native request")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			cfg := opensearch.Config{Addrs: []string{srv.URL}, Username: "admin", Password: "secret", CACert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})}
			a, err := opensearch.Open(context.Background(), cfg, registry.New())
			if (err != nil) != tc.wantError {
				t.Fatalf("Open: %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatalf("credential in error: %v", err)
			}
			if tc.status == 401 {
				var fe *fabriqerr.Error
				if !errors.As(err, &fe) || fe.Code != fabriqerr.CodeUnauthorized {
					t.Fatalf("wrong auth error: %v", err)
				}
			}
			if a != nil {
				defer a.Close()
				if err := a.Ping(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			cfg.CACert = nil
			if _, err := opensearch.Open(context.Background(), cfg, registry.New()); err == nil {
				t.Fatal("untrusted TLS accepted")
			}
		})
	}
}

func testCACert(t *testing.T, hostname string) []byte {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: hostname}, DNSNames: []string{hostname}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestInlineCATrustRejectsBeforeRequest(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"version":{"distribution":"opensearch"}}`))
	}))
	defer srv.Close()
	valid := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	key := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("credential-do-not-log")})
	for _, tc := range []struct {
		name string
		ca   []byte
		file string
	}{
		{"HTTP endpoint", valid, ""},
		{"conflicting sources", valid, "/must-not-read"},
		{"whitespace", []byte(" \n"), ""},
		{"garbage", []byte("credential-do-not-log"), ""},
		{"empty certificate", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE"}), ""},
		{"invalid DER", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("invalid")}), ""},
		{"private key", key, ""},
		{"mixed key", append(append([]byte{}, valid...), key...), ""},
		{"trailing garbage", append(append([]byte{}, valid...), []byte("credential-do-not-log")...), ""},
		{"leading garbage", append([]byte("credential-do-not-log"), valid...), ""},
		{"malformed before valid", append([]byte("-----BEGIN CERTIFICATE-----\nbroken\n-----END CERTIFICATE-----\n"), valid...), ""},
		{"oversized", append(append([]byte{}, valid...), []byte(strings.Repeat(" ", (64<<10)+1))...), ""},
		{"non-certificate block", pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("invalid")}), ""},
		{"certificate headers", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Headers: map[string]string{"Comment": "invalid"}, Bytes: srv.Certificate().Raw}), ""},
		{"nested certificate header", append([]byte("-----BEGIN CERTIFICATE-----\nbroken\n"), valid...), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests.Store(0)
			addr := srv.URL
			if tc.name == "HTTP endpoint" {
				addr = strings.Replace(addr, "https:", "http:", 1)
			}
			cfg := opensearch.Config{Addrs: []string{addr}, CACert: tc.ca, CAFile: tc.file}
			if err := cfg.Validate(); err == nil {
				t.Error("invalid trust passed no-dial validation")
			}
			a, err := opensearch.Open(context.Background(), cfg, registry.New())
			if a != nil {
				_ = a.Close()
			}
			var fe *fabriqerr.Error
			if !errors.As(err, &fe) || fe.Code != fabriqerr.CodeInvalidInput {
				t.Fatalf("expected invalid input, got %v", err)
			}
			if strings.Contains(err.Error(), "credential-do-not-log") || strings.Contains(err.Error(), "BEGIN") {
				t.Fatal("trust material leaked into error")
			}
			if requests.Load() != 0 {
				t.Fatal("invalid trust reached server")
			}
		})
	}
}

func TestInlineCAWrongTrustAndHostname(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"version":{"distribution":"opensearch"}}`))
	}))
	defer srv.Close()
	for _, tc := range []struct {
		name string
		ca   []byte
		addr string
	}{
		{"wrong CA", testCACert(t, "other.test"), srv.URL},
		{"wrong hostname", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := opensearch.Open(context.Background(), opensearch.Config{Addrs: []string{tc.addr}, CACert: tc.ca}, registry.New())
			if a != nil {
				_ = a.Close()
			}
			if err == nil {
				t.Fatal("TLS verification accepted invalid trust or hostname")
			}
		})
	}
}

func TestInlineCACompatibleTrustSources(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"version":{"distribution":"opensearch"}}`))
	}))
	defer srv.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, append(append([]byte{}, ca...), []byte(strings.Repeat(" ", 64<<10))...), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		ca   []byte
		file string
	}{
		{"CA file over inline limit", nil, caFile},
		{"certificate bundle", append(append([]byte{}, ca...), ca...), ""},
		{"CRLF", []byte(strings.ReplaceAll(string(ca), "\n", "\r\n")), ""},
		{"maximum length", append(append([]byte{}, ca...), []byte(strings.Repeat(" ", (64<<10)-len(ca)))...), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := opensearch.Open(context.Background(), opensearch.Config{Addrs: []string{srv.URL}, CACert: tc.ca, CAFile: tc.file}, registry.New())
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			if err := a.Ping(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
	cfg := opensearch.Config{Addrs: []string{srv.URL, "http://unused:9200"}, CACert: ca}
	if err := cfg.Validate(); err == nil {
		t.Fatal("mixed HTTP/HTTPS trust accepted")
	}
	cfg.Addrs = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("inline trust without an HTTPS endpoint accepted")
	}
}

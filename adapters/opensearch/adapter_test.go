package opensearch_test

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

package fabriq

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
)

func TestSearchBackendConfig(t *testing.T) {
	base := Config{Postgres: PostgresConfig{DSN: "postgres://unused"}, Redis: RedisConfig{Addr: "unused"}, Projections: ProjectionsConfig{Search: true}}
	for _, engine := range []string{"elastic", "opensearch"} {
		cfg := base
		if engine == "elastic" {
			cfg.Elasticsearch.Addrs = []string{"http://unused"}
		} else {
			cfg.OpenSearch.Addrs = []string{"http://unused"}
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("%s: %v", engine, err)
		}
	}
	base.Elasticsearch.Addrs = []string{"http://unused"}
	base.OpenSearch.Addrs = []string{"http://unused"}
	_, _, err := Open(context.Background(), registry.New(), base)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("must reject both before dialing: %v", err)
	}
}

func TestSearchBackendRuntimeSelection(t *testing.T) {
	for _, engine := range []string{"opensearch", "elasticsearch"} {
		t.Run(engine, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if engine == "elasticsearch" {
					w.Header().Set("X-Elastic-Product", "Elasticsearch")
				}
				_, _ = w.Write([]byte(`{"version":{"distribution":"opensearch","number":"3.9.0"}}`))
			}))
			defer srv.Close()
			cfg := Config{}
			if engine == "opensearch" {
				cfg.OpenSearch.Addrs = []string{srv.URL}
			} else {
				cfg.Elasticsearch.Addrs = []string{srv.URL}
			}
			stores := &Stores{}
			ports := Ports{}
			if err := openSearch(context.Background(), cfg, registry.New(), stores, &ports); err != nil {
				t.Fatal(err)
			}
			if !stores.SearchConfigured() || ports.Search == nil {
				t.Fatal("search backend not available")
			}
			if (stores.Elastic != nil) != (engine == "elasticsearch") {
				t.Fatal("public Elastic handle changed engine")
			}
			if err := stores.PingSearch(context.Background()); err != nil {
				t.Fatal(err)
			}
			// External callers can still construct Stores with the public Elastic field.
			if stores.Elastic != nil {
				legacy := &Stores{Elastic: stores.Elastic}
				if !legacy.SearchConfigured() {
					t.Fatal("legacy Elastic handle ignored")
				}
			}
			if err := stores.Close(); err != nil {
				t.Fatal(err)
			}
			if err := stores.PingSearch(context.Background()); err == nil {
				t.Fatal("search transport survived Stores.Close")
			}
		})
	}
}

func TestOpenSearchInlineCAConfigAndTransport(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if username, password, ok := r.BasicAuth(); !ok || username != "admin" || password != "secret" {
			t.Error("missing authentication")
		}
		_, _ = w.Write([]byte(`{"version":{"distribution":"opensearch"}}`))
	}))
	defer srv.Close()
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	cfg := Config{OpenSearch: OpenSearchConfig{Addrs: []string{srv.URL}, Username: "admin", Password: "secret", CACert: ca}}
	stores := &Stores{}
	ports := Ports{}
	if err := openSearch(context.Background(), cfg, registry.New(), stores, &ports); err != nil {
		t.Fatal(err)
	}
	defer stores.Close()
	if err := stores.PingSearch(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, ca, file, addr string }{
		{"conflicting", ca, "/must-not-read", srv.URL},
		{"invalid", "credential-do-not-log", "", srv.URL},
		{"HTTP", ca, "", "http://unused:9200"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := Config{OpenSearch: OpenSearchConfig{Addrs: []string{tc.addr}, CACert: tc.ca, CAFile: tc.file}}
			// Open validates before attempting to open the absent primary database.
			_, _, err := Open(context.Background(), registry.New(), bad)
			var fe *fabriqerr.Error
			if !errors.As(err, &fe) || !strings.HasPrefix(fe.Op, "opensearch") {
				t.Fatalf("trust was not validated before opening stores: %v", err)
			}
			if strings.Contains(err.Error(), "credential-do-not-log") || strings.Contains(err.Error(), "BEGIN") {
				t.Fatal("trust input leaked")
			}
		})
	}
}

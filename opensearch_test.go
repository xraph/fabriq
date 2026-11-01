package fabriq

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

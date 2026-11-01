package elastic_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/fabriq/adapters/elastic"
	"github.com/xraph/fabriq/core/registry"
)

func TestNativeProductCheck(t *testing.T) {
	for _, native := range []bool{false, true} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if native {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
			}
			_, _ = w.Write([]byte(`{"version":{"number":"9.4.1"}}`))
		}))
		a, err := elastic.Open(context.Background(), elastic.Config{Addrs: []string{srv.URL}}, registry.New())
		srv.Close()
		if (err == nil) != native {
			t.Fatalf("native=%t: %v", native, err)
		}
		if native && a == nil {
			t.Fatal("missing compatible Elasticsearch adapter")
		}
	}
}

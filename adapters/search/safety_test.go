package search_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/fabriq/adapters/elastic"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

func TestProjectionIndexFailuresRemainErrors(t *testing.T) {
	for _, op := range []string{"create", "drop"} {
		t.Run(op, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				switch r.Method {
				case http.MethodPut:
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"error":{"type":"invalid_index_name_exception"}}`))
				case http.MethodDelete:
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"error":{"type":"security_exception"}}`))
				default:
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			defer srv.Close()
			reg := registry.New()
			if err := domain.RegisterAll(reg); err != nil {
				t.Fatal(err)
			}
			a, err := elastic.Open(context.Background(), elastic.Config{Addrs: []string{srv.URL}}, reg)
			if err != nil {
				t.Fatal(err)
			}
			ctx := tenant.MustWithTenant(context.Background(), "acme")
			if op == "create" {
				err = a.ApplyMutations(ctx, "v1", []projection.Mutation{projection.DocIndex{Index: "assets", ID: "A1", Version: 1, Doc: map[string]any{"id": "A1"}}})
			} else {
				err = a.DropTarget(ctx, "v1")
			}
			if err == nil {
				t.Fatalf("%s index failure reported as success", op)
			}
		})
	}
}

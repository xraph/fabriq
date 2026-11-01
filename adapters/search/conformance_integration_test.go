//go:build integration

package search_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/xraph/fabriq/adapters/elastic"
	"github.com/xraph/fabriq/adapters/opensearch"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
)

type searchPort interface {
	query.SearchQuerier
	projection.TargetSink
	FlipAliases(context.Context, string, int, int) error
	AggregateVersions(context.Context, string, *registry.Entity) (map[string]int64, error)
	Ping(context.Context) error
	Close() error
}

func TestNativeSearchConformance(t *testing.T) {
	for _, engine := range []string{"opensearch", "elasticsearch"} {
		t.Run(engine, func(t *testing.T) {
			reg := registry.New()
			if err := domain.RegisterAll(reg); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			liveVersion := 1
			resolver := func(context.Context, string) (int, error) { return liveVersion, nil }
			var a searchPort
			switch engine {
			case "opensearch":
				server := fabriqtest.StartOpenSearch(t)
				caFile := filepath.Join(t.TempDir(), "ca.pem")
				if err := os.WriteFile(caFile, server.CACert, 0o600); err != nil {
					t.Fatal(err)
				}
				cfg := opensearch.Config{Addrs: []string{server.URL}, Username: server.Username, Password: server.Password, CAFile: caFile}
				native, err := opensearch.Open(ctx, cfg, reg, opensearch.WithModelVersionResolver(resolver))
				if err != nil {
					t.Fatal(err)
				}
				a = native
				cfg.Password = "wrong-password"
				_, err = opensearch.Open(ctx, cfg, reg)
				var fe *fabriqerr.Error
				if !errors.As(err, &fe) || fe.Code != fabriqerr.CodeUnauthorized {
					t.Fatalf("invalid credentials: %v", err)
				}
				cfg.Password = server.Password
				cfg.CAFile = ""
				if _, err = opensearch.Open(ctx, cfg, reg); err == nil {
					t.Fatal("untrusted real-engine certificate accepted")
				}
				if _, err = elastic.Open(ctx, elastic.Config{Addrs: []string{server.URL}, Username: server.Username, Password: server.Password}, reg); err == nil {
					t.Fatal("Elastic accepted OpenSearch")
				}
			case "elasticsearch":
				url := fabriqtest.StartElasticsearch(t)
				native, err := elastic.Open(ctx, elastic.Config{Addrs: []string{url}}, reg, elastic.WithModelVersionResolver(resolver))
				if err != nil {
					t.Fatal(err)
				}
				a = native
				if _, err := opensearch.Open(ctx, opensearch.Config{Addrs: []string{url}}, reg); err == nil {
					t.Fatal("OpenSearch accepted Elastic")
				}
			}
			t.Cleanup(func() { _ = a.Close() })
			if err := a.Ping(ctx); err != nil {
				t.Fatal(err)
			}
			acme, err := tenant.WithTenant(ctx, "search-acme")
			if err != nil {
				t.Fatal(err)
			}
			rival, err := tenant.WithTenant(ctx, "search-rival")
			if err != nil {
				t.Fatal(err)
			}
			doc := func(id, name string, v int64) projection.Mutation {
				return projection.DocIndex{Index: "assets", ID: id, Version: v, Doc: map[string]any{"id": id, "name": name, "version": v}}
			}
			apply := func(ctx context.Context, target string, muts ...projection.Mutation) {
				t.Helper()
				if err := a.ApplyMutations(ctx, target, muts); err != nil {
					t.Fatal(err)
				}
			}
			find := func(ctx context.Context, q query.SearchQuery) []map[string]any {
				t.Helper()
				q.Entity = "asset"
				var hits []map[string]any
				if err := a.Search(ctx, q, &hits); err != nil {
					t.Fatal(err)
				}
				return hits
			}
			apply(acme, "", doc("A1", "Main Pump", 2), doc("A2", "Backup Pump", 3), doc("A3", "Inlet Valve", 4))
			apply(acme, "", doc("A1", "Main Pump", 2), doc("A1", "Stale Name", 1))
			if hits := find(acme, query.SearchQuery{Query: "pump", Sort: "version"}); len(hits) != 2 || hits[0]["name"] != "Main Pump" {
				t.Fatalf("duplicate/stale gate: %v", hits)
			}
			if hits := find(acme, query.SearchQuery{Query: "pump", Filter: query.Where{query.Gte("version", 3)}, Sort: "version"}); len(hits) != 1 || hits[0]["id"] != "A2" {
				t.Fatalf("text/filter: %v", hits)
			}
			if hits := find(acme, query.SearchQuery{Filter: query.Where{query.Eq("name", "Main Pump")}}); len(hits) != 1 {
				t.Fatalf("keyword filter: %v", hits)
			}
			if hits := find(acme, query.SearchQuery{Sort: "version", Offset: 1, Limit: 1}); len(hits) != 1 || hits[0]["id"] != "A2" {
				t.Fatalf("sort/pagination: %v", hits)
			}
			apply(rival, "", doc("A1", "Rival Pump", 1))
			if hits := find(rival, query.SearchQuery{Query: "pump"}); len(hits) != 1 || hits[0]["name"] != "Rival Pump" {
				t.Fatalf("tenant isolation: %v", hits)
			}
			var hits []map[string]any
			if err := a.Search(ctx, query.SearchQuery{Entity: "asset"}, &hits); !errors.Is(err, tenant.ErrNoTenant) {
				t.Fatalf("unscoped search: %v", err)
			}
			if err := a.ApplyMutations(ctx, "", []projection.Mutation{doc("X", "No Tenant", 1)}); !errors.Is(err, tenant.ErrNoTenant) {
				t.Fatalf("unscoped write: %v", err)
			}
			if err := a.Search(acme, query.SearchQuery{Entity: "asset", Filter: query.Where{query.Eq("site_id", "other")}}, &hits); err == nil {
				t.Fatal("undeclared filter accepted")
			}
			ent, _ := reg.Get("asset")
			versions, err := a.AggregateVersions(ctx, "search-acme", ent)
			if err != nil || versions["A1"] != 2 || len(versions) != 3 {
				t.Fatalf("versions: %v %v", versions, err)
			}
			apply(acme, "", projection.DocDeindex{Index: "assets", ID: "A2", Version: 4})
			apply(acme, "", doc("A2", "Stale Deleted Pump", 3))
			if hits := find(acme, query.SearchQuery{Query: "pump"}); len(hits) != 1 {
				t.Fatalf("delete/stale replay: %v", hits)
			}
			apply(acme, "v2", doc("A1", "Rebuilt Motor", 5))
			if hits := find(acme, query.SearchQuery{Query: "motor"}); len(hits) != 0 {
				t.Fatalf("rebuild visible before cutover: %v", hits)
			}
			if err := a.FlipAliases(ctx, "search-acme", 1, 2); err != nil {
				t.Fatal(err)
			}
			liveVersion = 2
			if hits := find(acme, query.SearchQuery{Query: "motor"}); len(hits) != 1 {
				t.Fatalf("cutover: %v", hits)
			}
			apply(acme, "", doc("A4", "Live Motor", 1))
			if err := a.DropTarget(acme, "v1"); err != nil {
				t.Fatal(err)
			}
			if hits := find(acme, query.SearchQuery{Query: "motor"}); len(hits) != 2 {
				t.Fatalf("live routing after cleanup: %v", hits)
			}
			if hits := find(rival, query.SearchQuery{Query: "pump"}); len(hits) != 1 {
				t.Fatalf("cutover crossed tenant: %v", hits)
			}
		})
	}
}

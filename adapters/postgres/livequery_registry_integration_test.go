//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestPGLiveSubscriptionRegistry(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
	db := pgdriver.New()
	if err := db.Open(ctx, superDSN); err != nil {
		t.Fatalf("open pg: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	orch, err := migrations.NewOrchestrator(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	reg := postgres.NewLiveSubscriptionRegistry(db)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	mk := func(id, tenant, entity, gw string, where query.Where) livequery.Registration {
		return livequery.Registration{
			SubID: id, TenantID: tenant, Entity: entity, Mode: livequery.ModeMaintained,
			Query:     livequery.LiveQuery{Entity: entity, Where: where, Sort: []livequery.SortKey{{Column: "name"}}, Limit: 10},
			GatewayID: gw, Watermark: "0-0",
		}
	}

	must(reg.Put(ctx, mk("s1", "acme", "asset", "gw1", query.Where{query.Eq("kind", "pump")})))
	must(reg.Put(ctx, mk("s2", "acme", "asset", "gw2", query.Where{query.Eq("kind", "valve")})))
	must(reg.Put(ctx, mk("s3", "acme", "site", "gw1", nil)))

	// Partition rebuild query: a shard owning (acme, asset) loads s1 + s2.
	parts, err := reg.ByPartition(ctx, "acme", "asset")
	must(err)
	if len(parts) != 2 {
		t.Fatalf("ByPartition(acme,asset) = %d rows, want 2", len(parts))
	}
	var sawS1 bool
	for _, p := range parts {
		if p.SubID == "s1" {
			sawS1 = true
			if len(p.Query.Where) != 1 || p.Query.Where[0].Column != "kind" || p.Query.Where[0].Value != "pump" {
				t.Fatalf("query did not round-trip: %+v", p.Query.Where)
			}
		}
	}
	if !sawS1 {
		t.Fatal("s1 missing from partition rebuild")
	}

	// Gateway recovery query.
	gw, err := reg.ByGateway(ctx, "gw1")
	must(err)
	if len(gw) != 2 {
		t.Fatalf("ByGateway(gw1) = %d rows, want 2 (s1, s3)", len(gw))
	}

	// Idempotent update (re-Put same id).
	must(reg.Put(ctx, mk("s1", "acme", "asset", "gw1", query.Where{query.Eq("kind", "pump")})))
	parts, _ = reg.ByPartition(ctx, "acme", "asset")
	if len(parts) != 2 {
		t.Fatalf("after re-Put = %d rows, want 2 (no duplicate)", len(parts))
	}

	// Clean unsubscribe.
	must(reg.Delete(ctx, "s1"))
	parts, _ = reg.ByPartition(ctx, "acme", "asset")
	if len(parts) != 1 || parts[0].SubID != "s2" {
		t.Fatalf("after delete = %+v, want [s2]", parts)
	}
}

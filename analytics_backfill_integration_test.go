//go:build integration

package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestBackfill_RealTwoTenantDBs proves the CLI backfill path end to end
// against real Postgres containers: a tenant database (RLS-scoped, two
// tenants) and a SEPARATE analytics database, wired exactly the way
// Stores.AnalyticsBackfiller assembles them (routed snapshot -> applier ->
// sink). It does not use a catalog/two-DB tenancy harness — the routed
// snapshot in single-shard mode is the same analytics.SnapshotFunc a
// catalog-mode deployment would use (Task 8 already proved catalog-agnostic
// applier/sink behavior), so this is the minimal reliable rig for the CLI
// wiring itself.
func TestBackfill_RealTwoTenantDBs(t *testing.T) {
	ctx := context.Background()

	tenantSuperDSN := fabriqtest.StartPostgres(t)
	analyticsDSN := fabriqtest.StartPostgres(t)
	if tenantSuperDSN == analyticsDSN {
		t.Fatal("expected distinct tenant and analytics DSNs")
	}

	// Register just the "site" entity (statically, not via domain.RegisterAll
	// + Replace — Replace is dynamic-entities-only) with an AnalyticsSpec
	// attached, so its writes are marked for the sink. domain.DemoDDL below
	// still provisions the backing "sites" table.
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:      "site",
		Kind:      registry.KindAggregate,
		Model:     (*domain.Site)(nil),
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
		Analytics: &registry.AnalyticsSpec{Include: []string{"name", "code", "region"}},
	}); err != nil {
		t.Fatal(err)
	}

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, tenantSuperDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()
	fabriqtest.ApplyDDL(t, tenantSuperDSN, domain.DemoDDL())
	tenantAppDSN := fabriqtest.CreateAppRole(t, tenantSuperDSN)

	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres:  fabriq.PostgresConfig{DSN: tenantAppDSN},
		Analytics: fabriq.AnalyticsConfig{DSN: analyticsDSN},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	// Write rows for two tenants through the facade (command plane), the
	// same path production writes take. No relay/consumer is run here —
	// backfill reads current state directly, not the event stream.
	tenants := []string{"t1", "t2"}
	siteIDs := map[string]string{}
	for _, tid := range tenants {
		tctx, err := tenant.WithTenant(ctx, tid)
		if err != nil {
			t.Fatal(err)
		}
		res, err := f.Exec(tctx, command.Command{Entity: "site", Op: command.OpCreate,
			Payload: &domain.Site{Name: "Plant " + tid, Code: "P-" + tid, Region: "us"}})
		if err != nil {
			t.Fatalf("exec create for tenant %s: %v", tid, err)
		}
		siteIDs[tid] = res.AggID
	}

	bf, err := stores.AnalyticsBackfiller(reg)
	if err != nil {
		t.Fatal(err)
	}
	counts, err := bf.AllTenants(ctx, tenants, 2)
	if err != nil {
		t.Fatalf("AllTenants backfill: %v", err)
	}
	for _, tid := range tenants {
		if counts[tid] < 1 {
			t.Fatalf("tenant %s: expected >=1 backfilled row, got %d", tid, counts[tid])
		}
	}

	// Assert both tenants' facts landed in the analytics sink at their
	// current (v1) version.
	for _, tid := range tenants {
		v, err := stores.Analytics.Watermark(ctx, tid, "site", siteIDs[tid])
		if err != nil {
			t.Fatalf("watermark for tenant %s: %v", tid, err)
		}
		if v != 1 {
			t.Fatalf("tenant %s: watermark = %d, want 1", tid, v)
		}
	}

	// Re-running backfill is idempotent (version-gated no-op) — proves the
	// sink's dedup/gate semantics hold from the CLI's calling shape too.
	counts2, err := bf.AllTenants(ctx, tenants, 2)
	if err != nil {
		t.Fatalf("second AllTenants backfill: %v", err)
	}
	for _, tid := range tenants {
		if counts2[tid] != counts[tid] {
			t.Fatalf("tenant %s: re-run count = %d, want %d (idempotent)", tid, counts2[tid], counts[tid])
		}
	}

	// NOTE: step 6 (live write + brief consumer run advancing a fact past
	// the backfilled version) is skipped here — wiring the Redis-backed
	// proj:analytics consumer into this harness is a materially larger rig
	// (relay + elector + consumer group) than this CLI-focused test
	// warrants. Steps 1-5 (the required core) are covered above: two real
	// Postgres containers, real facade writes, real routed snapshot,
	// real backfiller, real analytics sink assertions, plus idempotency.
}

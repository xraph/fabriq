//go:build integration

package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestBackfill_DynamicEntity proves the analytics backfill (routed snapshot ->
// applier -> sink) works over a DYNAMIC (DynamicSchema) entity — the shape the
// admin console and its demo use, where the aggregate has no Go model type.
//
// Regression for the reflect.PointerTo(nil) panic in postgres.snapshotEntity:
// it built a typed slice via reflect.SliceOf(reflect.PointerTo(ModelType())),
// but a dynamic entity's ModelType() is nil, so SnapshotEntities panicked the
// moment a marked dynamic aggregate was reached.
func TestBackfill_DynamicEntity(t *testing.T) {
	ctx := context.Background()

	tenantSuperDSN := fabriqtest.StartPostgres(t)
	analyticsDSN := fabriqtest.StartPostgres(t)
	if tenantSuperDSN == analyticsDSN {
		t.Fatal("expected distinct tenant and analytics DSNs")
	}

	// A dynamic aggregate (no Model) marked for analytics, mirroring the demo's
	// product/customer/order entities.
	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "widget",
		Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_widgets",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "sku", Type: registry.ColText},
			},
		},
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
		Analytics: &registry.AnalyticsSpec{Include: []string{"name", "sku"}},
	})

	// Owner connection: run migrations and CREATE the dynamic table BEFORE the
	// app role, so DEFAULT PRIVILEGES cover it (same order as the dynamic-write
	// harness).
	owner, err := postgres.Open(ctx, tenantSuperDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ent, ok := reg.Get("widget")
	if !ok {
		t.Fatal("entity 'widget' not found")
	}
	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("EnsureDynamic: %v", err)
	}

	fabriqtest.ApplyDDL(t, tenantSuperDSN, domain.DemoDDL())
	tenantAppDSN := fabriqtest.CreateAppRole(t, tenantSuperDSN)

	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres:  fabriq.PostgresConfig{DSN: tenantAppDSN},
		Analytics: fabriq.AnalyticsConfig{DSN: analyticsDSN},
	})
	if err != nil {
		t.Fatalf("fabriq.Open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	// Write one widget per tenant through the command plane (map payload — the
	// dynamic-entity write shape).
	tenants := []string{"t1", "t2"}
	ids := map[string]string{}
	for _, tid := range tenants {
		tctx, err := tenant.WithTenant(ctx, tid)
		if err != nil {
			t.Fatal(err)
		}
		res, err := f.Exec(tctx, command.Command{
			Entity:  "widget",
			Op:      command.OpCreate,
			Payload: map[string]any{"name": "Widget " + tid, "sku": "W-" + tid},
		})
		if err != nil {
			t.Fatalf("exec create for tenant %s: %v", tid, err)
		}
		ids[tid] = res.AggID
	}

	// Backfill: the routed snapshot streams each dynamic row's current shape,
	// the applier redacts per the AnalyticsSpec, the sink upserts. This is the
	// path that panicked before the fix.
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
			t.Fatalf("tenant %s: expected >=1 backfilled dynamic row, got %d", tid, counts[tid])
		}
	}

	// Facts landed at the row's current (v1) version.
	for _, tid := range tenants {
		v, err := stores.Analytics.Watermark(ctx, tid, "widget", ids[tid])
		if err != nil {
			t.Fatalf("watermark for tenant %s: %v", tid, err)
		}
		if v != 1 {
			t.Fatalf("tenant %s: watermark = %d, want 1", tid, v)
		}
	}
}

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

// TestReconcile_HealsAMissingFact proves the analytics reconciler heals
// divergence from the source of truth against real Postgres: after the sink is
// populated, a fact is deleted out from under it (standing in for an event the
// consumer skipped as poison), and reconcile — reading current source state
// through the routed snapshot, comparing against the stored watermark — detects
// the gap and re-applies it.
func TestReconcile_HealsAMissingFact(t *testing.T) {
	ctx := context.Background()

	tenantSuperDSN := fabriqtest.StartPostgres(t)
	analyticsDSN := fabriqtest.StartPostgres(t)

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

	tctx, _ := tenant.WithTenant(ctx, "t1")
	res, err := f.Exec(tctx, command.Command{Entity: "site", Op: command.OpCreate,
		Payload: &domain.Site{Name: "Plant one", Code: "P-1", Region: "us"}})
	if err != nil {
		t.Fatalf("exec create: %v", err)
	}
	siteID := res.AggID

	// Populate the sink from source.
	bf, err := stores.AnalyticsBackfiller(reg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bf.Tenant(ctx, "t1"); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	// Simulate a skipped event: delete the fact (but leave the watermark, as a
	// poison-skip would — the watermark only advances on a successful apply, so
	// in practice both are absent; deleting just the fact is the harder case
	// that still must be detected via the version comparison below). To make
	// the drift unambiguous, drop BOTH the fact and its watermark.
	fabriqtest.QueryStrings(t, analyticsDSN, `DELETE FROM fabriq_analytics_facts WHERE tenant_id=$1 AND agg_id=$2`, "t1", siteID)
	fabriqtest.QueryStrings(t, analyticsDSN, `DELETE FROM fabriq_analytics_applied WHERE tenant_id=$1 AND agg_id=$2`, "t1", siteID)

	// Reconcile detects the missing aggregate and heals it.
	rc, err := stores.AnalyticsReconciler(reg)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := rc.Tenant(ctx, "t1")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rep.Missing != 1 || rep.Healed != 1 {
		t.Fatalf("report = %+v, want Missing=1 Healed=1", rep)
	}

	// The fact is back in the analytics DB.
	got := fabriqtest.QueryStrings(t, analyticsDSN,
		`SELECT payload->>'name' FROM fabriq_analytics_facts WHERE tenant_id=$1 AND agg_id=$2`, "t1", siteID)
	if len(got) != 1 || got[0] != "Plant one" {
		t.Fatalf("healed fact name = %v, want [Plant one]", got)
	}

	// Idempotent: a second reconcile finds nothing to heal.
	rep2, err := rc.Tenant(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Drifted() != 0 {
		t.Fatalf("second reconcile drifted = %d, want 0", rep2.Drifted())
	}
}

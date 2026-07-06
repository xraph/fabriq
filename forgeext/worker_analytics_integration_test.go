//go:build integration

package forgeext_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/migrations"
)

// TestAnalyticsConsumer_EndToEnd proves an analytics-marked entity written by
// a tenant flows through the outbox relay and the real proj:analytics
// consumer into a separate analytics Postgres database. It brings up the
// full plane via Extension with RunWorker=true (same harness shape as
// TestWorker_RunsAndProjects) but with FalkorDB/Elasticsearch omitted and
// Config.Analytics pointed at a THIRD, distinct Postgres container — the
// point being that analytics data lives in a database separate from the
// tenant's own.
func TestAnalyticsConsumer_EndToEnd(t *testing.T) {
	ctx := context.Background()

	// Boot infrastructure: tenant Postgres, Redis (shared event stream), and
	// a SEPARATE analytics Postgres (distinct DSN/container from the tenant).
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	analyticsDSN := fabriqtest.StartPostgres(t)

	if superDSN == analyticsDSN {
		t.Fatal("analytics DSN must differ from the tenant DSN")
	}

	// Run migrations as superuser on the tenant DB.
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatalf("open orchestrator: %v", err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = closeFn()

	// Create restricted app role so RLS applies, and seed the demo DDL (the
	// "site" table) that domain.Site maps onto.
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)

	// Build a registry with exactly one aggregate ("site"), marked for the
	// cross-tenant analytics sink with IncludeAll — the smallest surface
	// that still exercises the real Applier/Sink path end to end.
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:      "site",
		Kind:      registry.KindAggregate,
		Model:     (*domain.Site)(nil),
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
		Analytics: &registry.AnalyticsSpec{IncludeAll: true},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	// Create the extension with the worker enabled and analytics configured
	// against the separate analytics database.
	ext := forgeext.New(reg,
		forgeext.WithConfig(fabriq.Config{
			Postgres:  fabriq.PostgresConfig{DSN: appDSN},
			Redis:     fabriq.RedisConfig{Addr: redisAddr},
			Analytics: fabriq.AnalyticsConfig{DSN: analyticsDSN},
			Subscriptions: fabriq.SubscriptionsConfig{
				ConflationWindow: 30 * time.Millisecond,
			},
		}),
		forgeext.WithWorker(true),
	)

	app := forge.NewApp(forge.AppConfig{
		Name:        "fabriq-analytics-worker-test",
		HTTPAddress: ":0",
	})

	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := ext.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = ext.Shutdown(shutdownCtx)
		_ = ext.Stop(context.Background())
	})

	if err := ext.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	f := ext.Fabriq()
	if f == nil {
		t.Fatal("ext.Fabriq() returned nil after Start")
	}

	// Tenant "acme" creates a site.
	acmeCtx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("tenant.WithTenant(acme): %v", err)
	}
	acmeRes, err := f.Exec(acmeCtx, command.Command{
		Entity:  "site",
		Op:      command.OpCreate,
		Payload: &domain.Site{Name: "Plant A"},
	})
	if err != nil {
		t.Fatalf("Exec(OpCreate site, acme): %v", err)
	}

	// A second tenant "globex" also creates a site, to prove co-location
	// (both tenants' facts land in the same shared analytics DB) without
	// cross-contaminating each other's rows.
	globexCtx, err := tenant.WithTenant(ctx, "globex")
	if err != nil {
		t.Fatalf("tenant.WithTenant(globex): %v", err)
	}
	globexRes, err := f.Exec(globexCtx, command.Command{
		Entity:  "site",
		Op:      command.OpCreate,
		Payload: &domain.Site{Name: "Plant G"},
	})
	if err != nil {
		t.Fatalf("Exec(OpCreate site, globex): %v", err)
	}

	// Poll the analytics DB (opened directly, independent of the consumer)
	// until the proj:analytics consumer — started by ext.Run above inside
	// the worker plane because Analytics is configured and "site" is
	// analytics-marked — has applied both tenants' facts.
	db := pgdriver.New()
	if err := db.Open(ctx, analyticsDSN); err != nil {
		t.Fatalf("open analytics db: %v", err)
	}
	defer db.Close()

	deadline := time.Now().Add(30 * time.Second)
	var acmeVersion, globexVersion int64
	var acmeName, globexName string
	for {
		acmeVersion, acmeName = queryFactVersion(t, ctx, db, "acme", "site", acmeRes.AggID)
		globexVersion, globexName = queryFactVersion(t, ctx, db, "globex", "site", globexRes.AggID)
		if acmeVersion > 0 && globexVersion > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for analytics facts: acme version=%d globex version=%d", acmeVersion, globexVersion)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if acmeName != "Plant A" {
		t.Fatalf("acme fact payload name = %q; want %q", acmeName, "Plant A")
	}
	if globexName != "Plant G" {
		t.Fatalf("globex fact payload name = %q; want %q", globexName, "Plant G")
	}

	// The event table must also contain the create event for each tenant.
	assertEventExists(t, ctx, db, "acme", "site", acmeRes.AggID)
	assertEventExists(t, ctx, db, "globex", "site", globexRes.AggID)

	t.Logf("analytics consumer confirmed: acme site=%q (v%d), globex site=%q (v%d) co-located in the shared analytics DB",
		acmeName, acmeVersion, globexName, globexVersion)
}

// queryFactVersion reads the current fact version + the "name" field out of
// the redacted JSON payload (0, "" if the fact has not landed yet).
func queryFactVersion(t *testing.T, ctx context.Context, db *pgdriver.PgDB, tenantID, aggregate, aggID string) (int64, string) {
	t.Helper()
	rows, err := db.Query(ctx,
		`SELECT version, payload->>'name' FROM fabriq_analytics_facts
		 WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`,
		tenantID, aggregate, aggID)
	if err != nil {
		t.Fatalf("query fact: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, ""
	}
	var version int64
	var name string
	if err := rows.Scan(&version, &name); err != nil {
		t.Fatalf("scan fact: %v", err)
	}
	return version, name
}

// assertEventExists fails the test unless at least one analytics event row
// exists for the given aggregate instance.
func assertEventExists(t *testing.T, ctx context.Context, db *pgdriver.PgDB, tenantID, aggregate, aggID string) {
	t.Helper()
	rows, err := db.Query(ctx,
		`SELECT count(*) FROM fabriq_analytics_events WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`,
		tenantID, aggregate, aggID)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("no count row for %s/%s/%s", tenantID, aggregate, aggID)
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatalf("scan count: %v", err)
	}
	if n < 1 {
		t.Fatalf("expected at least one analytics event for %s/%s/%s, got %d", tenantID, aggregate, aggID, n)
	}
}

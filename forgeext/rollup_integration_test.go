//go:build integration

package forgeext_test

// TestRollupMaintainer_EndToEnd_PerTenantIsolation proves Task 6's
// leader-elected rollup:insights job (forgeext/rollup.go: runRollupMaintainer,
// wired into worker.go behind the same gate/lock-key pattern as the
// reconciler/blob-gc jobs) end to end against a real Postgres: tracked
// "checkout" events for two tenants get materialized into
// fabriq_insights_rollup_checkouts with the RIGHT per-tenant counts (not just
// "some rows exist") and no cross-tenant leakage, and the customer-facing
// f.Analytics().Query surface reads the materialized data back through the
// rollup-aware stitching router (Task 5).
//
// Unlike most sibling worker_*_integration_test.go files, this test points
// Config.Postgres.DSN directly at the SUPERUSER dsn rather than a
// fabriqtest.CreateAppRole-restricted role. Two of the maintainer's own
// operations are deliberately schema-owner/RLS-bypassing work, run over the
// plain connection pool (no per-tenant SET LOCAL app.tenant_id) rather than
// a tenant transaction:
//
//   - EnsureRollupTable — DDL (CREATE TABLE/ALTER), the same schema-owner
//     exec seam EnsureDynamic uses.
//   - TenantsForInsightsEvent — a bare SELECT DISTINCT tenant_id, which only
//     sees rows across every tenant if the connecting role can bypass RLS
//     (fabriq_insights_events has FORCE ROW LEVEL SECURITY: the policy's
//     `tenant_id = current_setting('app.tenant_id', true)` clause hides
//     every row from a non-bypassrls role with no tenant stamped).
//
// A CreateAppRole role deliberately has neither CREATE nor BYPASSRLS (it
// exists specifically to prove RLS holds even for the most restrictive
// role — see insights_rollup_integration_test.go/insights_integration_test.go
// for that coverage), so under it both operations above would silently see
// nothing — not a bug in the maintainer, but the wrong harness to observe
// its per-tenant tick/routing logic with. This test's job is proving THAT
// logic (leader-elected wiring, tenant enumeration, per-tenant routing,
// rollup-table data isolation via tenant_id column values) — a superuser
// DSN is the realistic shape for a deployment that grants its runtime role
// full schema ownership (a common, simpler posture; RLS is defense-in-depth
// layered ON TOP of correct tenant-scoping code, not a substitute for it).
//
// Reuses the same StartPostgres/StartRedis harness the sibling
// worker_analytics_integration_test.go and worker_compact_integration_test.go
// use.

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/migrations"
)

// rollupOwnerIntegModel is a placeholder aggregate used only to host this
// file's MetricSpec via EntitySpec.Metrics (Register requires exactly one of
// Model/Schema on every EntitySpec). Nothing in this test does CRUD against
// it and no migration ever creates its table — Open/Start never introspects
// a registered entity's physical table, so a model with no matching table is
// safe here (mirrors forgeext's own worker_insights_test.go imodel fixture).
type rollupOwnerIntegModel struct {
	grove.BaseModel `grove:"table:rollup_integ_owners"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
}

func TestRollupMaintainer_EndToEnd_PerTenantIsolation(t *testing.T) {
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatalf("open orchestrator: %v", err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = closeFn()

	// One materialized metric ("checkouts", sourcing the "checkout" event):
	// a 1-second bucket with a short seal grace so the maintainer's default
	// 1-minute polling isn't needed — WithRollupInterval below drives it fast
	// for the test.
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name:  "rollup_integ_owner",
		Kind:  registry.KindAggregate,
		Model: (*rollupOwnerIntegModel)(nil),
		Metrics: []registry.MetricSpec{
			{
				Name:     "checkouts",
				Source:   "checkout",
				Measures: []registry.MetricMeasure{{Kind: "count", As: "n"}},
				Rollup: &registry.RollupSpec{
					Bucket:       time.Second,
					SealGrace:    200 * time.Millisecond,
					RerollWindow: 2 * time.Second,
				},
			},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	// Config.Postgres.DSN is the superuser DSN directly (see the file-header
	// comment) — the worker's own boot-time EnsureRollupTable call handles
	// provisioning the rollup table; no separate owner adapter is needed.
	ext := forgeext.New(reg,
		forgeext.WithConfig(fabriq.Config{
			Postgres: fabriq.PostgresConfig{DSN: superDSN},
			Redis:    fabriq.RedisConfig{Addr: redisAddr},
			Insights: fabriq.InsightsConfig{Enabled: true},
		}),
		forgeext.WithWorker(true),
		// Production default is 1 minute; drive it fast so the test doesn't
		// wait that long for the first pass.
		forgeext.WithRollupInterval(300*time.Millisecond),
	)

	app := forge.NewApp(forge.AppConfig{
		Name:        "fabriq-rollup-worker-test",
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

	acmeCtx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("tenant.WithTenant(acme): %v", err)
	}
	globexCtx, err := tenant.WithTenant(ctx, "globex")
	if err != nil {
		t.Fatalf("tenant.WithTenant(globex): %v", err)
	}

	// Track events safely inside a bucket that will be fully sealed by the
	// time the maintainer's first tick runs: 3 checkouts for acme, 2 for
	// globex — different counts so a tenant mix-up in the maintainer's
	// per-tenant routing would be caught, not just an empty-vs-nonempty check.
	past := time.Now().Add(-10 * time.Second)
	trackN := func(tctx context.Context, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if err := f.Analytics().Track(tctx, []query.AnalyticsEvent{{Name: "checkout", At: past}}); err != nil {
				t.Fatalf("Track: %v", err)
			}
		}
	}
	trackN(acmeCtx, 3)
	trackN(globexCtx, 2)

	// Poll the physical rollup table directly as OWNER (bypassing RLS) until
	// the leader-elected rollup:insights maintainer has materialized both
	// tenants — proof the job actually ran, independent of the query router.
	db := pgdriver.New()
	if err := db.Open(ctx, superDSN); err != nil {
		t.Fatalf("open owner db: %v", err)
	}
	defer db.Close()

	countFor := func(tid string) (n int64, ok bool) {
		rows, qerr := db.Query(context.Background(),
			`SELECT n FROM fabriq_insights_rollup_checkouts WHERE tenant_id = $1`, tid)
		if qerr != nil {
			t.Fatalf("query rollup table for %s: %v", tid, qerr)
		}
		defer rows.Close()
		if !rows.Next() {
			return 0, false
		}
		if serr := rows.Scan(&n); serr != nil {
			t.Fatalf("scan rollup count for %s: %v", tid, serr)
		}
		return n, true
	}

	deadline := time.Now().Add(30 * time.Second)
	var acmeN, globexN int64
	var acmeOK, globexOK bool
	for {
		acmeN, acmeOK = countFor("acme")
		globexN, globexOK = countFor("globex")
		if acmeOK && globexOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for rollup maintainer: acme ok=%v n=%d, globex ok=%v n=%d",
				acmeOK, acmeN, globexOK, globexN)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Isolation: each tenant's rollup row carries ONLY that tenant's count —
	// no cross-tenant leakage.
	if acmeN != 3 {
		t.Fatalf("acme rollup count = %d, want 3", acmeN)
	}
	if globexN != 2 {
		t.Fatalf("globex rollup count = %d, want 2", globexN)
	}

	// Also confirm no OTHER tenant's rows exist in the table (exactly the
	// two rows this test produced).
	totalRows := func() int {
		rows, qerr := db.Query(context.Background(), `SELECT count(*) FROM fabriq_insights_rollup_checkouts`)
		if qerr != nil {
			t.Fatalf("count rollup rows: %v", qerr)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("count rollup rows: no row")
		}
		var n int
		if serr := rows.Scan(&n); serr != nil {
			t.Fatalf("scan rollup row count: %v", serr)
		}
		return n
	}
	if got := totalRows(); got != 2 {
		t.Fatalf("fabriq_insights_rollup_checkouts has %d rows, want exactly 2 (one per tenant)", got)
	}

	// The customer-facing surface (f.Analytics().Query, Source naming the
	// declared metric) must read the materialized rollup back through the
	// stitching router (Task 5), RLS-scoped to the querying tenant only.
	var acmeRows []map[string]any
	if err := f.Analytics().Query(acmeCtx, query.AnalyticsQuery{Source: "checkouts"}, &acmeRows); err != nil {
		t.Fatalf("Query(checkouts, acme): %v", err)
	}
	if len(acmeRows) != 1 {
		t.Fatalf("acme Query(checkouts) rows = %d, want 1: %+v", len(acmeRows), acmeRows)
	}
	if n := rollupQueryToInt(t, acmeRows[0]["n"]); n != 3 {
		t.Fatalf("acme Query(checkouts)[0][\"n\"] = %v (%d), want 3 (row: %+v)", acmeRows[0]["n"], n, acmeRows[0])
	}
}

// rollupQueryToInt tolerantly converts a scanned dynamic-row value (the
// driver may return int, int32, int64, or a numeric string depending on
// column type) to an int64 for comparison.
func rollupQueryToInt(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64)
		if err != nil {
			t.Fatalf("rollupQueryToInt: parse %q: %v", n, err)
		}
		return i
	case []byte:
		i, err := strconv.ParseInt(strings.TrimSpace(string(n)), 10, 64)
		if err != nil {
			t.Fatalf("rollupQueryToInt: parse %q: %v", n, err)
		}
		return i
	default:
		t.Fatalf("rollupQueryToInt: unsupported type %T (%v)", v, v)
		return 0
	}
}

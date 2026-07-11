//go:build integration

package postgres_test

// TestInsights_TrackDedup proves the postgres adapter's Track method (the
// outbox-bypass customer-analytics ingest into fabriq_insights_events) against
// a real Postgres: two events sharing a DedupKey under one tenant collapse to
// exactly one row, and an event with a distinct name/no dedup key inserts
// normally alongside it.

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// newInsightsHarness boots one Postgres container, runs fabriq migrations to
// head (which creates fabriq_insights_events with its RLS policy — migration
// 0031), then opens the adapter under test as the restricted app role so RLS
// actually constrains its writes. It also returns the superuser owner
// adapter, which bypasses RLS, so the test can verify raw row counts without
// needing a tenant-stamped read path (Query/QueryRaw don't exist yet — this
// task only implements Track).
func newInsightsHarness(t *testing.T) (a, owner *postgres.Adapter) {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	owner, err := postgres.Open(ctx, superDSN, reg)
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

	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err = postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app role): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a, owner
}

func TestInsights_TrackDedup(t *testing.T) {
	a, owner := newInsightsHarness(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	dup := query.AnalyticsEvent{
		Name:     "signup",
		At:       now,
		Props:    map[string]any{"plan": "pro"},
		DedupKey: "evt-123",
	}
	dupAgain := query.AnalyticsEvent{
		Name:     "signup",
		At:       now.Add(time.Second),
		Props:    map[string]any{"plan": "pro", "retried": true},
		DedupKey: "evt-123",
	}
	distinct := query.AnalyticsEvent{
		Name:  "page_view",
		At:    now.Add(2 * time.Second),
		Props: map[string]any{"path": "/pricing"},
		// no DedupKey — normal insert, never conflicts.
	}

	if err := a.Track(ctx, []query.AnalyticsEvent{dup, dupAgain, distinct}); err != nil {
		t.Fatalf("Track: %v", err)
	}

	dedupCount := countInsightsEvents(t, owner, "acme", "evt-123")
	if dedupCount != 1 {
		t.Fatalf("expected exactly 1 row for dedup_key=evt-123, got %d", dedupCount)
	}

	total := countAllInsightsEvents(t, owner, "acme")
	if total != 2 {
		t.Fatalf("expected 2 total rows (1 deduped signup + 1 page_view), got %d", total)
	}
}

func countInsightsEvents(t *testing.T, a *postgres.Adapter, tenantID, dedupKey string) int {
	t.Helper()
	var n int
	row := a.Driver().QueryRow(context.Background(),
		`SELECT count(*) FROM fabriq_insights_events WHERE tenant_id = $1 AND dedup_key = $2`,
		tenantID, dedupKey)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count dedup rows: %v", err)
	}
	return n
}

func countAllInsightsEvents(t *testing.T, a *postgres.Adapter, tenantID string) int {
	t.Helper()
	var n int
	row := a.Driver().QueryRow(context.Background(),
		`SELECT count(*) FROM fabriq_insights_events WHERE tenant_id = $1`, tenantID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count all rows: %v", err)
	}
	return n
}

// TestPgInsights_Conformance gates the real Postgres adapter against the
// SAME behavioral suite (insights.RunConformance) that already passes
// against fabriqtest.NewFakeAnalytics — the drift gate for Track/Query
// semantics. It reuses newInsightsHarness (the exact opener + migrate-to-head
// setup TestInsights_TrackDedup uses above), which also runs domain.DemoDDL
// as a side effect — the only thing in this suite's setup path that issues
// `CREATE EXTENSION IF NOT EXISTS timescaledb` (see domain/demo.go), which
// TimeBucketGroups' use of time_bucket() depends on.
//
// One adapter instance is opened once and reused across every RunConformance
// sub-test: query.AnalyticsQuerier is stateless per call (tenant travels on
// ctx, stamped fresh by inTenantTx/inDynamicTenantTx per call), so unlike a
// pooled resource that needs a fresh handle per sub-test, the SAME
// *postgres.InsightsAdapter can simply be returned every time the factory is
// invoked. Isolation between sub-tests instead comes from truncating the
// insights tables before each factory call, mirroring the noCloseSink +
// truncating-factory idiom in
// adapters/pganalytics/conformance_integration_test.go.
func TestPgInsights_Conformance(t *testing.T) {
	a, owner := newInsightsHarness(t)
	ctx := context.Background()
	ia := postgres.NewInsightsAdapter(a)

	insights.RunConformance(t, func() query.AnalyticsQuerier {
		truncateInsights(t, ctx, owner)
		return ia
	})
}

// truncateInsights empties both insights tables via the superuser/owner
// connection. The app role (what `a` in newInsightsHarness connects as) is
// only granted SELECT/INSERT/UPDATE/DELETE — not TRUNCATE — so this must run
// as owner, which also bypasses RLS and can see rows from every tenant the
// previous sub-test wrote.
func truncateInsights(t *testing.T, ctx context.Context, owner *postgres.Adapter) {
	t.Helper()
	if _, err := owner.Driver().Exec(ctx, `TRUNCATE fabriq_insights_events, fabriq_insights_facts`); err != nil {
		t.Fatalf("truncate insights tables: %v", err)
	}
}

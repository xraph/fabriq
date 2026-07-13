//go:build integration

package postgres_test

// TestInsights_TrackDedup proves the postgres adapter's Track method (the
// outbox-bypass customer-analytics ingest into fabriq_insights_events) against
// a real Postgres: two events sharing a DedupKey under one tenant collapse to
// exactly one row, and an event with a distinct name/no dedup key inserts
// normally alongside it.

import (
	"context"
	"encoding/json"
	"reflect"
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

// newInsightsHarness boots one Postgres container, registers the example
// domain pack into reg, runs fabriq migrations to head (which creates
// fabriq_insights_events/fabriq_insights_facts with their RLS policies —
// migration 0031), then opens the adapter under test as the restricted app
// role so RLS actually constrains its writes. It also returns the superuser
// owner adapter, which bypasses RLS, so the test can verify raw row counts
// without needing a tenant-stamped read path.
//
// reg is supplied by the CALLER rather than built internally, so a caller
// can register additional entities into it — before OR after calling this
// function — and have both returned adapters observe them: postgres.Open
// stores the *registry.Registry pointer as-is (adapters/postgres/adapter.go's
// newAdapter does `reg: reg`, no snapshot/copy), and every per-call resolver
// (insights.ResolveSource inside InsightsAdapter.Query, entity lookups inside
// Get/List/...) reads that same live registry fresh on each call. So a
// registry.Register call on the identical object, issued after Open has
// already returned, is visible to the very next Query. TestPgInsights_Conformance
// below exploits this to bind the adapter under test to RunConformance's OWN
// suite registry — the one insights.RunConformance (core/insights/conformance.go)
// registers its "order" InsightsSpec entity into for the facts subtests —
// instead of a disjoint registry the adapter could never see that entity in.
func newInsightsHarness(t *testing.T, reg *registry.Registry) (a, owner *postgres.Adapter) {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

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
	a, owner := newInsightsHarness(t, registry.New())
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
// against fabriqtest.NewFakeAnalytics — the drift gate for Track/Query/
// UpsertInsightFacts semantics. It reuses newInsightsHarness (the exact
// opener + migrate-to-head setup TestInsights_TrackDedup uses above), which
// also runs domain.DemoDDL as a side effect — the only thing in this suite's
// setup path that issues `CREATE EXTENSION IF NOT EXISTS timescaledb` (see
// domain/demo.go), which TimeBucketGroups' use of time_bucket() depends on.
//
// The harness is built LAZILY, on the factory's FIRST invocation, using
// RunConformance's OWN suite registry — the reg parameter the factory
// receives — rather than a registry built ahead of time. This is the fix for
// the facts subtests (Task 6): insights.RunConformance registers an "order"
// InsightsSpec entity into reg partway through the suite (right before its
// facts subtests), and because newInsightsHarness stashes that IDENTICAL reg
// pointer on the *postgres.Adapter it opens (see newInsightsHarness's doc
// comment — postgres.Open never snapshots the registry), the very next
// InsightsAdapter.Query call after that registration sees the new entity and
// resolves Source: "order" as SourceFacts, exactly like the fake. Previously
// `ia` was bound to a private, disjoint registry built via domain.RegisterAll
// alone — the suite's facts entity never reached it, so Source: "order" in
// the facts subtests would have resolved as a (data-less) event source
// against the real adapter.
//
// One adapter instance is still opened once (guarded by ia == nil) and reused
// across every RunConformance sub-test after that: query.AnalyticsQuerier is
// stateless per call (tenant travels on ctx, stamped fresh by
// inTenantTx/inDynamicTenantTx per call), so unlike a pooled resource that
// needs a fresh handle per sub-test, the SAME *postgres.InsightsAdapter can
// simply be returned every time the factory is invoked thereafter. Isolation
// between sub-tests instead comes from truncating the insights tables before
// each factory call, mirroring the noCloseSink + truncating-factory idiom in
// adapters/pganalytics/conformance_integration_test.go.
func TestPgInsights_Conformance(t *testing.T) {
	ctx := context.Background()
	var owner *postgres.Adapter
	var ia *postgres.InsightsAdapter

	insights.RunConformance(t, func(reg *registry.Registry) query.AnalyticsQuerier {
		if ia == nil {
			var a *postgres.Adapter
			a, owner = newInsightsHarness(t, reg)
			ia = postgres.NewInsightsAdapter(a)
		}
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

// TestInsights_UpsertFactsVersionGate proves (*postgres.InsightsAdapter).
// UpsertInsightFacts — the proj:insights write path — against a real
// Postgres: the version gate (an older write is a silent no-op, a newer
// write replaces the row), the NULLIF($2,”) scope handling (unscoped writes
// store scope_id NULL, scoped writes store the literal scope id), and tenant
// isolation (RLS contains each tenant's writes to its own rows), all read
// back via the RLS-bypassing owner adapter exactly as TestInsights_TrackDedup
// reads back Track's rows.
func TestInsights_UpsertFactsVersionGate(t *testing.T) {
	a, owner := newInsightsHarness(t, registry.New())
	ia := postgres.NewInsightsAdapter(a)

	acmeCtx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()

	// 1. Version gate: v2 then an older v1 for the same (tenant, entity,
	// agg_id) — the older write must be a no-op, leaving v2 in place.
	v2Payload := json.RawMessage(`{"total":200}`)
	if err := ia.UpsertInsightFacts(acmeCtx, []insights.Fact{{
		TenantID: "acme", Entity: "order", AggID: "o1",
		Version: 2, Payload: v2Payload, At: now,
	}}); err != nil {
		t.Fatalf("upsert v2: %v", err)
	}
	v1Payload := json.RawMessage(`{"total":100}`)
	if err := ia.UpsertInsightFacts(acmeCtx, []insights.Fact{{
		TenantID: "acme", Entity: "order", AggID: "o1",
		Version: 1, Payload: v1Payload, At: now,
	}}); err != nil {
		t.Fatalf("upsert older v1 (should be a silent no-op, not an error): %v", err)
	}
	gotVersion, gotPayload, _ := readInsightFact(t, owner, "acme", "order", "o1")
	if gotVersion != 2 {
		t.Fatalf("version gate: want version 2 to survive an older v1 write, got %d", gotVersion)
	}
	if !jsonEqual(t, gotPayload, v2Payload) {
		t.Fatalf("version gate: want v2 payload to survive, got %s", gotPayload)
	}

	// 2. Newer version: v3 must replace both version and payload.
	v3Payload := json.RawMessage(`{"total":300}`)
	if err := ia.UpsertInsightFacts(acmeCtx, []insights.Fact{{
		TenantID: "acme", Entity: "order", AggID: "o1",
		Version: 3, Payload: v3Payload, At: now,
	}}); err != nil {
		t.Fatalf("upsert v3: %v", err)
	}
	gotVersion, gotPayload, _ = readInsightFact(t, owner, "acme", "order", "o1")
	if gotVersion != 3 {
		t.Fatalf("want version 3 after a newer write, got %d", gotVersion)
	}
	if !jsonEqual(t, gotPayload, v3Payload) {
		t.Fatalf("want v3 payload after a newer write, got %s", gotPayload)
	}

	// 3. Scope handling: a fact written under a scoped ctx lands with
	// scope_id = 's1'; a fact written unscoped (acmeCtx above) lands with
	// scope_id IS NULL.
	scopedCtx, err := tenant.WithScope(acmeCtx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if err := ia.UpsertInsightFacts(scopedCtx, []insights.Fact{{
		TenantID: "acme", Entity: "invoice", AggID: "i1",
		Version: 1, Payload: json.RawMessage(`{}`), At: now,
	}}); err != nil {
		t.Fatalf("upsert scoped fact: %v", err)
	}
	_, _, gotScope := readInsightFact(t, owner, "acme", "invoice", "i1")
	if gotScope != "s1" {
		t.Fatalf("want scope_id 's1' for a fact written under a scoped ctx, got %q", gotScope)
	}
	_, _, unscoped := readInsightFact(t, owner, "acme", "order", "o1")
	if unscoped != "" {
		t.Fatalf("want scope_id NULL for a fact written under an unscoped ctx, got %q", unscoped)
	}

	// 4. Tenant isolation: a fact written under tenant t1 must not appear
	// under tenant t2's rows, even for the same (entity, agg_id) — RLS
	// contains the write's visibility to its own tenant.
	t1Ctx, err := tenant.WithTenant(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	t2Ctx, err := tenant.WithTenant(context.Background(), "t2")
	if err != nil {
		t.Fatal(err)
	}
	if err := ia.UpsertInsightFacts(t1Ctx, []insights.Fact{{
		TenantID: "t1", Entity: "order", AggID: "shared",
		Version: 1, Payload: json.RawMessage(`{"who":"t1"}`), At: now,
	}}); err != nil {
		t.Fatalf("upsert under t1: %v", err)
	}
	if err := ia.UpsertInsightFacts(t2Ctx, []insights.Fact{{
		TenantID: "t2", Entity: "order", AggID: "shared",
		Version: 1, Payload: json.RawMessage(`{"who":"t2"}`), At: now,
	}}); err != nil {
		t.Fatalf("upsert under t2: %v", err)
	}
	t1Version, t1Payload, _ := readInsightFact(t, owner, "t1", "order", "shared")
	if t1Version != 1 || !jsonEqual(t, t1Payload, json.RawMessage(`{"who":"t1"}`)) {
		t.Fatalf("t1 row missing or wrong: version=%d payload=%s", t1Version, t1Payload)
	}
	t2Version, t2Payload, _ := readInsightFact(t, owner, "t2", "order", "shared")
	if t2Version != 1 || !jsonEqual(t, t2Payload, json.RawMessage(`{"who":"t2"}`)) {
		t.Fatalf("t2 row missing or wrong: version=%d payload=%s", t2Version, t2Payload)
	}
	if countInsightsFactsForTenant(t, owner, "t1") != 1 {
		t.Fatalf("want exactly 1 fact row visible under tenant_id='t1'")
	}
}

// jsonEqual compares two JSON documents by decoded value rather than by raw
// bytes: Postgres's jsonb re-serializes payloads (e.g. inserting a space
// after ':'), so a byte-exact comparison against the literal we wrote would
// spuriously fail.
func jsonEqual(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("unmarshal %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	return reflect.DeepEqual(av, bv)
}

// readInsightFact reads back one fabriq_insights_facts row via the
// RLS-bypassing owner connection, mirroring countInsightsEvents/
// countAllInsightsEvents above.
func readInsightFact(t *testing.T, owner *postgres.Adapter, tenantID, entity, aggID string) (version int64, payload json.RawMessage, scopeID string) {
	t.Helper()
	row := owner.Driver().QueryRow(context.Background(),
		`SELECT version, payload, coalesce(scope_id, '') FROM fabriq_insights_facts
		 WHERE tenant_id = $1 AND entity = $2 AND agg_id = $3`,
		tenantID, entity, aggID)
	if err := row.Scan(&version, &payload, &scopeID); err != nil {
		t.Fatalf("read back fact (tenant=%s entity=%s agg_id=%s): %v", tenantID, entity, aggID, err)
	}
	return version, payload, scopeID
}

// countInsightsFactsForTenant counts fabriq_insights_facts rows visible
// under tenant_id, via the RLS-bypassing owner connection.
func countInsightsFactsForTenant(t *testing.T, owner *postgres.Adapter, tenantID string) int {
	t.Helper()
	var n int
	row := owner.Driver().QueryRow(context.Background(),
		`SELECT count(*) FROM fabriq_insights_facts WHERE tenant_id = $1`, tenantID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count facts for tenant %s: %v", tenantID, err)
	}
	return n
}

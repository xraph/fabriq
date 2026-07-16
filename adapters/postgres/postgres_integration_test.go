//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// harness boots one container + adapter per test function.
type harness struct {
	A   *postgres.Adapter
	X   *command.Executor
	Reg *registry.Registry
}

func newHarness(t testing.TB) *harness {
	t.Helper()
	superDSN := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	// Migrations run as the schema owner (superuser)...
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = owner.Close()

	// ...the adapter under test connects as the restricted app role, so
	// RLS actually constrains it (it never constrains superusers).
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg, postgres.WithGuardedTables(domain.ReadingsSeries))
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	x, err := command.NewExecutor(reg, a)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{A: a, X: x, Reg: reg}
}

func tctx(t testing.TB, id string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

type outboxRow struct {
	ID        string
	TenantID  string
	Aggregate string
	AggID     string
	Version   int64
	Type      string
	Published bool
}

func (h *harness) outboxRows(t testing.TB) []outboxRow {
	t.Helper()
	rows, err := h.A.Driver().Query(context.Background(),
		`SELECT id, tenant_id, aggregate, agg_id, version, type, published_at IS NOT NULL
		 FROM fabriq_outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.ID, &r.TenantID, &r.Aggregate, &r.AggID, &r.Version, &r.Type, &r.Published); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPG_CreateUpdateDelete_EmitsExactlyOneEventEach(t *testing.T) {
	h := newHarness(t)
	ctx := tctx(t, "acme")

	created, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "Plant A", Code: "PA"}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpUpdate, AggID: created.AggID, Payload: &domain.Site{Name: "Plant A2", Code: "PA"}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpDelete, AggID: created.AggID}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	rows := h.outboxRows(t)
	if len(rows) != 3 {
		t.Fatalf("outbox rows = %d, want 3", len(rows))
	}
	wantTypes := []string{"site.created", "site.updated", "site.deleted"}
	for i, r := range rows {
		if r.Type != wantTypes[i] || r.Version != int64(i+1) || r.TenantID != "acme" || r.AggID != created.AggID {
			t.Fatalf("outbox[%d] = %+v", i, r)
		}
		if r.Published {
			t.Fatalf("outbox[%d] already published", i)
		}
	}

	// Row is gone.
	var ghost domain.Site
	if err := h.A.Get(ctx, "site", created.AggID, &ghost); !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("after delete want ErrNotFound, got %v", err)
	}
}

func TestPG_OptimisticConcurrency(t *testing.T) {
	h := newHarness(t)
	ctx := tctx(t, "acme")

	created, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "P"}})
	if err != nil {
		t.Fatal(err)
	}
	stale := int64(9)
	_, err = h.X.Exec(ctx, command.Command{
		Entity: "site", Op: command.OpUpdate, AggID: created.AggID,
		Payload: &domain.Site{Name: "Q"}, ExpectedVersion: &stale,
	})
	if !errors.Is(err, fabriqerr.ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}
	if rows := h.outboxRows(t); len(rows) != 1 {
		t.Fatalf("conflicted command leaked an event: %d rows", len(rows))
	}
}

func TestPG_BatchAtomicity(t *testing.T) {
	h := newHarness(t)
	ctx := tctx(t, "acme")

	// Second command fails inside the tx (update of a missing aggregate):
	// the first create must roll back with it.
	_, err := h.X.ExecBatch(ctx, []command.Command{
		{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "A"}},
		{Entity: "site", Op: command.OpUpdate, AggID: "01HNOPE000000000000000000X", Payload: &domain.Site{Name: "B"}},
	})
	if !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("want ErrNotFound from second command, got %v", err)
	}
	if rows := h.outboxRows(t); len(rows) != 0 {
		t.Fatalf("failed batch leaked %d outbox rows", len(rows))
	}
	var count int
	if err := h.A.Driver().QueryRow(context.Background(), `SELECT count(*) FROM sites`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed batch leaked %d site rows", count)
	}
}

func TestPG_RLSTenantIsolation(t *testing.T) {
	h := newHarness(t)
	acme := tctx(t, "acme")
	rival := tctx(t, "rival")

	created, err := h.X.Exec(acme, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "Secret"}})
	if err != nil {
		t.Fatal(err)
	}

	var leak domain.Site
	if err := h.A.Get(rival, "site", created.AggID, &leak); !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("cross-tenant Get: want ErrNotFound, got %v (leak=%+v)", err, leak)
	}

	var list []*domain.Site
	if err := h.A.List(rival, "site", query.ListQuery{}, &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("cross-tenant List leaked %d rows", len(list))
	}

	// Even the raw escape hatch is contained: the tx is stamped with the
	// rival tenant, so RLS hides acme's rows from arbitrary SQL.
	var raw []*domain.Site
	if err := h.A.Query(rival, &raw, `SELECT * FROM sites`); err != nil {
		t.Fatal(err)
	}
	if len(raw) != 0 {
		t.Fatalf("raw SQL leaked %d rows across tenants", len(raw))
	}

	// Same tenant sees its row.
	var mine domain.Site
	if err := h.A.Get(acme, "site", created.AggID, &mine); err != nil || mine.Name != "Secret" {
		t.Fatalf("same-tenant Get = (%+v, %v)", mine, err)
	}
}

func TestPG_TenantBackstopTripsOnPoolPathAccess(t *testing.T) {
	h := newHarness(t)

	// Pool-path grove access to a tenant table is always a bug in fabriq's
	// architecture (tenant tables are reachable only through stamped
	// transactions). The grove hook backstop must deny it loudly.
	pg := h.A.Driver()
	var sites []*domain.Site
	err := pg.NewSelect(&sites).Scan(context.Background())
	if err == nil {
		t.Fatal("pool-path select on a tenant table must be denied by the backstop")
	}
	if !errors.Is(err, tenant.ErrTenantHookTripped) {
		t.Fatalf("want ErrTenantHookTripped, got %v", err)
	}
	if h.A.BackstopTrips() != 1 {
		t.Fatalf("trip counter = %d, want 1", h.A.BackstopTrips())
	}
}

func TestPG_GetManyBatchedAndOrdered(t *testing.T) {
	h := newHarness(t)
	ctx := tctx(t, "acme")

	ids := make([]string, 3)
	for i, name := range []string{"A", "B", "C"} {
		res, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: name}})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = res.AggID
	}
	var got []*domain.Site
	if err := h.A.GetMany(ctx, "site", []string{ids[2], ids[0], "missing"}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != ids[2] || got[1].ID != ids[0] {
		t.Fatalf("GetMany order/skip wrong: %+v", got)
	}
}

func TestPG_ListFilterLimitOrder(t *testing.T) {
	h := newHarness(t)
	ctx := tctx(t, "acme")

	site, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "S"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"P3", "P1", "P2"} {
		if _, err := h.X.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
			Payload: &domain.Asset{Name: name, SiteID: site.AggID}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.X.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "other"}}); err != nil {
		t.Fatal(err)
	}

	var got []*domain.Asset
	err = h.A.List(ctx, "asset", query.ListQuery{
		Where: query.Eqs(map[string]any{"site_id": site.AggID}), OrderBy: "name", Limit: 2,
	}, &got)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "P1" || got[1].Name != "P2" {
		t.Fatalf("List = %+v", got)
	}

	// Filter columns are validated against the binding (injection guard).
	err = h.A.List(ctx, "asset", query.ListQuery{Where: []query.Cond{query.Eq("name; DROP TABLE sites--", "x")}}, &got)
	if err == nil {
		t.Fatal("unknown filter column must be rejected")
	}
}

// TestPG_ListRichFilter exercises the structured operator filter against
// real Postgres: LIKE/ILIKE, IN/NOT IN, comparisons, OR groups, and
// Filter+Where combined — all engine-neutral, all column-validated.
func TestPG_ListRichFilter(t *testing.T) {
	h := newHarness(t)
	ctx := tctx(t, "acme")

	site, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "S"}})
	if err != nil {
		t.Fatal(err)
	}
	mk := func(name, kind, siteID string) string {
		res, err := h.X.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
			Payload: &domain.Asset{Name: name, Kind: kind, SiteID: siteID}})
		if err != nil {
			t.Fatal(err)
		}
		return res.AggID
	}
	pumpA := mk("Main Pump", "pump", site.AggID)
	mk("Backup Pump", "pump", site.AggID)
	mk("Inlet Valve", "valve", site.AggID)
	mk("Spare Motor", "motor", "")

	names := func(q query.ListQuery) []string {
		var got []*domain.Asset
		if err := h.A.List(ctx, "asset", q, &got); err != nil {
			t.Fatalf("List: %v", err)
		}
		out := make([]string, len(got))
		for i, a := range got {
			out[i] = a.Name
		}
		return out
	}

	if n := names(query.ListQuery{Where: []query.Cond{query.Like("name", "%Pump")}, OrderBy: "name"}); len(n) != 2 || n[0] != "Backup Pump" || n[1] != "Main Pump" {
		t.Fatalf("LIKE = %v", n)
	}
	if n := names(query.ListQuery{Where: []query.Cond{query.ILike("name", "%pump")}}); len(n) != 2 {
		t.Fatalf("ILIKE = %v", n)
	}
	if n := names(query.ListQuery{Where: []query.Cond{query.In("kind", []string{"valve", "motor"})}, OrderBy: "name"}); len(n) != 2 || n[0] != "Inlet Valve" || n[1] != "Spare Motor" {
		t.Fatalf("IN = %v", n)
	}
	if n := names(query.ListQuery{Where: []query.Cond{query.NotIn("kind", []string{"pump"})}, OrderBy: "name"}); len(n) != 2 || n[0] != "Inlet Valve" {
		t.Fatalf("NOT IN = %v", n)
	}
	// OR group + equality, combined in one Where.
	if n := names(query.ListQuery{
		Where: append(query.Eqs(map[string]any{"site_id": site.AggID}),
			query.Or(query.Eq("kind", "valve"), query.Like("name", "Main%"))),
		OrderBy: "name",
	}); len(n) != 2 || n[0] != "Inlet Valve" || n[1] != "Main Pump" {
		t.Fatalf("OR + equality = %v", n)
	}

	// Comparison: bump one asset's version and select version > 1.
	for i := 0; i < 2; i++ {
		if _, err := h.X.Exec(ctx, command.Command{Entity: "asset", Op: command.OpUpdate, AggID: pumpA,
			Payload: &domain.Asset{Name: "Main Pump", Kind: "pump", SiteID: site.AggID}}); err != nil {
			t.Fatal(err)
		}
	}
	if n := names(query.ListQuery{Where: []query.Cond{query.Gt("version", 1)}}); len(n) != 1 || n[0] != "Main Pump" {
		t.Fatalf("version > 1 = %v", n)
	}

	// Validation still guards structured filters.
	var sink []*domain.Asset
	if err := h.A.List(ctx, "asset", query.ListQuery{Where: []query.Cond{query.Eq("nope", "x")}}, &sink); err == nil {
		t.Fatal("unknown structured-filter column must be rejected")
	}
}

func TestPG_TimescaleBulkWriteAndRange(t *testing.T) {
	h := newHarness(t)
	ctx := tctx(t, "acme")
	base := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)

	points := make([]query.Point, 100)
	for i := range points {
		points[i] = query.Point{Key: "tag-1", At: base.Add(time.Duration(i) * time.Second), Value: float64(i), Quality: 1}
	}
	if err := h.A.BulkWrite(ctx, domain.ReadingsSeries, points); err != nil {
		t.Fatalf("BulkWrite: %v", err)
	}

	var got []query.Point
	if err := h.A.Range(ctx, query.RangeQuery{
		Series: domain.ReadingsSeries, Key: "tag-1",
		From: base.Add(10 * time.Second), To: base.Add(20 * time.Second),
	}, &got); err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(got) != 10 {
		t.Fatalf("Range = %d points, want 10", len(got))
	}
	if got[0].Value != 10 || got[9].Value != 19 {
		t.Fatalf("Range window wrong: first=%v last=%v", got[0], got[9])
	}

	// Tenant isolation is structural here (no RLS on the hypertable —
	// columnstore is incompatible): the adapter must still scope.
	var rivalPoints []query.Point
	if err := h.A.Range(tctx(t, "rival"), query.RangeQuery{
		Series: domain.ReadingsSeries, Key: "tag-1", From: base, To: base.Add(time.Hour),
	}, &rivalPoints); err != nil {
		t.Fatal(err)
	}
	if len(rivalPoints) != 0 {
		t.Fatalf("readings leaked across tenants: %d", len(rivalPoints))
	}

	// Raw SQL touching the unprotected readings table without a tenant
	// predicate must be rejected by the adapter's guard.
	var leaked []*domain.Site
	if err := h.A.Query(ctx, &leaked, `SELECT * FROM tag_readings`); err == nil {
		t.Fatal("raw SQL on guarded table without tenant_id must be rejected")
	}
}

func TestPG_VectorUpsertAndSimilar(t *testing.T) {
	h := newHarness(t)
	ctx := tctx(t, "acme")

	emb := func(x, y float32) []float32 {
		v := make([]float32, 768)
		v[0], v[1] = x, y
		return v
	}
	if err := h.A.Upsert(ctx, "asset", "A1", emb(1, 0), map[string]any{"name": "pump"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := h.A.Upsert(ctx, "asset", "A2", emb(0, 1), nil); err != nil {
		t.Fatal(err)
	}
	if err := h.A.Upsert(ctx, "asset", "A3", emb(0.9, 0.1), nil); err != nil {
		t.Fatal(err)
	}

	var got []query.VectorMatch
	if err := h.A.Similar(ctx, query.VectorQuery{Entity: "asset", Embedding: emb(1, 0), K: 2}, &got); err != nil {
		t.Fatalf("Similar: %v", err)
	}
	if len(got) != 2 || got[0].ID != "A1" || got[1].ID != "A3" {
		t.Fatalf("Similar = %+v", got)
	}

	var rival []query.VectorMatch
	if err := h.A.Similar(tctx(t, "rival"), query.VectorQuery{Entity: "asset", Embedding: emb(1, 0), K: 5}, &rival); err != nil {
		t.Fatal(err)
	}
	if len(rival) != 0 {
		t.Fatalf("vector leak across tenants: %+v", rival)
	}
}

func TestPG_ProjectionStateRepo(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	repo := h.A.ProjectionState()

	v, err := repo.AppliedVersion(ctx, "acme", "graph", "asset", "A1")
	if err != nil || v != 0 {
		t.Fatalf("empty AppliedVersion = (%d, %v), want (0, nil)", v, err)
	}
	if err := repo.SetApplied(ctx, "acme", "graph", "asset", "A1", 4); err != nil {
		t.Fatal(err)
	}
	// Lower versions never regress the watermark.
	if err := repo.SetApplied(ctx, "acme", "graph", "asset", "A1", 2); err != nil {
		t.Fatal(err)
	}
	v, err = repo.AppliedVersion(ctx, "acme", "graph", "asset", "A1")
	if err != nil || v != 4 {
		t.Fatalf("AppliedVersion = (%d, %v), want (4, nil)", v, err)
	}
}

func BenchmarkPG_ExecCreate(b *testing.B) {
	h := newHarness(b)
	ctx := tctx(b, "acme")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := h.X.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate,
			Payload: &domain.Site{Name: fmt.Sprintf("S%d", i)}}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPG_BulkWrite1k(b *testing.B) {
	h := newHarness(b)
	ctx := tctx(b, "acme")
	base := time.Now().UTC()
	points := make([]query.Point, 1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := range points {
			points[j] = query.Point{Key: "tag-bench", At: base.Add(time.Duration(i*1000+j) * time.Millisecond), Value: float64(j)}
		}
		if err := h.A.BulkWrite(ctx, domain.ReadingsSeries, points); err != nil {
			b.Fatal(err)
		}
	}
}

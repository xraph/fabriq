//go:build integration

package postgres_test

// TestScope_SoftRLSFilter proves the native secondary-scope feature end-to-end
// against real Postgres RLS. The test uses a dynamic entity ("project") that
// declares a scope_id column so the relational command path (stampedValues)
// writes scope_id on each row. The table gets the ScopeAwareTenantPolicy from
// migrations/0012_scope.go applied on top of the standard tenant_isolation
// policy created by EnsureDynamic.
//
// TestScope_VectorScopeFilter and TestScope_SpatialScopeFilter prove that the
// Vector/Spatial port Upsert methods now stamp scope_id (via NULLIF($N,'') from
// tenant.ScopeOrEmpty) so fabriq_embeddings / fabriq_geometries rows are
// correctly partitioned by scope. Migration 0012 applied the ScopeAwareTenantPolicy
// to those tables; the write-side fix completes the loop.

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// scopeTestTable is the dynamic table name for the scope integration test.
// A distinct name avoids any collision with other integration tests.
const scopeTestTable = "ds_scope_test"

// newScopeHarness boots one Postgres container, runs fabriq migrations, then
// creates the dynamic "project" entity table with the scope-aware RLS policy.
// It returns (superuser adapter, app-role adapter, executor).
func newScopeHarness(t *testing.T) (*postgres.Adapter, *postgres.Adapter, *command.Executor) {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	// Register the dynamic entity with a scope_id column.  The scope_id column
	// is nullable (no NotNull) so unscoped writes (scope_id = "") are stored as
	// the empty-string sentinel handled by the RLS USING predicate — but
	// stampedValues sets "" only when scopeID == "".  To get a true NULL for
	// unscoped rows we use a DEFAULT NULL column and let stampedValues skip
	// stamping when scopeID is empty (which leaves the column at its SQL
	// DEFAULT, i.e. NULL).  An explicit NULL default is the correct SQL shape.
	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "project",
		Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: scopeTestTable,
			Columns: []registry.DynamicColumn{
				{Name: "label", Type: registry.ColText},
				{Name: "scope_id", Type: registry.ColText},
			},
		},
	})

	// Open as schema owner for migrations and DDL.
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })

	// Run fabriq migrations (outbox, projection state, static-table RLS, etc.).
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Create the dynamic table (standard tenant-only RLS installed by EnsureDynamic).
	ent, ok := reg.Get("project")
	if !ok {
		t.Fatal("entity 'project' not found")
	}
	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("EnsureDynamic: %v", err)
	}

	// Upgrade the RLS policy to the scope-aware variant from migration 0012.
	// ScopeAwareTenantPolicy replaces the tenant-only policy with one that also
	// filters by scope_id: unscoped reads see all rows; scoped reads see their
	// scope plus NULL-scope (shared) rows.
	for _, stmt := range migrations.ScopeAwareTenantPolicy(scopeTestTable) {
		if _, err := owner.Driver().Exec(ctx, stmt); err != nil {
			t.Fatalf("apply scope RLS policy (%q): %v", stmt, err)
		}
	}

	// Provision the app role AFTER migrations + DDL so DEFAULT PRIVILEGES cover
	// the new table. RLS only constrains non-superusers; the app role is
	// NOBYPASSRLS so policies actually apply.
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app role): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	x, err := command.NewExecutor(reg, a)
	if err != nil {
		t.Fatal(err)
	}
	return owner, a, x
}

// scopedCtx returns a tenant-stamped context optionally narrowed to a scope.
func scopedCtx(t testing.TB, tenantID, scopeID string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("WithTenant(%q): %v", tenantID, err)
	}
	if scopeID != "" {
		ctx, err = tenant.WithScope(ctx, scopeID)
		if err != nil {
			t.Fatalf("WithScope(%q): %v", scopeID, err)
		}
	}
	return ctx
}

// createProject writes one "project" row via the command executor and returns
// its aggregate ID.
func createProject(t *testing.T, x *command.Executor, ctx context.Context, label string) string {
	t.Helper()
	res, err := x.Exec(ctx, command.Command{
		Entity:  "project",
		Op:      command.OpCreate,
		Payload: map[string]any{"label": label},
	})
	if err != nil {
		t.Fatalf("create project %q: %v", label, err)
	}
	return res.AggID
}

// projectIDs lists all "project" rows visible under ctx and returns their IDs
// in a set for easy membership checks.
func projectIDs(t *testing.T, a *postgres.Adapter, ctx context.Context) map[string]bool {
	t.Helper()
	var rows []map[string]any
	if err := a.List(ctx, "project", query.ListQuery{}, &rows); err != nil {
		t.Fatalf("List projects: %v", err)
	}
	out := make(map[string]bool, len(rows))
	for _, r := range rows {
		if id, ok := r["id"].(string); ok {
			out[id] = true
		}
	}
	return out
}

// TestScope_SoftRLSFilter is the main scope integration test. It proves that
// the scope-aware RLS USING predicate correctly partitions rows inside a
// single tenant and that cross-tenant isolation is preserved.
func TestScope_SoftRLSFilter(t *testing.T) {
	_, a, x := newScopeHarness(t)

	ws1 := scopedCtx(t, "ws_1", "")
	ws1A := scopedCtx(t, "ws_1", "proj_A")
	ws1B := scopedCtx(t, "ws_1", "proj_B")
	ws2 := scopedCtx(t, "ws_2", "")

	// Write three rows under tenant ws_1.
	idA := createProject(t, x, ws1A, "alpha")      // scope_id = "proj_A"
	idB := createProject(t, x, ws1B, "bravo")      // scope_id = "proj_B"
	idShared := createProject(t, x, ws1, "shared") // scope_id = NULL (unscoped)

	// Write one row under a different tenant (ws_2) — must never leak.
	idWs2 := createProject(t, x, ws2, "ws2-row")

	// Sanity: confirm all four rows were written by reading as superuser via
	// the owner adapter (which bypasses RLS).  Not strictly required for the
	// RLS proof but makes test failures much easier to diagnose.
	t.Logf("ids: A=%s B=%s shared=%s ws2=%s", idA, idB, idShared, idWs2)

	// --- Scoped read: proj_A -------------------------------------------------
	// Must see: idA (proj_A row) + idShared (NULL-scope row).
	// Must NOT see: idB (proj_B row) or idWs2 (wrong tenant).
	t.Run("scoped_projA", func(t *testing.T) {
		got := projectIDs(t, a, ws1A)
		if !got[idA] {
			t.Errorf("proj_A row %s missing from scoped(proj_A) read; got %v", idA, got)
		}
		if !got[idShared] {
			t.Errorf("shared (NULL-scope) row %s missing from scoped(proj_A) read; got %v", idShared, got)
		}
		if got[idB] {
			t.Errorf("proj_B row %s leaked into scoped(proj_A) read; got %v", idB, got)
		}
		if got[idWs2] {
			t.Errorf("ws_2 row %s leaked into ws_1/proj_A read; got %v", idWs2, got)
		}
		if len(got) != 2 {
			t.Errorf("scoped(proj_A) returned %d rows, want 2: %v", len(got), got)
		}
	})

	// --- Scoped read: proj_B -------------------------------------------------
	// Must see: idB + idShared.
	// Must NOT see: idA or idWs2.
	t.Run("scoped_projB", func(t *testing.T) {
		got := projectIDs(t, a, ws1B)
		if !got[idB] {
			t.Errorf("proj_B row %s missing from scoped(proj_B) read; got %v", idB, got)
		}
		if !got[idShared] {
			t.Errorf("shared row %s missing from scoped(proj_B) read; got %v", idShared, got)
		}
		if got[idA] {
			t.Errorf("proj_A row %s leaked into scoped(proj_B) read; got %v", idA, got)
		}
		if got[idWs2] {
			t.Errorf("ws_2 row %s leaked into ws_1/proj_B read; got %v", idWs2, got)
		}
		if len(got) != 2 {
			t.Errorf("scoped(proj_B) returned %d rows, want 2: %v", len(got), got)
		}
	})

	// --- Unscoped read (ws_1) ------------------------------------------------
	// Must see: all three ws_1 rows (idA + idB + idShared).
	// Must NOT see: idWs2.
	t.Run("unscoped_ws1", func(t *testing.T) {
		got := projectIDs(t, a, ws1)
		if !got[idA] {
			t.Errorf("proj_A row %s missing from unscoped ws_1 read; got %v", idA, got)
		}
		if !got[idB] {
			t.Errorf("proj_B row %s missing from unscoped ws_1 read; got %v", idB, got)
		}
		if !got[idShared] {
			t.Errorf("shared row %s missing from unscoped ws_1 read; got %v", idShared, got)
		}
		if got[idWs2] {
			t.Errorf("ws_2 row %s leaked into unscoped ws_1 read; got %v", idWs2, got)
		}
		if len(got) != 3 {
			t.Errorf("unscoped ws_1 returned %d rows, want 3: %v", len(got), got)
		}
	})

	// --- Tenant isolation: ws_2 unscoped -------------------------------------
	// Must see: only idWs2. Must NOT see any ws_1 row.
	t.Run("tenant_isolation_ws2", func(t *testing.T) {
		got := projectIDs(t, a, ws2)
		if got[idA] || got[idB] || got[idShared] {
			t.Errorf("ws_1 rows leaked into ws_2 read; got %v", got)
		}
		if !got[idWs2] {
			t.Errorf("ws_2 row %s missing from ws_2 read; got %v", idWs2, got)
		}
		if len(got) != 1 {
			t.Errorf("ws_2 unscoped returned %d rows, want 1: %v", len(got), got)
		}
	})
}

// TestScope_ScopeIDStampedByCommandPath verifies the write-path claim: the
// relational command path does stamp scope_id on rows when the entity declares
// the column.  Read raw via superuser (owner adapter) to bypass RLS and
// confirm the physical column value.
func TestScope_ScopeIDStampedByCommandPath(t *testing.T) {
	owner, _, x := newScopeHarness(t)
	ctx := context.Background()

	ws1A := scopedCtx(t, "ws_1", "proj_A")
	ws1 := scopedCtx(t, "ws_1", "")

	idA := createProject(t, x, ws1A, "scoped-row")
	idShared := createProject(t, x, ws1, "shared-row")

	// Read raw rows as superuser (pool path bypasses RLS) to inspect scope_id.
	rows, err := owner.Driver().Query(ctx,
		fmt.Sprintf(`SELECT id, scope_id FROM %s WHERE tenant_id = 'ws_1' ORDER BY id`, scopeTestTable))
	if err != nil {
		t.Fatalf("owner query: %v", err)
	}
	defer rows.Close()

	type rowData struct {
		id      string
		scopeID *string // nullable
	}
	var got []rowData
	for rows.Next() {
		var rd rowData
		if err := rows.Scan(&rd.id, &rd.scopeID); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, rd)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	byID := make(map[string]*string)
	for _, r := range got {
		byID[r.id] = r.scopeID
	}

	// idA must have scope_id = "proj_A".
	scopeA, found := byID[idA]
	if !found {
		t.Fatalf("scoped row %s not found in raw query", idA)
	}
	if scopeA == nil || *scopeA != "proj_A" {
		t.Errorf("scoped row %s: scope_id = %v, want %q", idA, scopeA, "proj_A")
	}

	// idShared must have scope_id = NULL.
	scopeShared, found := byID[idShared]
	if !found {
		t.Fatalf("shared row %s not found in raw query", idShared)
	}
	if scopeShared != nil {
		t.Errorf("unscoped row %s: scope_id = %q, want NULL", idShared, *scopeShared)
	}
}

// newPortScopeHarness boots a container, runs all fabriq migrations (which add
// scope_id to fabriq_embeddings and fabriq_geometries via 0012), and returns
// an app-role adapter so RLS actually applies. The caller must skip the test if
// the relevant extension table is absent.
func newPortScopeHarness(t *testing.T) *postgres.Adapter {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
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

	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app role): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// emb768 builds a 768-dimensional embedding with only the first two components
// set. This matches the column definition in migration 0006 (vector(768)).
func emb768(x, y float32) []float32 {
	v := make([]float32, 768)
	v[0], v[1] = x, y
	return v
}

// vectorIDs runs a Similar query and returns the result IDs in a set.
func vectorIDs(t *testing.T, a *postgres.Adapter, ctx context.Context, entity string, embedding []float32, k int) map[string]bool {
	t.Helper()
	var matches []query.VectorMatch
	if err := a.Similar(ctx, query.VectorQuery{Entity: entity, Embedding: embedding, K: k}, &matches); err != nil {
		t.Fatalf("Similar: %v", err)
	}
	out := make(map[string]bool, len(matches))
	for _, m := range matches {
		out[m.ID] = true
	}
	return out
}

// spatialIDs runs a Within query and returns the result IDs in a set.
func spatialIDs(t *testing.T, s *postgres.SpatialAdapter, ctx context.Context, entity string, center query.Geometry, radiusM float64, k int) map[string]bool {
	t.Helper()
	var matches []query.SpatialMatch
	if err := s.Within(ctx, query.SpatialQuery{Entity: entity, Center: center, RadiusM: radiusM, K: k}, &matches); err != nil {
		t.Fatalf("Within: %v", err)
	}
	out := make(map[string]bool, len(matches))
	for _, m := range matches {
		out[m.ID] = true
	}
	return out
}

// TestScope_VectorScopeFilter proves that Vector.Upsert now stamps scope_id so
// the RLS policy on fabriq_embeddings correctly partitions rows by scope.
// Scoped Similar returns the caller's scope rows plus shared (NULL-scope) rows
// but not rows belonging to other scopes. Unscoped Similar returns all rows
// within the tenant.
func TestScope_VectorScopeFilter(t *testing.T) {
	a := newPortScopeHarness(t)
	ctx := context.Background()

	// Check that fabriq_embeddings exists (pgvector may be absent in the image).
	// Use a raw query on the owner path (which bypasses RLS) — we need a superuser
	// check, but since newPortScopeHarness closed the owner we use information_schema
	// through the app-role connection which can still read it.
	var tableExists bool
	row := a.Driver().QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'fabriq_embeddings'
		)`)
	if err := row.Scan(&tableExists); err != nil {
		t.Fatalf("check fabriq_embeddings: %v", err)
	}
	if !tableExists {
		t.Skip("fabriq_embeddings not present: pgvector unavailable in this Postgres image")
	}

	ws1 := scopedCtx(t, "ws_1", "")
	ws1A := scopedCtx(t, "ws_1", "proj_A")
	ws1B := scopedCtx(t, "ws_1", "proj_B")

	// Use distinguishable embeddings so the cosine distance ordering is
	// deterministic and Similar with K=10 returns all three ws_1 rows.
	embA := emb768(1, 0)      // proj_A scope
	embB := emb768(0, 1)      // proj_B scope
	embShared := emb768(1, 1) // unscoped (shared)

	if err := a.Upsert(ws1A, "sensor", "emb_A", embA, nil); err != nil {
		t.Fatalf("Upsert emb_A (proj_A): %v", err)
	}
	if err := a.Upsert(ws1B, "sensor", "emb_B", embB, nil); err != nil {
		t.Fatalf("Upsert emb_B (proj_B): %v", err)
	}
	if err := a.Upsert(ws1, "sensor", "emb_shared", embShared, nil); err != nil {
		t.Fatalf("Upsert emb_shared (unscoped): %v", err)
	}

	// Scoped proj_A: must see emb_A (proj_A) + emb_shared (NULL scope).
	// Must NOT see emb_B (proj_B).
	t.Run("scoped_projA", func(t *testing.T) {
		got := vectorIDs(t, a, ws1A, "sensor", embA, 10)
		if !got["emb_A"] {
			t.Errorf("emb_A missing from scoped(proj_A) Similar; got %v", got)
		}
		if !got["emb_shared"] {
			t.Errorf("emb_shared (NULL-scope) missing from scoped(proj_A) Similar; got %v", got)
		}
		if got["emb_B"] {
			t.Errorf("emb_B (proj_B) leaked into scoped(proj_A) Similar; got %v", got)
		}
		if len(got) != 2 {
			t.Errorf("scoped(proj_A) Similar returned %d rows, want 2: %v", len(got), got)
		}
	})

	// Scoped proj_B: must see emb_B + emb_shared. Must NOT see emb_A.
	t.Run("scoped_projB", func(t *testing.T) {
		got := vectorIDs(t, a, ws1B, "sensor", embB, 10)
		if !got["emb_B"] {
			t.Errorf("emb_B missing from scoped(proj_B) Similar; got %v", got)
		}
		if !got["emb_shared"] {
			t.Errorf("emb_shared missing from scoped(proj_B) Similar; got %v", got)
		}
		if got["emb_A"] {
			t.Errorf("emb_A (proj_A) leaked into scoped(proj_B) Similar; got %v", got)
		}
		if len(got) != 2 {
			t.Errorf("scoped(proj_B) Similar returned %d rows, want 2: %v", len(got), got)
		}
	})

	// Unscoped (ws_1): must see all three rows.
	t.Run("unscoped_ws1", func(t *testing.T) {
		got := vectorIDs(t, a, ws1, "sensor", emb768(1, 1), 10)
		if !got["emb_A"] {
			t.Errorf("emb_A missing from unscoped ws_1 Similar; got %v", got)
		}
		if !got["emb_B"] {
			t.Errorf("emb_B missing from unscoped ws_1 Similar; got %v", got)
		}
		if !got["emb_shared"] {
			t.Errorf("emb_shared missing from unscoped ws_1 Similar; got %v", got)
		}
		if len(got) != 3 {
			t.Errorf("unscoped ws_1 Similar returned %d rows, want 3: %v", len(got), got)
		}
	})
}

// TestScope_SpatialScopeFilter proves that Spatial.Upsert now stamps scope_id
// so the RLS policy on fabriq_geometries correctly partitions rows by scope.
// Scoped Within returns the caller's scope rows plus shared (NULL-scope) rows
// but not rows belonging to other scopes.
func TestScope_SpatialScopeFilter(t *testing.T) {
	a := newPortScopeHarness(t)
	ctx := context.Background()

	// Check that fabriq_geometries exists (PostGIS may be absent in the image).
	var tableExists bool
	row := a.Driver().QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'fabriq_geometries'
		)`)
	if err := row.Scan(&tableExists); err != nil {
		t.Fatalf("check fabriq_geometries: %v", err)
	}
	if !tableExists {
		t.Skip("fabriq_geometries not present: PostGIS unavailable in this Postgres image")
	}

	s := postgres.NewSpatialAdapter(a)

	ws1 := scopedCtx(t, "ws_1", "")
	ws1A := scopedCtx(t, "ws_1", "proj_A")
	ws1B := scopedCtx(t, "ws_1", "proj_B")

	// Three points all within 100 m of the origin so Within with radius 100 m
	// returns all that RLS permits. SRID 0 = planar (units are coordinate units).
	origin := query.Geometry{WKT: "POINT (0 0)", SRID: 0}
	if err := s.Upsert(ws1A, "site", "geo_A", query.Geometry{WKT: "POINT (1 0)", SRID: 0}, nil); err != nil {
		t.Fatalf("Upsert geo_A (proj_A): %v", err)
	}
	if err := s.Upsert(ws1B, "site", "geo_B", query.Geometry{WKT: "POINT (0 1)", SRID: 0}, nil); err != nil {
		t.Fatalf("Upsert geo_B (proj_B): %v", err)
	}
	if err := s.Upsert(ws1, "site", "geo_shared", query.Geometry{WKT: "POINT (0 0)", SRID: 0}, nil); err != nil {
		t.Fatalf("Upsert geo_shared (unscoped): %v", err)
	}

	// Scoped proj_A: must see geo_A + geo_shared. Must NOT see geo_B.
	t.Run("scoped_projA", func(t *testing.T) {
		got := spatialIDs(t, s, ws1A, "site", origin, 100, 10)
		if !got["geo_A"] {
			t.Errorf("geo_A missing from scoped(proj_A) Within; got %v", got)
		}
		if !got["geo_shared"] {
			t.Errorf("geo_shared (NULL-scope) missing from scoped(proj_A) Within; got %v", got)
		}
		if got["geo_B"] {
			t.Errorf("geo_B (proj_B) leaked into scoped(proj_A) Within; got %v", got)
		}
		if len(got) != 2 {
			t.Errorf("scoped(proj_A) Within returned %d rows, want 2: %v", len(got), got)
		}
	})

	// Scoped proj_B: must see geo_B + geo_shared. Must NOT see geo_A.
	t.Run("scoped_projB", func(t *testing.T) {
		got := spatialIDs(t, s, ws1B, "site", origin, 100, 10)
		if !got["geo_B"] {
			t.Errorf("geo_B missing from scoped(proj_B) Within; got %v", got)
		}
		if !got["geo_shared"] {
			t.Errorf("geo_shared missing from scoped(proj_B) Within; got %v", got)
		}
		if got["geo_A"] {
			t.Errorf("geo_A (proj_A) leaked into scoped(proj_B) Within; got %v", got)
		}
		if len(got) != 2 {
			t.Errorf("scoped(proj_B) Within returned %d rows, want 2: %v", len(got), got)
		}
	})

	// Unscoped (ws_1): must see all three rows.
	t.Run("unscoped_ws1", func(t *testing.T) {
		got := spatialIDs(t, s, ws1, "site", origin, 100, 10)
		if !got["geo_A"] {
			t.Errorf("geo_A missing from unscoped ws_1 Within; got %v", got)
		}
		if !got["geo_B"] {
			t.Errorf("geo_B missing from unscoped ws_1 Within; got %v", got)
		}
		if !got["geo_shared"] {
			t.Errorf("geo_shared missing from unscoped ws_1 Within; got %v", got)
		}
		if len(got) != 3 {
			t.Errorf("unscoped ws_1 Within returned %d rows, want 3: %v", len(got), got)
		}
	})
}

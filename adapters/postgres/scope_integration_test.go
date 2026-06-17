//go:build integration

package postgres_test

// TestScope_SoftRLSFilter proves the native secondary-scope feature end-to-end
// against real Postgres RLS. The test uses a dynamic entity ("project") that
// declares a scope_id column so the relational command path (stampedValues)
// writes scope_id on each row. The table gets the ScopeAwareTenantPolicy from
// migrations/0012_scope.go applied on top of the standard tenant_isolation
// policy created by EnsureDynamic.
//
// Write-path finding: Vector/Spatial port Upserts do NOT stamp scope_id — the
// INSERT for fabriq_embeddings / fabriq_geometries does not include the
// scope_id column, so every Upsert stores NULL regardless of context scope.
// The scope_id SET LOCAL in inTenantTx is present only for read-side RLS
// filtering. This is a port-level gap: scoped Vector/Spatial writes always
// produce shared (NULL-scope) rows. The test therefore uses the relational
// command path, which is the only path that stamps scope_id via stampedValues.

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
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

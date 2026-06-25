//go:build integration

package postgres_test

// TestScope_DocumentSyncFilter and TestScope_DocumentScopeIDStamped prove the
// native secondary-scope feature for the Document/CRDT port end-to-end against
// real Postgres RLS, mirroring scope_integration_test.go for the relational /
// vector / spatial ports.
//
// The CRDT content tables (fabriq_crdt_updates, fabriq_crdt_snapshots) gain a
// nullable scope_id column and the ScopeAwareTenantPolicy (migration 0013) so
// Sync inherits the soft scope filter: a scoped reader sees its own scope plus
// shared (NULL-scope) docs; an unscoped reader sees all docs in the tenant; a
// cross-scope or cross-tenant reader sees nothing. DocStore.ApplyUpdate stamps
// scope_id on writes; Compact stamps the snapshot with the doc's recorded scope
// so the compacted state is filtered exactly like the raw update log.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/xraph/grove/crdt"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// newDocScopeHarness boots one Postgres container, runs every fabriq migration
// (which adds scope_id + the scope-aware RLS policy to the CRDT content tables
// via 0013), registers the example domain (for the "page" KindDocument entity),
// and returns (superuser/owner adapter, app-role adapter). RLS only constrains
// the NOBYPASSRLS app role, so scoped reads must go through it.
func newDocScopeHarness(t *testing.T) (*postgres.Adapter, *postgres.Adapter) {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatalf("domain.RegisterAll: %v", err)
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

	// The demo "pages" materialization target moved out of the shipped
	// migrations (host-collision hazard); create it as owner before the app role.
	fabriqtest.ApplyDDL(t, superDSN, domain.PagesDDL())

	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app role): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	return owner, a
}

// crdtLWWUpdate encodes one LWW field write as a grove-crdt update blob — the
// []crdt.ChangeRecord shape DocStore.ApplyUpdate folds through the merge engine.
func crdtLWWUpdate(t testing.TB, table, docID, field string, value any, hlcWall int64, node string) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := json.Marshal([]crdt.ChangeRecord{{
		Table: table, PK: docID, Field: field, CRDTType: crdt.TypeLWW,
		HLC: crdt.HLC{Timestamp: hlcWall, NodeID: node}, NodeID: node, Value: raw,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return blob
}

// docHasContent reports whether a Sync from scratch under ctx returns anything
// for docID — either later updates or a compacted snapshot. Under the soft scope
// filter a hidden doc Syncs to an empty payload (no error), so absence of
// content is the read-side proof that RLS hid the doc.
func docHasContent(t *testing.T, docs *postgres.DocStore, ctx context.Context, docID string) bool {
	t.Helper()
	blob, err := docs.Sync(ctx, docID, nil)
	if err != nil {
		t.Fatalf("Sync(%s): %v", docID, err)
	}
	var sp struct {
		Snapshot json.RawMessage   `json:"snapshot,omitempty"`
		Updates  []json.RawMessage `json:"updates"`
	}
	if err := json.Unmarshal(blob, &sp); err != nil {
		t.Fatalf("unmarshal sync payload for %s: %v", docID, err)
	}
	return len(sp.Updates) > 0 || len(sp.Snapshot) > 0
}

// TestScope_DocumentSyncFilter is the main Document/CRDT scope integration test:
// the scope-aware RLS predicate on fabriq_crdt_updates / fabriq_crdt_snapshots
// partitions docs inside a single tenant (own scope + shared) while preserving
// cross-tenant isolation, observed through Sync.
func TestScope_DocumentSyncFilter(t *testing.T) {
	_, a := newDocScopeHarness(t)
	docs := a.Documents()

	ws1 := scopedCtx(t, "ws_1", "")
	ws1A := scopedCtx(t, "ws_1", "proj_A")
	ws1B := scopedCtx(t, "ws_1", "proj_B")
	ws2 := scopedCtx(t, "ws_2", "")

	docA := "page/" + event.NewID()      // ws_1 / proj_A
	docB := "page/" + event.NewID()      // ws_1 / proj_B
	docShared := "page/" + event.NewID() // ws_1 / unscoped (shared)
	docWs2 := "page/" + event.NewID()    // ws_2 / unscoped

	if err := docs.ApplyUpdate(ws1A, docA, crdtLWWUpdate(t, "pages", docA, "title", "alpha", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate docA: %v", err)
	}
	if err := docs.ApplyUpdate(ws1B, docB, crdtLWWUpdate(t, "pages", docB, "title", "bravo", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate docB: %v", err)
	}
	if err := docs.ApplyUpdate(ws1, docShared, crdtLWWUpdate(t, "pages", docShared, "title", "shared", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate docShared: %v", err)
	}
	if err := docs.ApplyUpdate(ws2, docWs2, crdtLWWUpdate(t, "pages", docWs2, "title", "ws2", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate docWs2: %v", err)
	}

	visible := func(t *testing.T, ctx context.Context, label, docID string) {
		t.Helper()
		if !docHasContent(t, docs, ctx, docID) {
			t.Errorf("%s: doc %s should be VISIBLE but Sync returned no content", label, docID)
		}
	}
	hidden := func(t *testing.T, ctx context.Context, label, docID string) {
		t.Helper()
		if docHasContent(t, docs, ctx, docID) {
			t.Errorf("%s: doc %s should be HIDDEN but Sync returned content", label, docID)
		}
	}

	// Scoped proj_A: own scope (docA) + shared (docShared); not proj_B, not ws_2.
	t.Run("scoped_projA", func(t *testing.T) {
		visible(t, ws1A, "scoped(proj_A)", docA)
		visible(t, ws1A, "scoped(proj_A)", docShared)
		hidden(t, ws1A, "scoped(proj_A)", docB)
		hidden(t, ws1A, "scoped(proj_A)", docWs2)
	})

	// Scoped proj_B: own scope (docB) + shared (docShared); not proj_A, not ws_2.
	t.Run("scoped_projB", func(t *testing.T) {
		visible(t, ws1B, "scoped(proj_B)", docB)
		visible(t, ws1B, "scoped(proj_B)", docShared)
		hidden(t, ws1B, "scoped(proj_B)", docA)
		hidden(t, ws1B, "scoped(proj_B)", docWs2)
	})

	// Unscoped ws_1: every ws_1 doc regardless of scope; not ws_2.
	t.Run("unscoped_ws1", func(t *testing.T) {
		visible(t, ws1, "unscoped(ws_1)", docA)
		visible(t, ws1, "unscoped(ws_1)", docB)
		visible(t, ws1, "unscoped(ws_1)", docShared)
		hidden(t, ws1, "unscoped(ws_1)", docWs2)
	})

	// Tenant isolation: ws_2 sees only its own doc.
	t.Run("tenant_isolation_ws2", func(t *testing.T) {
		visible(t, ws2, "unscoped(ws_2)", docWs2)
		hidden(t, ws2, "unscoped(ws_2)", docA)
		hidden(t, ws2, "unscoped(ws_2)", docB)
		hidden(t, ws2, "unscoped(ws_2)", docShared)
	})

	// Compaction folds the update log into a snapshot; the snapshot must inherit
	// the doc's scope so Sync stays filtered exactly like the raw log.
	t.Run("compact_snapshot_scope_filter", func(t *testing.T) {
		if err := docs.Compact(ws1A, docA); err != nil {
			t.Fatalf("Compact docA: %v", err)
		}
		visible(t, ws1A, "post-compact scoped(proj_A)", docA)
		visible(t, ws1, "post-compact unscoped(ws_1)", docA)
		hidden(t, ws1B, "post-compact scoped(proj_B)", docA)
		hidden(t, ws2, "post-compact unscoped(ws_2)", docA)
	})
}

// rawScope reads scope_id for docID from table via the owner (superuser) adapter,
// which bypasses RLS, so it inspects the physical column value. Returns (nil,
// false) when no row exists.
func rawScope(t *testing.T, owner *postgres.Adapter, table, docID string) (*string, bool) {
	t.Helper()
	rows, err := owner.Driver().Query(context.Background(),
		fmt.Sprintf(`SELECT scope_id FROM %s WHERE doc_id = $1 LIMIT 1`, table), docID)
	if err != nil {
		t.Fatalf("owner query %s: %v", table, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, false
	}
	var scope *string
	if err := rows.Scan(&scope); err != nil {
		t.Fatalf("scan %s.scope_id: %v", table, err)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err %s: %v", table, err)
	}
	return scope, true
}

// TestScope_DocumentScopeIDStamped verifies the write-path claim: ApplyUpdate
// stamps scope_id on the update log and the bookkeeping row, and Compact stamps
// the snapshot with the doc's scope. A scoped write yields scope_id = "proj_A";
// an unscoped write yields a true NULL (the shared sentinel).
func TestScope_DocumentScopeIDStamped(t *testing.T) {
	owner, a := newDocScopeHarness(t)
	docs := a.Documents()

	ws1A := scopedCtx(t, "ws_1", "proj_A")
	ws1 := scopedCtx(t, "ws_1", "")

	docA := "page/" + event.NewID()
	docShared := "page/" + event.NewID()

	if err := docs.ApplyUpdate(ws1A, docA, crdtLWWUpdate(t, "pages", docA, "title", "a", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate docA: %v", err)
	}
	if err := docs.ApplyUpdate(ws1, docShared, crdtLWWUpdate(t, "pages", docShared, "title", "s", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate docShared: %v", err)
	}

	wantScoped := func(t *testing.T, table, docID string) {
		t.Helper()
		scope, found := rawScope(t, owner, table, docID)
		if !found {
			t.Fatalf("%s: no row for %s", table, docID)
		}
		if scope == nil || *scope != "proj_A" {
			t.Errorf("%s[%s].scope_id = %v, want %q", table, docID, scope, "proj_A")
		}
	}
	wantShared := func(t *testing.T, table, docID string) {
		t.Helper()
		scope, found := rawScope(t, owner, table, docID)
		if !found {
			t.Fatalf("%s: no row for %s", table, docID)
		}
		if scope != nil {
			t.Errorf("%s[%s].scope_id = %q, want NULL", table, docID, *scope)
		}
	}

	wantScoped(t, "fabriq_crdt_updates", docA)
	wantScoped(t, "fabriq_crdt_docs", docA)
	wantShared(t, "fabriq_crdt_updates", docShared)
	wantShared(t, "fabriq_crdt_docs", docShared)

	// Compaction stamps the snapshot with the doc's recorded scope, regardless of
	// the caller's ctx scope.
	if err := docs.Compact(ws1A, docA); err != nil {
		t.Fatalf("Compact docA (scoped): %v", err)
	}
	if err := docs.Compact(ws1, docShared); err != nil {
		t.Fatalf("Compact docShared (unscoped): %v", err)
	}
	wantScoped(t, "fabriq_crdt_snapshots", docA)
	wantShared(t, "fabriq_crdt_snapshots", docShared)
}

// docScopeMatTable is the dynamic KindDocument table for the materialization
// scope-carry test. A scope_id column makes it a scope-aware entity table, which
// the demo "page" entity (no scope_id) cannot be without breaking conformance.
const docScopeMatTable = "ds_doc_scope_mat"

// newDocMaterializeScopeHarness boots one Postgres container, runs the fabriq
// migrations, then provisions a *scope-aware* dynamic KindDocument entity
// ("note") whose table carries scope_id and the ScopeAwareTenantPolicy. It
// returns (owner adapter, app-role adapter). The short QuietWindow lets
// MaterializeQuiet fire after a brief sleep instead of the page entity's 2s.
func newDocMaterializeScopeHarness(t *testing.T) (*postgres.Adapter, *postgres.Adapter) {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "note",
		Kind: registry.KindDocument,
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt", SnapshotEvery: 64, QuietWindow: 10 * time.Millisecond},
		Schema: &registry.DynamicSchema{
			Table: docScopeMatTable,
			Columns: []registry.DynamicColumn{
				{Name: "title", Type: registry.ColText},
				{Name: "scope_id", Type: registry.ColText},
			},
		},
	})

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

	ent, ok := reg.Get("note")
	if !ok {
		t.Fatal("entity 'note' not registered")
	}
	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("EnsureDynamic: %v", err)
	}
	// Upgrade to the scope-aware policy so the materialized rows are filtered by
	// scope, mirroring scope_integration_test.go's project harness.
	for _, stmt := range migrations.ScopeAwareTenantPolicy(docScopeMatTable) {
		if _, err := owner.Driver().Exec(ctx, stmt); err != nil {
			t.Fatalf("apply scope RLS policy (%q): %v", stmt, err)
		}
	}

	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app role): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	return owner, a
}

// noteIDs lists every "note" row visible under ctx and returns the id set.
func noteIDs(t *testing.T, a *postgres.Adapter, ctx context.Context) map[string]bool {
	t.Helper()
	var rows []map[string]any
	if err := a.List(ctx, "note", query.ListQuery{}, &rows); err != nil {
		t.Fatalf("List notes: %v", err)
	}
	out := make(map[string]bool, len(rows))
	for _, r := range rows {
		if id, ok := r["id"].(string); ok {
			out[id] = true
		}
	}
	return out
}

// TestScope_DocumentMaterializationCarriesScope proves that the quiet-window
// materializer preserves a document's scope: a CRDT doc written under scope
// proj_A materializes into an entity row stamped scope_id = "proj_A" (so RLS
// keeps it partitioned), while an unscoped doc materializes to NULL (shared).
// Without scope-carry the materialized row would default to NULL and leak the
// scoped doc to every scope in the tenant.
func TestScope_DocumentMaterializationCarriesScope(t *testing.T) {
	owner, a := newDocMaterializeScopeHarness(t)
	docs := a.Documents()

	ws1 := scopedCtx(t, "ws_1", "")
	ws1A := scopedCtx(t, "ws_1", "proj_A")
	ws1B := scopedCtx(t, "ws_1", "proj_B")

	docA := "note/" + event.NewID()      // proj_A
	docShared := "note/" + event.NewID() // unscoped (shared)

	if err := docs.ApplyUpdate(ws1A, docA, crdtLWWUpdate(t, docScopeMatTable, docA, "title", "alpha", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate docA: %v", err)
	}
	if err := docs.ApplyUpdate(ws1, docShared, crdtLWWUpdate(t, docScopeMatTable, docShared, "title", "shared", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate docShared: %v", err)
	}

	// Quiet window (10ms) elapses, then materialize both docs.
	time.Sleep(100 * time.Millisecond)
	n, err := docs.MaterializeQuiet(context.Background(), nil)
	if err != nil {
		t.Fatalf("MaterializeQuiet: %v", err)
	}
	if n != 2 {
		t.Fatalf("materialized %d docs, want 2", n)
	}

	// Raw column proof (owner bypasses RLS): the materialized row carries the
	// doc's scope.
	matScope := func(t *testing.T, id string) (*string, bool) {
		t.Helper()
		rows, err := owner.Driver().Query(context.Background(),
			fmt.Sprintf(`SELECT scope_id FROM %s WHERE id = $1`, docScopeMatTable), id)
		if err != nil {
			t.Fatalf("owner query: %v", err)
		}
		defer rows.Close()
		if !rows.Next() {
			return nil, false
		}
		var scope *string
		if err := rows.Scan(&scope); err != nil {
			t.Fatalf("scan scope_id: %v", err)
		}
		return scope, true
	}

	if scope, found := matScope(t, docA); !found {
		t.Fatalf("materialized row for %s missing", docA)
	} else if scope == nil || *scope != "proj_A" {
		t.Errorf("materialized %s scope_id = %v, want %q", docA, scope, "proj_A")
	}
	if scope, found := matScope(t, docShared); !found {
		t.Fatalf("materialized row for %s missing", docShared)
	} else if scope != nil {
		t.Errorf("materialized %s scope_id = %q, want NULL", docShared, *scope)
	}

	// RLS visibility proof through List: the scoped row is partitioned exactly
	// like any natively-written scope-aware row.
	t.Run("scoped_projA_sees_own_and_shared", func(t *testing.T) {
		got := noteIDs(t, a, ws1A)
		if !got[docA] || !got[docShared] {
			t.Errorf("scoped(proj_A) should see docA + docShared; got %v", got)
		}
	})
	t.Run("scoped_projB_sees_only_shared", func(t *testing.T) {
		got := noteIDs(t, a, ws1B)
		if got[docA] {
			t.Errorf("docA (proj_A) leaked into scoped(proj_B) read; got %v", got)
		}
		if !got[docShared] {
			t.Errorf("shared row missing from scoped(proj_B) read; got %v", got)
		}
	})
	t.Run("unscoped_ws1_sees_both", func(t *testing.T) {
		got := noteIDs(t, a, ws1)
		if !got[docA] || !got[docShared] {
			t.Errorf("unscoped ws_1 should see both rows; got %v", got)
		}
	})
}

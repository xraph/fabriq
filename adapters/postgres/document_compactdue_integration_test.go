//go:build integration

package postgres_test

// CompactDue is the scheduled-compaction sweep the forgeext worker drives:
// every document whose un-compacted update log has reached its entity's
// CRDTSpec.SnapshotEvery budget is compacted (log folded into the snapshot
// row and trimmed). These tests pin the sweep contract:
//
//   - a doc at/over budget compacts; one under budget is left alone;
//   - the merged state is unchanged by compaction (storage shape only);
//   - a second sweep is a no-op until the log grows back to budget;
//   - SnapshotEvery <= 0 disables scheduled compaction for the entity;
//   - flagged docs are skipped (their raw log is evidence for resolution);
//   - the sweep spans tenants from a single tenant-less worker context.

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

const (
	compactNoteTable = "ds_compact_notes"
	compactMemoTable = "ds_compact_memos"
)

// newCompactDueHarness boots Postgres, runs the fabriq migrations, and
// registers two dynamic KindDocument entities: "cnote" with a tiny
// SnapshotEvery budget (3) and "cmemo" with scheduled compaction disabled
// (SnapshotEvery 0). QuietWindow is an hour so the materializer never
// interferes. Returns (owner adapter, app-role adapter).
func newCompactDueHarness(t *testing.T) (*postgres.Adapter, *postgres.Adapter) {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "cnote",
		Kind: registry.KindDocument,
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt", SnapshotEvery: 3, QuietWindow: time.Hour},
		Schema: &registry.DynamicSchema{
			Table:   compactNoteTable,
			Columns: []registry.DynamicColumn{{Name: "title", Type: registry.ColText}},
		},
	})
	reg.MustRegister(registry.EntitySpec{
		Name: "cmemo",
		Kind: registry.KindDocument,
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt", SnapshotEvery: 0, QuietWindow: time.Hour},
		Schema: &registry.DynamicSchema{
			Table:   compactMemoTable,
			Columns: []registry.DynamicColumn{{Name: "title", Type: registry.ColText}},
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

	for _, name := range []string{"cnote", "cmemo"} {
		ent, ok := reg.Get(name)
		if !ok {
			t.Fatalf("entity %q not registered", name)
		}
		if err := owner.EnsureDynamic(ctx, ent); err != nil {
			t.Fatalf("EnsureDynamic(%s): %v", name, err)
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

// rawDocRows counts rows for docID in table via the owner adapter (bypasses
// RLS), inspecting the physical storage shape.
func rawDocRows(t *testing.T, owner *postgres.Adapter, table, docID string) int {
	t.Helper()
	rows, err := owner.Driver().Query(context.Background(),
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE doc_id = $1`, table), docID)
	if err != nil {
		t.Fatalf("owner count %s: %v", table, err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("owner count %s: no row", table)
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatalf("scan count: %v", err)
	}
	return n
}

// applyN appends n LWW title updates to docID with ascending HLC timestamps.
func applyN(t *testing.T, ds *postgres.DocStore, ctx context.Context, table, docID string, n int, hlcBase int64) {
	t.Helper()
	for i := 0; i < n; i++ {
		u := crdtLWWUpdate(t, table, docID, "title", fmt.Sprintf("v%d", hlcBase+int64(i)), hlcBase+int64(i), "n1")
		if err := ds.ApplyUpdate(ctx, docID, u); err != nil {
			t.Fatalf("ApplyUpdate %s #%d: %v", docID, i, err)
		}
	}
}

func TestCompactDue_CompactsAtBudgetAcrossTenants(t *testing.T) {
	owner, a := newCompactDueHarness(t)
	ds := a.Documents()

	t1, _ := tenant.WithTenant(context.Background(), "t1")
	t2, _ := tenant.WithTenant(context.Background(), "t2")

	docA := "cnote/" + event.NewID() // t1, at budget (3)
	docB := "cnote/" + event.NewID() // t1, under budget (2)
	docC := "cnote/" + event.NewID() // t2, at budget (3)

	applyN(t, ds, t1, compactNoteTable, docA, 3, 100)
	applyN(t, ds, t1, compactNoteTable, docB, 2, 100)
	applyN(t, ds, t2, compactNoteTable, docC, 3, 100)

	before, err := ds.Snapshot(t1, docA)
	if err != nil {
		t.Fatalf("Snapshot(docA) pre-compact: %v", err)
	}

	// The sweep runs from a tenant-less worker context and must reach every
	// tenant's docs itself.
	n, err := ds.CompactDue(context.Background())
	if err != nil {
		t.Fatalf("CompactDue: %v", err)
	}
	if n != 2 {
		t.Fatalf("CompactDue compacted %d docs, want 2 (docA + docC)", n)
	}

	for doc, want := range map[string]struct{ updates, snaps int }{
		docA: {0, 1},
		docB: {2, 0},
		docC: {0, 1},
	} {
		if got := rawDocRows(t, owner, "fabriq_crdt_updates", doc); got != want.updates {
			t.Errorf("%s: %d update rows, want %d", doc, got, want.updates)
		}
		if got := rawDocRows(t, owner, "fabriq_crdt_snapshots", doc); got != want.snaps {
			t.Errorf("%s: %d snapshot rows, want %d", doc, got, want.snaps)
		}
	}

	// Compaction changes storage shape only — the merged state is identical.
	after, err := ds.Snapshot(t1, docA)
	if err != nil {
		t.Fatalf("Snapshot(docA) post-compact: %v", err)
	}
	if !reflect.DeepEqual(before.Snapshot, after.Snapshot) {
		t.Errorf("merged state changed by compaction:\n before %s\n after  %s", before.Snapshot, after.Snapshot)
	}

	// A second sweep finds nothing to do until the log grows back to budget.
	if n, err := ds.CompactDue(context.Background()); err != nil || n != 0 {
		t.Fatalf("second CompactDue = (%d, %v), want (0, nil)", n, err)
	}

	// The budget counts un-compacted updates, so the doc compacts again once
	// the fresh tail reaches it.
	applyN(t, ds, t1, compactNoteTable, docA, 3, 200)
	if n, err := ds.CompactDue(context.Background()); err != nil || n != 1 {
		t.Fatalf("third CompactDue = (%d, %v), want (1, nil)", n, err)
	}
	if got := rawDocRows(t, owner, "fabriq_crdt_updates", docA); got != 0 {
		t.Errorf("docA: %d update rows after re-compaction, want 0", got)
	}
	latest, err := ds.Snapshot(t1, docA)
	if err != nil {
		t.Fatalf("Snapshot(docA) after re-compaction: %v", err)
	}
	var title struct {
		Title string `json:"title"`
	}
	if err := json.Unmarshal(latest.Snapshot, &title); err != nil {
		t.Fatalf("unmarshal merged state: %v", err)
	}
	if title.Title != "v202" {
		t.Errorf("merged title = %q, want %q (last LWW write)", title.Title, "v202")
	}
}

func TestCompactDue_SnapshotEveryZeroDisables(t *testing.T) {
	owner, a := newCompactDueHarness(t)
	ds := a.Documents()

	t1, _ := tenant.WithTenant(context.Background(), "t1")
	doc := "cmemo/" + event.NewID()
	applyN(t, ds, t1, compactMemoTable, doc, 5, 100)

	if n, err := ds.CompactDue(context.Background()); err != nil || n != 0 {
		t.Fatalf("CompactDue = (%d, %v), want (0, nil) for disabled entity", n, err)
	}
	if got := rawDocRows(t, owner, "fabriq_crdt_updates", doc); got != 5 {
		t.Errorf("%d update rows, want 5 (log untouched)", got)
	}
	if got := rawDocRows(t, owner, "fabriq_crdt_snapshots", doc); got != 0 {
		t.Errorf("%d snapshot rows, want 0", got)
	}
}

func TestCompactDue_SkipsFlaggedDocs(t *testing.T) {
	owner, a := newCompactDueHarness(t)
	ds := a.Documents()

	t1, _ := tenant.WithTenant(context.Background(), "t1")
	doc := "cnote/" + event.NewID()
	applyN(t, ds, t1, compactNoteTable, doc, 3, 100)

	if _, err := owner.Driver().Exec(context.Background(),
		`UPDATE fabriq_crdt_docs SET flagged = TRUE, flag_reason = 'test' WHERE doc_id = $1`, doc); err != nil {
		t.Fatalf("flag doc: %v", err)
	}

	if n, err := ds.CompactDue(context.Background()); err != nil || n != 0 {
		t.Fatalf("CompactDue = (%d, %v), want (0, nil) for flagged doc", n, err)
	}
	if got := rawDocRows(t, owner, "fabriq_crdt_updates", doc); got != 3 {
		t.Errorf("%d update rows, want 3 (flagged doc's raw log preserved)", got)
	}
}

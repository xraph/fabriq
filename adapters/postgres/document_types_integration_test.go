//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

const docTypeMatTable = "ds_doc_type_mat"

// newDocTypeHarness boots one Postgres container, migrates, and provisions a
// dynamic KindDocument entity "twidget" whose table has a typed ColInt column
// (qty). noTypeCheck toggles the per-entity escape hatch. Returns (owner, app).
func newDocTypeHarness(t *testing.T, noTypeCheck bool) (*postgres.Adapter, *postgres.Adapter) {
	t.Helper()
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "twidget",
		Kind: registry.KindDocument,
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt", SnapshotEvery: 64, QuietWindow: 10 * time.Millisecond},
		Schema: &registry.DynamicSchema{
			Table:       docTypeMatTable,
			NoTypeCheck: noTypeCheck,
			Columns: []registry.DynamicColumn{
				{Name: "title", Type: registry.ColText},
				{Name: "qty", Type: registry.ColInt},
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
	ent, ok := reg.Get("twidget")
	if !ok {
		t.Fatal("twidget not registered")
	}
	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("EnsureDynamic: %v", err)
	}

	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (app role): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return owner, a
}

func docFlag(t *testing.T, owner *postgres.Adapter, docID string) (flagged bool, reason string, found bool) {
	t.Helper()
	rows, err := owner.Driver().Query(context.Background(),
		`SELECT flagged, COALESCE(flag_reason, '') FROM fabriq_crdt_docs WHERE doc_id = $1`, docID)
	if err != nil {
		t.Fatalf("query flag: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return false, "", false
	}
	if err := rows.Scan(&flagged, &reason); err != nil {
		t.Fatalf("scan flag: %v", err)
	}
	return flagged, reason, true
}

func matQty(t *testing.T, owner *postgres.Adapter, docID string) (int64, bool) {
	t.Helper()
	rows, err := owner.Driver().Query(context.Background(),
		`SELECT qty FROM `+docTypeMatTable+` WHERE id = $1`, docID)
	if err != nil {
		t.Fatalf("query row: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, false
	}
	var qty int64
	if err := rows.Scan(&qty); err != nil {
		t.Fatalf("scan qty: %v", err)
	}
	return qty, true
}

// A ColInt column fed a non-integral number flags the document, not the row.
func TestDocMaterialize_TypeMismatchFlags(t *testing.T) {
	owner, a := newDocTypeHarness(t, false)
	docs := a.Documents()
	ctx := scopedCtx(t, "ws_1", "")
	docID := "twidget/" + event.NewID()

	if err := docs.ApplyUpdate(ctx, docID, crdtLWWUpdate(t, docTypeMatTable, docID, "title", "a", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate title: %v", err)
	}
	if err := docs.ApplyUpdate(ctx, docID, crdtLWWUpdate(t, docTypeMatTable, docID, "qty", 3.5, 101, "n1")); err != nil {
		t.Fatalf("ApplyUpdate qty: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	n, err := docs.MaterializeQuiet(context.Background(), nil)
	if err != nil {
		t.Fatalf("MaterializeQuiet: %v", err)
	}
	if n != 0 {
		t.Fatalf("materialized %d, want 0 (should be flagged)", n)
	}
	flagged, reason, found := docFlag(t, owner, docID)
	if !found || !flagged {
		t.Fatalf("doc must be flagged; found=%v flagged=%v", found, flagged)
	}
	if !strings.Contains(reason, "qty") {
		t.Fatalf("flag_reason should name qty, got %q", reason)
	}
	if _, ok := matQty(t, owner, docID); ok {
		t.Fatalf("no row should be materialized for a flagged doc")
	}
}

// A ColInt column fed a JSON number materializes with a canonical int64.
func TestDocMaterialize_CoercesNumeric(t *testing.T) {
	owner, a := newDocTypeHarness(t, false)
	docs := a.Documents()
	ctx := scopedCtx(t, "ws_1", "")
	docID := "twidget/" + event.NewID()

	if err := docs.ApplyUpdate(ctx, docID, crdtLWWUpdate(t, docTypeMatTable, docID, "title", "a", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate title: %v", err)
	}
	if err := docs.ApplyUpdate(ctx, docID, crdtLWWUpdate(t, docTypeMatTable, docID, "qty", 3, 101, "n1")); err != nil {
		t.Fatalf("ApplyUpdate qty: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	n, err := docs.MaterializeQuiet(context.Background(), nil)
	if err != nil {
		t.Fatalf("MaterializeQuiet: %v", err)
	}
	if n != 1 {
		t.Fatalf("materialized %d, want 1", n)
	}
	if flagged, _, _ := docFlag(t, owner, docID); flagged {
		t.Fatalf("doc must not be flagged")
	}
	qty, ok := matQty(t, owner, docID)
	if !ok || qty != 3 {
		t.Fatalf("materialized qty = %v (ok=%v), want int64(3)", qty, ok)
	}
}

// NoTypeCheck bypasses coercion: the same non-integral number that flagged above
// now materializes (the DB assignment-casts it), and the doc is NOT flagged.
func TestDocMaterialize_NoTypeCheckBypass(t *testing.T) {
	owner, a := newDocTypeHarness(t, true) // NoTypeCheck: true
	docs := a.Documents()
	ctx := scopedCtx(t, "ws_1", "")
	docID := "twidget/" + event.NewID()

	if err := docs.ApplyUpdate(ctx, docID, crdtLWWUpdate(t, docTypeMatTable, docID, "title", "a", 100, "n1")); err != nil {
		t.Fatalf("ApplyUpdate title: %v", err)
	}
	if err := docs.ApplyUpdate(ctx, docID, crdtLWWUpdate(t, docTypeMatTable, docID, "qty", 3.5, 101, "n1")); err != nil {
		t.Fatalf("ApplyUpdate qty: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	n, err := docs.MaterializeQuiet(context.Background(), nil)
	if err != nil {
		t.Fatalf("MaterializeQuiet: %v", err)
	}
	if n != 1 {
		t.Fatalf("materialized %d, want 1 (NoTypeCheck bypasses coercion)", n)
	}
	if flagged, _, _ := docFlag(t, owner, docID); flagged {
		t.Fatalf("doc must NOT be flagged when NoTypeCheck is set")
	}
}

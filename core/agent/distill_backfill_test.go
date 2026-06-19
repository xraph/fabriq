package agent

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

// seedNote creates a note row through the command plane using a typed *tDoc
// payload. tDoc has fields ID, TenantID, Version, Title, Body — no site column.
func seedNoteViaFab(t testing.TB, fab query.Fabric, ctx context.Context, id, title, body string) {
	t.Helper()
	if _, err := fab.Exec(ctx, command.Command{
		Entity:  "note",
		Op:      command.OpCreate,
		AggID:   id,
		Payload: &tDoc{Title: title, Body: body},
	}); err != nil {
		t.Fatalf("seed note %s: %v", id, err)
	}
}

// TestDistill_BackfillFromExistingRows verifies that Distill pages through all
// distillable entity rows for the current tenant, calls DistillL0 per row, then
// Rollup once, producing at least as many Built nodes as seeded rows and leaving
// a tenant root in the tree.
func TestDistill_BackfillFromExistingRows(t *testing.T) {
	r := distillRegistry(t)
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	ctx := testCtx(t, "acme")

	// Seed rows directly via the command plane (no distill worker running).
	seedNoteViaFab(t, fab, ctx, "n1", "Pump", "ok")
	seedNoteViaFab(t, fab, ctx, "n2", "Valve", "leak")

	cas := fabriqtest.NewFakeCAS()
	d, err := NewDistiller(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, &fakeSummarizer{}, nil, cas, DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}

	rep, err := d.Distill(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Built < 2 {
		t.Fatalf("expected >=2 L0 nodes built, got %+v", rep)
	}
	if _, ok, err := d.getNode(ctx, TenantRootID()); err != nil {
		t.Fatalf("getNode tenant root: %v", err)
	} else if !ok {
		t.Fatal("backfill must produce a tenant root")
	}
}

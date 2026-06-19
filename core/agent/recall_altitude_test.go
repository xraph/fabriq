package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/fabriqtest"
)

// packEntities returns the set of distinct entity names present in a pack's items.
func packEntities(p ContextPack) map[string]int {
	out := map[string]int{}
	for _, it := range p.Items {
		out[it.Entity]++
	}
	return out
}

// noteTokens builds the pack once at a huge budget and returns the token count
// of the (single) note item, so the test can pick budget thresholds robustly.
func noteTokens(t *testing.T, tk *Toolkit, ctx context.Context) int {
	t.Helper()
	pack, err := tk.Recall(ctx, RecallRequest{
		Query: "pump", Budget: 1 << 20, Entities: []string{"note"}, Altitude: AltAuto,
	})
	if err != nil {
		t.Fatalf("probe recall: %v", err)
	}
	for _, it := range pack.Items {
		if it.Entity == "note" {
			return it.Tokens
		}
	}
	t.Fatalf("probe pack had no note item; entities=%v", packEntities(pack))
	return 0
}

// TestRecall_AltitudeAuto_BudgetDrivesLayer verifies that with AltAuto the token
// budget selects the layer: a generous budget descends to the source entity
// (note), a tight budget climbs to the tenant digest — never both.
func TestRecall_AltitudeAuto_BudgetDrivesLayer(t *testing.T) {
	r := distillRegistry(t)
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	ctx := testCtx(t, "acme")

	// A long body makes the note row's token count exceed any single digest
	// row's (digest_node has many fixed columns but its summary lives in CAS, so
	// its row token count is small and body-independent). That ordering is what
	// makes the tight-budget threshold deterministic: there is a budget that is
	// below the note's tokens (→ climb to the tenant digest) yet still affords a
	// digest row.
	body := "vibration " + strings.Repeat("vibration ", 100)

	// Seed a note row + its vector.
	if _, err := fab.Exec(ctx, command.Command{
		Entity: "note", Op: command.OpCreate, AggID: "n1",
		Payload: &tDoc{Title: "Pump", Body: body},
	}); err != nil {
		t.Fatalf("seed note: %v", err)
	}
	if err := w.Vector.Upsert(ctx, "note", "n1", []float32{1, 0, 0}, nil); err != nil {
		t.Fatalf("seed note vector: %v", err)
	}

	// Build the digest tree so digest_node rows + vectors exist (L0 + tenant root),
	// using the same stub vector so digests rank alongside the note.
	d, err := NewDistiller(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, &fakeSummarizer{}, nil, cas, DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatalf("new distiller: %v", err)
	}
	if _, err := d.DistillL0(ctx, "note", "n1", map[string]any{"id": "n1", "title": "Pump", "body": body}); err != nil {
		t.Fatalf("distill L0: %v", err)
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatalf("rollup: %v", err)
	}

	tk, err := NewToolkit(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3})
	if err != nil {
		t.Fatalf("new toolkit: %v", err)
	}

	nTok := noteTokens(t, tk, ctx)

	// Generous budget → AltEntity: pack has the note, no digest_node.
	generous, err := tk.Recall(ctx, RecallRequest{
		Query: "pump", Budget: 100000, Entities: []string{"note"}, Altitude: AltAuto,
	})
	if err != nil {
		t.Fatalf("generous recall: %v", err)
	}
	ge := packEntities(generous)
	if ge["note"] == 0 {
		t.Fatalf("generous budget must surface a note item; entities=%v warnings=%v", ge, generous.Warnings)
	}
	if ge[DigestEntity] != 0 {
		t.Fatalf("generous budget must NOT surface digest_node; entities=%v", ge)
	}

	// Tight budget (< the note's token count) → AltTenant: pack climbs to a
	// digest, no note. Use note-tokens-1 so resolveAltitude picks AltTenant
	// (entityTokens=nTok > budget) while still affording the (smaller) digest row.
	tight, err := tk.Recall(ctx, RecallRequest{
		Query: "pump", Budget: nTok - 1, Entities: []string{"note"}, Altitude: AltAuto,
	})
	if err != nil {
		t.Fatalf("tight recall: %v", err)
	}
	tg := packEntities(tight)
	if tg["note"] != 0 {
		t.Fatalf("tight budget must NOT surface a note item; entities=%v", tg)
	}
	if tg[DigestEntity] == 0 {
		t.Fatalf("tight budget must surface a digest_node item; entities=%v warnings=%v", tg, tight.Warnings)
	}
}

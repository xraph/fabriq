package agent

import (
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

// TestExactDedup_ReusesVector: a 2nd byte-identical source reuses the donor's
// vector — ZERO embed calls — while still getting its own vector row.
func TestExactDedup_ReusesVector(t *testing.T) {
	r := distillRegistry(t)
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	emb := &countingEmbedder{}
	d, err := NewDistiller(fab, r, emb, &fakeSummarizer{}, nil, cas, DistillConfig{VectorDims: 3, RecipeVersion: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t)
	src := func(id string) map[string]any {
		return map[string]any{"id": id, "title": "Same Pump", "body": "same body", "site_id": "s1"}
	}
	if _, err := d.DistillL0(ctx, "note", "a", src("a")); err != nil {
		t.Fatal(err)
	}
	embAfterFirst := emb.calls
	if _, err := d.DistillL0(ctx, "note", "b", src("b")); err != nil {
		t.Fatal(err)
	}
	if emb.calls != embAfterFirst {
		t.Fatalf("dedup must reuse donor vector (no embed); calls %d→%d", embAfterFirst, emb.calls)
	}
	// b still has its own vector.
	if _, err := w.Vector.Get(ctx, DigestEntity, L0ID("note", "b")); err != nil {
		t.Fatalf("deduped node must still have a vector: %v", err)
	}
}

// TestPersist_PopulatesTokens asserts a built L0 node stores a positive token
// count (so the adaptive-depth fit-check can read it from the row, not CAS).
func TestPersist_PopulatesTokens(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	d, _ := newDistiller(t, r, cas, &fakeSummarizer{}, nil)
	ctx := testCtx(t)

	if _, err := d.DistillL0(ctx, "note", "n1", map[string]any{"id": "n1", "title": "Pump A vibration warning", "body": "bearing", "site_id": "s1"}); err != nil {
		t.Fatal(err)
	}
	row, ok, err := d.getNode(ctx, L0ID("note", "n1"))
	if err != nil || !ok {
		t.Fatalf("node missing: ok=%v err=%v", ok, err)
	}
	if row.Tokens <= 0 {
		t.Fatalf("expected positive token count, got %d", row.Tokens)
	}
}

// TestTokenize_Default counts whitespace-delimited words by default.
func TestTokenize_Default(t *testing.T) {
	r := distillRegistry(t)
	d, _ := newDistiller(t, r, fabriqtest.NewFakeCAS(), &fakeSummarizer{}, nil)
	if n := d.tokenize("one two three"); n != 3 {
		t.Fatalf("default tokenize counts words; want 3 got %d", n)
	}
}

// TestExactDedup_ReusesSummary asserts two byte-identical sources trigger exactly
// one Summarize call (the second reuses the first's summary blob).
func TestExactDedup_ReusesSummary(t *testing.T) {
	r := distillRegistry(t)
	cas := fabriqtest.NewFakeCAS()
	sum := &fakeSummarizer{}
	d, _ := newDistiller(t, r, cas, sum, nil)
	ctx := testCtx(t)

	same := map[string]any{"title": "Identical Pump", "body": "identical body text", "site_id": "s1"}
	a := map[string]any{"id": "a"}
	b := map[string]any{"id": "b"}
	for k, v := range same {
		a[k] = v
		b[k] = v
	}
	if _, err := d.DistillL0(ctx, "note", "a", a); err != nil {
		t.Fatal(err)
	}
	callsAfterFirst := sum.calls
	if _, err := d.DistillL0(ctx, "note", "b", b); err != nil {
		t.Fatal(err)
	}
	if sum.calls != callsAfterFirst {
		t.Fatalf("identical source must reuse the summary (no new Summarize); calls %d→%d", callsAfterFirst, sum.calls)
	}
	// Both nodes exist and share the same summary blob.
	ra, _, _ := d.getNode(ctx, L0ID("note", "a"))
	rb, _, _ := d.getNode(ctx, L0ID("note", "b"))
	if ra.SummaryHash == "" || ra.SummaryHash != rb.SummaryHash {
		t.Fatalf("deduped nodes must share a SummaryHash; a=%q b=%q", ra.SummaryHash, rb.SummaryHash)
	}
}

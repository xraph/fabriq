package agent

import (
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

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

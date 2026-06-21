package agent

import (
	"context"
	"io"
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

// countingCAS wraps the fake CAS to count Retrieve calls. Store and every other
// blob.CAS method are promoted from the embedded *fabriqtest.FakeCAS.
type countingCAS struct {
	*fabriqtest.FakeCAS
	retrieves int
}

func (c *countingCAS) Retrieve(ctx context.Context, hash string) (io.ReadCloser, error) {
	c.retrieves++
	return c.FakeCAS.Retrieve(ctx, hash)
}

// TestRollup_NoCASReadOnShortCircuit asserts an unchanged rollup retrieves zero
// summaries from CAS (the Merkle short-circuit must not pay for CAS I/O).
func TestRollup_NoCASReadOnShortCircuit(t *testing.T) {
	r := distillRegistry(t)
	cas := &countingCAS{FakeCAS: fabriqtest.NewFakeCAS()}
	sum := &fakeSummarizer{}
	d, _ := newDistiller(t, r, cas, sum, nil)
	ctx := testCtx(t)

	for _, n := range []map[string]any{
		{"id": "n1", "title": "Pump A", "body": "ok", "site_id": "s1"},
		{"id": "n2", "title": "Pump B", "body": "warn", "site_id": "s1"},
	} {
		if _, err := d.DistillL0(ctx, "note", n["id"].(string), n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}

	// Second rollup: nothing changed → every internal node short-circuits → no
	// CAS retrieve should happen.
	cas.retrieves = 0
	if _, err := d.Rollup(ctx); err != nil {
		t.Fatal(err)
	}
	if cas.retrieves != 0 {
		t.Fatalf("unchanged rollup must not read CAS; got %d retrieves", cas.retrieves)
	}
}

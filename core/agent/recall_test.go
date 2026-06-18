// core/agent/recall_test.go
package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/fabriqtest"
)

// stubEmbedder returns a fixed vector so recall ordering is reproducible.
type stubEmbedder struct {
	dims int
	vec  []float32
}

func (s stubEmbedder) Dims() int { return s.dims }
func (s stubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = s.vec
	}
	return out, nil
}

func TestRecall_VectorChannelReturnsHydratedPack(t *testing.T) {
	reg := testRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, err := NewToolkit(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t, "acme")

	res, err := ff.Exec(ctx, command.Command{Entity: "doc", Op: command.OpCreate, Payload: &tDoc{Title: "Near", Body: "match"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Vector.Upsert(ctx, "doc", res.AggID, []float32{1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}

	pack, err := tk.Recall(ctx, RecallRequest{Query: "anything", Budget: 10000, Entities: []string{"doc"}})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(pack.Items) != 1 {
		t.Fatalf("want 1 item, got %d (warnings: %v)", len(pack.Items), pack.Warnings)
	}
	if pack.Items[0].ID != res.AggID {
		t.Fatalf("want id %q, got %q", res.AggID, pack.Items[0].ID)
	}
	if len(pack.Items[0].Source) == 0 || pack.Items[0].Source[0] != "vector" {
		t.Fatalf("want vector provenance, got %v", pack.Items[0].Source)
	}
}

func TestRecall_NoEmbedderDegradesWithWarning(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, _ := NewToolkit(ff, reg, nil, Config{})
	ctx := testCtx(t, "acme")

	pack, err := tk.Recall(ctx, RecallRequest{Query: "x", Budget: 100, Entities: []string{"doc"}})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(pack.Items) != 0 {
		t.Fatalf("want 0 items, got %d", len(pack.Items))
	}
	if len(pack.Warnings) == 0 {
		t.Fatal("want a degradation warning")
	}
}

// errEmbedder always returns an error from Embed; used by the Strict-mode test.
type errEmbedder struct{ dims int }

func (e errEmbedder) Dims() int { return e.dims }
func (e errEmbedder) Embed(_ context.Context, _ []string) ([][]float32, error) {
	return nil, errors.New("boom")
}

func TestRecall_StrictReturnsErrorOnChannelFailure(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, err := NewToolkit(ff, reg, errEmbedder{dims: 3}, Config{Strict: true, VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx := testCtx(t, "acme")

	_, recallErr := tk.Recall(ctx, RecallRequest{Query: "anything", Budget: 10000, Entities: []string{"doc"}})
	if recallErr == nil {
		t.Fatal("want non-nil error in Strict mode when embedder fails, got nil")
	}
}

func TestRecall_ValidatesInput(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, _ := NewToolkit(ff, reg, nil, Config{})
	ctx := testCtx(t, "acme")

	if _, err := tk.Recall(ctx, RecallRequest{Budget: 100, Entities: []string{"doc"}}); err == nil {
		t.Fatal("want error for empty query")
	}
	if _, err := tk.Recall(ctx, RecallRequest{Query: "x", Entities: []string{"doc"}}); err == nil {
		t.Fatal("want error for non-positive budget")
	}
	if _, err := tk.Recall(ctx, RecallRequest{Query: "x", Budget: 100}); err == nil {
		t.Fatal("want error for empty entities")
	}
}

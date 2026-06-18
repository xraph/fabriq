// core/agent/tools_test.go
package agent

import (
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestTools_RecallDispatch(t *testing.T) {
	reg := testRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, _ := NewToolkit(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3})
	ctx := testCtx(t, "acme")

	res, _ := ff.Exec(ctx, command.Command{Entity: "doc", Op: command.OpCreate, Payload: &tDoc{Title: "T", Body: "B"}})
	_ = w.Vector.Upsert(ctx, "doc", res.AggID, []float32{1, 0, 0}, nil)

	tools := tk.Tools()
	if len(tools) != 1 || tools[0].Name != "recall" {
		t.Fatalf("want one recall tool, got %+v", tools)
	}
	var schema map[string]any
	if err := json.Unmarshal(tools[0].InputSchema, &schema); err != nil {
		t.Fatalf("invalid input schema: %v", err)
	}
	out, err := tools[0].Handler(ctx, json.RawMessage(`{"query":"x","budget":10000,"entities":["doc"]}`))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	pack, ok := out.(ContextPack)
	if !ok {
		t.Fatalf("want ContextPack, got %T", out)
	}
	if len(pack.Items) != 1 {
		t.Fatalf("want 1 item, got %d", len(pack.Items))
	}
}

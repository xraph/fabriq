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

	res, err := ff.Exec(ctx, command.Command{Entity: "doc", Op: command.OpCreate, Payload: &tDoc{Title: "T", Body: "B"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Vector.Upsert(ctx, "doc", res.AggID, []float32{1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}

	tools := tk.Tools()
	recallTool, ok := toolByName(tools, "recall")
	if !ok {
		t.Fatalf("recall tool missing from %+v", tools)
	}
	var schema map[string]any
	if err := json.Unmarshal(recallTool.InputSchema, &schema); err != nil {
		t.Fatalf("invalid input schema: %v", err)
	}
	out, err := recallTool.Handler(ctx, json.RawMessage(`{"query":"x","budget":10000,"entities":["doc"]}`))
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

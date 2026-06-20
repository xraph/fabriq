// core/agent/tools_primitives_test.go
package agent

import (
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/fabriqtest"
)

func toolByName(tools []Tool, name string) (Tool, bool) {
	for _, t := range tools {
		if t.Name == name {
			return t, true
		}
	}
	return Tool{}, false
}

func TestTools_ListsAllPrimitives(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, _ := NewToolkit(ff, reg, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{VectorDims: 3})

	tools := tk.Tools()
	if len(tools) != 9 {
		t.Fatalf("want 9 tools, got %d", len(tools))
	}
	for _, name := range []string{"recall", "vector_similar", "search", "graph_traverse", "get", "remember", "map", "digest", "resolve"} {
		tl, ok := toolByName(tools, name)
		if !ok {
			t.Fatalf("missing tool %q", name)
		}
		var schema map[string]any
		if err := json.Unmarshal(tl.InputSchema, &schema); err != nil {
			t.Fatalf("tool %q invalid schema: %v", name, err)
		}
	}
}

func TestTools_GetDispatch(t *testing.T) {
	reg := testRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, _ := NewToolkit(ff, reg, nil, Config{})
	ctx := testCtx(t, "acme")

	res, err := ff.Exec(ctx, command.Command{Entity: "doc", Op: command.OpCreate, Payload: &tDoc{Title: "T", Body: "B"}})
	if err != nil {
		t.Fatal(err)
	}
	getTool, _ := toolByName(tk.Tools(), "get")
	out, err := getTool.Handler(ctx, json.RawMessage(`{"entity":"doc","id":"`+res.AggID+`"}`))
	if err != nil {
		t.Fatalf("get handler: %v", err)
	}
	raw, ok := out.(json.RawMessage)
	if !ok {
		t.Fatalf("want json.RawMessage, got %T", out)
	}
	var row tDoc
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	if row.Title != "T" {
		t.Fatalf("want Title T, got %q", row.Title)
	}
}

func TestTools_SearchDispatch(t *testing.T) {
	reg := testRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, _ := NewToolkit(ff, reg, nil, Config{})
	ctx := testCtx(t, "acme")

	res, err := ff.Exec(ctx, command.Command{Entity: "doc", Op: command.OpCreate, Payload: &tDoc{Title: "alpha", Body: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Search.ApplyMutations(ctx, "docs", []projection.Mutation{
		projection.DocIndex{Index: "docs", ID: res.AggID, Version: 1, Doc: map[string]any{
			"id": res.AggID, "tenant_id": "acme", "title": "alpha", "body": "b",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	searchTool, _ := toolByName(tk.Tools(), "search")
	out, err := searchTool.Handler(ctx, json.RawMessage(`{"entity":"doc","query":"alpha","limit":10}`))
	if err != nil {
		t.Fatalf("search handler: %v", err)
	}
	hits, ok := out.([]map[string]any)
	if !ok {
		t.Fatalf("want []map[string]any, got %T", out)
	}
	if len(hits) != 1 || hits[0]["id"] != res.AggID {
		t.Fatalf("want 1 hit %q, got %+v", res.AggID, hits)
	}
}

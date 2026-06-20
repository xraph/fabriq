// core/agent/tools_distill_test.go
package agent

import (
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

// TestTools_IncludesDistillTools verifies that Tools() exposes the three
// context-distillation tools: map, digest, resolve. It also does a
// round-trip smoke test on each Handler to confirm they don't panic on
// minimal input against an empty tree.
func TestTools_IncludesDistillTools(t *testing.T) {
	r := distillRegistry(t)
	w := fabriqtest.NewWorld(r)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()

	tk, err := NewToolkit(fab, r, stubEmbedder{dims: 3, vec: []float32{1, 0, 0}}, Config{
		VectorDims: 3,
		CAS:        cas,
	})
	if err != nil {
		t.Fatal(err)
	}

	// --- assert all three distill tools are present in Tools() ---
	names := map[string]bool{}
	for _, tl := range tk.Tools() {
		names[tl.Name] = true
	}
	for _, want := range []string{"map", "digest", "resolve"} {
		if !names[want] {
			t.Fatalf("Tools() missing %q; present: %v", want, names)
		}
	}

	ctx := testCtx(t)

	// --- map: empty args must not panic and must return a nil/empty slice (no tree seeded) ---
	mapTool, ok := toolByName(tk.Tools(), "map")
	if !ok {
		t.Fatal("map tool not found")
	}
	var mapSchema map[string]any
	if err = json.Unmarshal(mapTool.InputSchema, &mapSchema); err != nil {
		t.Fatalf("map: invalid InputSchema: %v", err)
	}
	out, err := mapTool.Handler(ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("map handler with empty tree: %v", err)
	}
	if out != nil {
		lines, isLines := out.([]MapLine)
		if !isLines {
			t.Fatalf("map returned %T, want []MapLine or nil", out)
		}
		if len(lines) != 0 {
			t.Fatalf("expected empty lines on empty tree, got %d", len(lines))
		}
	}

	// --- map: nil args must also be tolerated (no required fields) ---
	out2, err := mapTool.Handler(ctx, json.RawMessage(nil))
	if err != nil {
		t.Fatalf("map handler with nil args: %v", err)
	}
	_ = out2

	// --- digest: bad args (missing nodeId) must return a proper error, not panic ---
	digestTool, ok := toolByName(tk.Tools(), "digest")
	if !ok {
		t.Fatal("digest tool not found")
	}
	var digestSchema map[string]any
	if err = json.Unmarshal(digestTool.InputSchema, &digestSchema); err != nil {
		t.Fatalf("digest: invalid InputSchema: %v", err)
	}
	_, err = digestTool.Handler(ctx, json.RawMessage(`{"nodeId":"nonexistent"}`))
	if err == nil {
		t.Fatal("digest: expected error for nonexistent node")
	}

	// --- resolve: round-trip with a hash that matches nothing ---
	resolveTool, ok := toolByName(tk.Tools(), "resolve")
	if !ok {
		t.Fatal("resolve tool not found")
	}
	var resolveSchema map[string]any
	if err = json.Unmarshal(resolveTool.InputSchema, &resolveSchema); err != nil {
		t.Fatalf("resolve: invalid InputSchema: %v", err)
	}
	out3, err := resolveTool.Handler(ctx, json.RawMessage(`{"hash":"deadbeef"}`))
	if err != nil {
		t.Fatalf("resolve handler: %v", err)
	}
	_, ok = out3.(ResolveResult)
	if !ok {
		t.Fatalf("resolve returned %T, want ResolveResult", out3)
	}
}

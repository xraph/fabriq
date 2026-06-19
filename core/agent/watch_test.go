// core/agent/watch_test.go
package agent

import (
	"testing"
	"time"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestWatch_DeliversDeltasAndPassesScope(t *testing.T) {
	reg := testRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, _ := NewToolkit(ff, reg, nil, Config{})
	ctx := testCtx(t, "acme")

	scope := query.SubscribeScope{Entity: "doc", Scope: "tenant"}
	ch, err := tk.Watch(ctx, scope)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if ff.lastSubscribeScope != scope {
		t.Fatalf("Watch passed wrong scope: %+v", ff.lastSubscribeScope)
	}

	want := query.Delta{Aggregate: "doc", AggID: "d1", Version: 1, Type: "doc.created"}
	ff.pushDelta(want)
	select {
	case got := <-ch:
		if got.AggID != "d1" || got.Type != "doc.created" {
			t.Fatalf("unexpected delta %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delta")
	}
}

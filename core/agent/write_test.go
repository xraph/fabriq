// core/agent/write_test.go
package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/fabriqtest"
)

func newFakeFabricWithHook(t testing.TB, w *fabriqtest.World, hook command.LifecycleHook) *fakeFabric {
	t.Helper()
	x, err := command.NewExecutor(w.Registry, w.Store, command.WithHooks(hook))
	if err != nil {
		t.Fatal(err)
	}
	return &fakeFabric{w: w, x: x}
}

func writePolicy() WritePolicy {
	return WritePolicy{Allow: map[string][]command.Op{
		"doc": {command.OpCreate, command.OpUpdate},
	}}
}

func TestRemember_DeniedByDefault(t *testing.T) {
	reg := testRegistry(t) // "doc" searchable typed entity
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, _ := NewToolkit(ff, reg, nil, Config{}) // empty WritePolicy → deny all
	ctx := testCtx(t, "acme")

	_, err := tk.Remember(ctx, RememberRequest{Entity: "doc", Op: "create", Payload: []byte(`{"title":"x"}`)})
	var we *WriteError
	if !errors.As(err, &we) || we.Code != "not_allowed" {
		t.Fatalf("want WriteError not_allowed, got %v", err)
	}
}

func TestRemember_AllowedCreateTyped(t *testing.T) {
	reg := testRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	tk, _ := NewToolkit(ff, reg, nil, Config{Write: writePolicy()})
	ctx := testCtx(t, "acme")

	res, err := tk.Remember(ctx, RememberRequest{Entity: "doc", Op: "create", Payload: []byte(`{"title":"hello","body":"world"}`)})
	if err != nil {
		t.Fatalf("Remember: %v", err)
	}
	if res.AggID == "" || res.Version != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
	// the row is readable
	var got tDoc
	if err := w.Rel.Get(ctx, "doc", res.AggID, &got); err != nil {
		t.Fatalf("get written row: %v", err)
	}
	if got.Title != "hello" {
		t.Fatalf("want Title hello, got %q", got.Title)
	}
}

func TestRemember_InvalidOp(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, _ := NewToolkit(ff, reg, nil, Config{Write: writePolicy()})
	ctx := testCtx(t, "acme")
	_, err := tk.Remember(ctx, RememberRequest{Entity: "doc", Op: "frobnicate", Payload: []byte(`{}`)})
	var we *WriteError
	if !errors.As(err, &we) || we.Code != "validation_failed" {
		t.Fatalf("want validation_failed, got %v", err)
	}
}

func TestRemember_VersionConflict(t *testing.T) {
	reg := testRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabric(t, w)
	pol := WritePolicy{Allow: map[string][]command.Op{"doc": {command.OpCreate, command.OpUpdate}}}
	tk, _ := NewToolkit(ff, reg, nil, Config{Write: pol})
	ctx := testCtx(t, "acme")

	res, err := tk.Remember(ctx, RememberRequest{Entity: "doc", Op: "create", Payload: []byte(`{"title":"v1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	stale := int64(99)
	_, err = tk.Remember(ctx, RememberRequest{Entity: "doc", Op: "update", AggID: res.AggID, ExpectedVersion: &stale, Payload: []byte(`{"title":"v2"}`)})
	var we *WriteError
	if !errors.As(err, &we) || we.Code != "version_conflict" {
		t.Fatalf("want version_conflict, got %v", err)
	}
	if !errors.Is(err, fabriqerr.ErrVersionConflict) {
		t.Fatalf("want wrapped ErrVersionConflict, got %v", err)
	}
}

func TestRemember_UnknownEntity(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	// policy lists "ghost" — but "ghost" is not registered; unknown-entity check fires first.
	pol := WritePolicy{Allow: map[string][]command.Op{
		"ghost": {command.OpCreate},
	}}
	tk, _ := NewToolkit(ff, reg, nil, Config{Write: pol})
	ctx := testCtx(t, "acme")

	_, err := tk.Remember(ctx, RememberRequest{Entity: "ghost", Op: "create", Payload: []byte(`{"x":1}`)})
	var we *WriteError
	if !errors.As(err, &we) || we.Code != "validation_failed" {
		t.Fatalf("want WriteError validation_failed for unknown entity, got %v", err)
	}
}

func TestRemember_EmptyPayload(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, _ := NewToolkit(ff, reg, nil, Config{Write: writePolicy()})
	ctx := testCtx(t, "acme")

	_, err := tk.Remember(ctx, RememberRequest{Entity: "doc", Op: "create"}) // Payload is nil/empty
	var we *WriteError
	if !errors.As(err, &we) || we.Code != "validation_failed" {
		t.Fatalf("want WriteError validation_failed for empty payload, got %v", err)
	}
}

func TestRemember_LifecycleHookVeto(t *testing.T) {
	reg := testRegistry(t)
	w := fabriqtest.NewWorld(reg)
	ff := newFakeFabricWithHook(t, w, command.HookFunc(func(_ context.Context, _ command.Tx, _ command.Change) error {
		return errors.New("vetoed by policy")
	}))
	tk, _ := NewToolkit(ff, reg, nil, Config{Write: writePolicy()})
	ctx := testCtx(t, "acme")

	_, err := tk.Remember(ctx, RememberRequest{Entity: "doc", Op: "create", Payload: []byte(`{"title":"x"}`)})
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("want WriteError, got %v", err)
	}
	if we.Code != "exec_failed" {
		t.Fatalf("want exec_failed for hook veto, got %q", we.Code)
	}
}

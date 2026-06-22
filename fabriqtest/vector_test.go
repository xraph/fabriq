package fabriqtest_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestFakeVector_SimilarFilter(t *testing.T) {
	r := registry.New()
	w := fabriqtest.NewWorld(r)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Vector.Upsert(ctx, "doc", "a", []float32{1, 0, 0}, map[string]any{"kind": "note"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Vector.Upsert(ctx, "doc", "b", []float32{1, 0, 0}, map[string]any{"kind": "task"}); err != nil {
		t.Fatal(err)
	}
	var got []query.VectorMatch
	if err := w.Vector.Similar(ctx, query.VectorQuery{Entity: "doc", Embedding: []float32{1, 0, 0}, K: 10, Filter: map[string]string{"kind": "note"}}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("filtered Similar = %+v, want only id=a", got)
	}
}

func TestFakeVector_Delete(t *testing.T) {
	r := registry.New()
	w := fabriqtest.NewWorld(r)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Vector.Upsert(ctx, "doc", "d1", []float32{1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	// present
	var before []query.VectorMatch
	if err := w.Vector.Similar(ctx, query.VectorQuery{Entity: "doc", Embedding: []float32{1, 0, 0}, K: 10}, &before); err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 {
		t.Fatalf("want 1 before delete, got %d", len(before))
	}
	// delete
	if err := w.Vector.Delete(ctx, "doc", "d1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	var after []query.VectorMatch
	if err := w.Vector.Similar(ctx, query.VectorQuery{Entity: "doc", Embedding: []float32{1, 0, 0}, K: 10}, &after); err != nil {
		t.Fatal(err)
	}
	if len(after) != 0 {
		t.Fatalf("want 0 after delete, got %d", len(after))
	}
	// deleting a missing id is a no-op (no error)
	if err := w.Vector.Delete(ctx, "doc", "nope"); err != nil {
		t.Fatalf("delete missing id should be no-op, got %v", err)
	}
}

func TestFakeVector_DeleteByMeta(t *testing.T) {
	r := registry.New()
	w := fabriqtest.NewWorld(r)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Vector.Upsert(ctx, "doc", "a", []float32{1, 0, 0}, map[string]any{"kind": "note"})
	_ = w.Vector.Upsert(ctx, "doc", "b", []float32{1, 0, 0}, map[string]any{"kind": "task"})

	if err := w.Vector.DeleteByMeta(ctx, "doc", map[string]string{"kind": "note"}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Vector.Get(ctx, "doc", "a"); err == nil {
		t.Fatalf("id a should be deleted")
	}
	if _, err := w.Vector.Get(ctx, "doc", "b"); err != nil {
		t.Fatalf("id b should survive: %v", err)
	}
}

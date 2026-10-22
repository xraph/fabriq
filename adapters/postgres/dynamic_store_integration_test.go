//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestDynamicSpecStore_SaveLoadDelete pins the fabriq_dynamic_entity store:
// the revision only moves when the stored spec changes, load is ordered by
// name, and deleting an unknown name is not an error.
func TestDynamicSpecStore_SaveLoadDelete(t *testing.T) {
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	a, err := postgres.Open(ctx, superDSN, registry.New())
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	orch, err := migrations.NewOrchestrator(a.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	save := func(name, spec string, want int64) {
		t.Helper()
		rev, err := a.SaveDynamicSpec(ctx, name, []byte(spec))
		if err != nil {
			t.Fatalf("SaveDynamicSpec(%s): %v", name, err)
		}
		if rev != want {
			t.Fatalf("SaveDynamicSpec(%s, %s): revision = %d, want %d", name, spec, rev, want)
		}
	}

	save("widget", `{"format":1,"name":"widget"}`, 1)
	// Unchanged spec: the revision must not move.
	save("widget", `{"format":1,"name":"widget"}`, 1)
	// Same JSON with different key order and whitespace is still unchanged.
	save("widget", `{ "name": "widget", "format": 1 }`, 1)
	// Changed spec: the revision moves.
	const last = `{"format":1,"name":"widget","x":1}`
	save("widget", last, 2)

	recs, err := a.LoadDynamicSpecs(ctx)
	if err != nil {
		t.Fatalf("LoadDynamicSpecs: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("LoadDynamicSpecs: got %d records, want 1", len(recs))
	}
	if recs[0].Name != "widget" || recs[0].Revision != 2 {
		t.Fatalf("record = %s rev %d, want widget rev 2", recs[0].Name, recs[0].Revision)
	}
	var got, want any
	if err := json.Unmarshal(recs[0].Spec, &got); err != nil {
		t.Fatalf("stored spec is not JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(last), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stored spec = %s, want JSON-equal to %s", recs[0].Spec, last)
	}

	// Ordering: records come back sorted by name.
	save("alpha", `{"format":1,"name":"alpha"}`, 1)
	recs, err = a.LoadDynamicSpecs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].Name != "alpha" || recs[1].Name != "widget" {
		t.Fatalf("LoadDynamicSpecs order = %+v, want [alpha widget]", recs)
	}

	if err := a.DeleteDynamicSpec(ctx, "alpha"); err != nil {
		t.Fatalf("DeleteDynamicSpec(alpha): %v", err)
	}
	if err := a.DeleteDynamicSpec(ctx, "widget"); err != nil {
		t.Fatalf("DeleteDynamicSpec(widget): %v", err)
	}
	recs, err = a.LoadDynamicSpecs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("after delete: got %d records, want 0", len(recs))
	}

	// Deleting a name that was never saved is a no-op.
	if err := a.DeleteDynamicSpec(ctx, "missing"); err != nil {
		t.Fatalf("DeleteDynamicSpec(missing) = %v, want nil", err)
	}

	// A re-created type starts over at revision 1.
	save("widget", `{"format":1,"name":"widget"}`, 1)
}

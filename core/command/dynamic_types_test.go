package command_test

import (
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
)

// typedRegistry registers a dynamic entity whose qty is ColInt so type checks
// can be exercised. skipEntity toggles the per-entity escape hatch.
func typedRegistry(t testing.TB, skipEntity bool) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "widgets", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table:       "ds_widgets",
			NoTypeCheck: skipEntity,
			Columns: []registry.DynamicColumn{
				{Name: "sku", Type: registry.ColText, NotNull: true},
				{Name: "qty", Type: registry.ColInt},
			},
		},
	})
	return r
}

func typedExecutor(t testing.TB, store command.Store, skipEntity bool) *command.Executor {
	t.Helper()
	x, err := command.NewExecutor(typedRegistry(t, skipEntity), store)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func TestExec_DynamicTypeMismatchRejected(t *testing.T) {
	x := typedExecutor(t, newFakeStore(), false)
	_, err := x.Exec(acmeCtx(t), command.Command{
		Entity: "widgets", Op: command.OpCreate,
		Payload: map[string]any{"sku": "A1", "qty": "not-a-number"},
	})
	if err == nil || !strings.Contains(err.Error(), "qty") {
		t.Fatalf("type mismatch must be rejected mentioning qty, got %v", err)
	}
}

func TestExec_DynamicTypeCoercion(t *testing.T) {
	store := newFakeStore()
	x := typedExecutor(t, store, false)
	// JSON-shaped payload: qty arrives as float64.
	res, err := x.Exec(acmeCtx(t), command.Command{
		Entity: "widgets", Op: command.OpCreate,
		Payload: map[string]any{"sku": "A1", "qty": float64(3)},
	})
	if err != nil {
		t.Fatalf("coercible payload must succeed: %v", err)
	}
	row := store.rows[key("widgets", res.AggID)]
	if got, ok := row["qty"].(int64); !ok || got != 3 {
		t.Fatalf("qty must be coerced to int64(3), got %#v", row["qty"])
	}
}

func TestExec_DynamicTypeCheckSkippedPerEntity(t *testing.T) {
	x := typedExecutor(t, newFakeStore(), true) // NoTypeCheck: true
	_, err := x.Exec(acmeCtx(t), command.Command{
		Entity: "widgets", Op: command.OpCreate,
		Payload: map[string]any{"sku": "A1", "qty": "not-a-number"},
	})
	if err != nil {
		t.Fatalf("per-entity NoTypeCheck must bypass validation, got %v", err)
	}
}

func TestExec_DynamicTypeCheckSkippedPerWrite(t *testing.T) {
	x := typedExecutor(t, newFakeStore(), false)
	_, err := x.Exec(acmeCtx(t), command.Command{
		Entity: "widgets", Op: command.OpCreate, SkipTypeCheck: true,
		Payload: map[string]any{"sku": "A1", "qty": "not-a-number"},
	})
	if err != nil {
		t.Fatalf("per-write SkipTypeCheck must bypass validation, got %v", err)
	}
}

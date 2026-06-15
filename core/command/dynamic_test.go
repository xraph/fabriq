package command_test

import (
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
)

func dynRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "orders", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_orders",
			Columns: []registry.DynamicColumn{
				{Name: "sku", Type: registry.ColText, NotNull: true},
				{Name: "qty", Type: registry.ColInt},
			},
		},
	})
	return r
}

func dynExecutor(t testing.TB, store command.Store) *command.Executor {
	t.Helper()
	x, err := command.NewExecutor(dynRegistry(t), store)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func TestExec_DynamicCreateUpdate(t *testing.T) {
	store := newFakeStore()
	x := dynExecutor(t, store)
	ctx := acmeCtx(t)

	res, err := x.Exec(ctx, command.Command{Entity: "orders", Op: command.OpCreate, Payload: map[string]any{"sku": "A1", "qty": 3}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if res.Version != 1 || store.outbox[0].Type != "orders.created" {
		t.Fatalf("create: version=%d type=%q", res.Version, store.outbox[0].Type)
	}
	p := string(store.outbox[0].Payload)
	if !strings.Contains(p, `"sku":"A1"`) || !strings.Contains(p, `"tenant_id":"acme"`) || !strings.Contains(p, `"version":1`) {
		t.Fatalf("payload not column-keyed/stamped: %s", p)
	}

	upd, err := x.Exec(ctx, command.Command{Entity: "orders", Op: command.OpUpdate, AggID: res.AggID, Payload: map[string]any{"sku": "A2", "qty": 5}})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if upd.Version != 2 || store.outbox[1].Type != "orders.updated" {
		t.Fatalf("update: version=%d type=%q", upd.Version, store.outbox[1].Type)
	}
}

func TestExec_DynamicUpsertThenDelete(t *testing.T) {
	store := newFakeStore()
	x := dynExecutor(t, store)
	ctx := acmeCtx(t)
	const id = "01HORDERS0000000000000001"

	if _, err := x.Exec(ctx, command.Command{Entity: "orders", Op: command.OpUpsert, AggID: id, Payload: map[string]any{"sku": "A1", "qty": 1}}); err != nil {
		t.Fatalf("upsert(create): %v", err)
	}
	r2, err := x.Exec(ctx, command.Command{Entity: "orders", Op: command.OpUpsert, AggID: id, Payload: map[string]any{"sku": "A1", "qty": 2}})
	if err != nil || r2.Version != 2 || store.outbox[1].Type != "orders.updated" {
		t.Fatalf("upsert(update): v=%d type=%q err=%v", r2.Version, store.outbox[1].Type, err)
	}
	del, err := x.Exec(ctx, command.Command{Entity: "orders", Op: command.OpDelete, AggID: id})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if del.Version != 3 || store.outbox[2].Type != "orders.deleted" || string(store.outbox[2].Payload) != "{}" {
		t.Fatalf("delete: v=%d type=%q payload=%s", del.Version, store.outbox[2].Type, store.outbox[2].Payload)
	}
}

func TestExec_DynamicRequiredValidation(t *testing.T) {
	x := dynExecutor(t, newFakeStore())
	_, err := x.Exec(acmeCtx(t), command.Command{Entity: "orders", Op: command.OpCreate, Payload: map[string]any{"qty": 1}}) // missing sku
	if err == nil || !strings.Contains(err.Error(), "sku") {
		t.Fatalf("missing required column must error mentioning sku, got %v", err)
	}
}

func TestExec_DynamicTenantForgeryRejected(t *testing.T) {
	x := dynExecutor(t, newFakeStore())
	_, err := x.Exec(acmeCtx(t), command.Command{Entity: "orders", Op: command.OpCreate, Payload: map[string]any{"sku": "A1", "tenant_id": "victim"}})
	if err == nil || !strings.Contains(err.Error(), "tenant") {
		t.Fatalf("payload with foreign tenant_id must be rejected, got %v", err)
	}
}

//go:build integration

package fabriq_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

var persistedOrdersSpec = registry.EntitySpec{
	Name: "orders",
	Kind: registry.KindAggregate,
	Schema: &registry.DynamicSchema{
		Table:   "ds_orders",
		Columns: []registry.DynamicColumn{{Name: "sku", Type: registry.ColText}},
	},
}

// TestDynamicEntity_SurvivesReopen proves Config.Dynamic.Persist end to end: a
// type defined at runtime is saved to fabriq_dynamic_entity, a fresh Open
// with an empty registry loads it back, its rows are still readable, and the
// app repeating its define at boot is a no-op success.
func TestDynamicEntity_SurvivesReopen(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()

	// DefineDynamic runs CREATE TABLE, which the unprivileged app role cannot,
	// so this opens as the owner like fabriq_dynamic_e2e_integration_test.go.
	cfg := fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: superDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
		Dynamic:  fabriq.DynamicConfig{Persist: true},
	}

	f1, _, err := fabriq.Open(ctx, registry.New(), cfg)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := f1.DefineDynamic(ctx, persistedOrdersSpec); err != nil {
		_ = f1.Close()
		t.Fatalf("DefineDynamic: %v", err)
	}
	tctx, err := tenant.WithTenant(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	res, err := f1.Exec(tctx, command.Command{
		Entity:  "orders",
		Op:      command.OpCreate,
		Payload: map[string]any{"sku": "A1"},
	})
	if err != nil {
		_ = f1.Close()
		t.Fatalf("Exec(orders): %v", err)
	}
	if err := f1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	reg := registry.New()
	f2, _, err := fabriq.Open(ctx, reg, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = f2.Close() })

	if _, ok := reg.Get("orders"); !ok {
		t.Fatal("orders not registered after reopen")
	}
	if got := f2.LastDynamicSync().Added; !reflect.DeepEqual(got, []string{"orders"}) {
		t.Fatalf("LastDynamicSync().Added = %v, want [orders]", got)
	}

	var row map[string]any
	if err := f2.Relational().Get(tctx, "orders", res.AggID, &row); err != nil {
		t.Fatalf("Get(orders): %v", err)
	}
	if row["sku"] != "A1" {
		t.Fatalf("sku = %v, want A1", row["sku"])
	}

	if err := f2.DefineDynamic(ctx, persistedOrdersSpec); err != nil {
		t.Fatalf("repeat DefineDynamic after reopen: %v", err)
	}
}

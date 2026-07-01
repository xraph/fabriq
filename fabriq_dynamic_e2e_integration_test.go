//go:build integration

package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// runtimeWidgetSpec is registered live (via f.DefineDynamic), NOT pre-declared
// on the boot registry, proving the type resolves without a process restart.
var runtimeWidgetSpec = registry.EntitySpec{
	Name: "runtime_widget",
	Kind: registry.KindAggregate,
	Schema: &registry.DynamicSchema{
		Table: "ds_runtime_widgets",
		Columns: []registry.DynamicColumn{
			{Name: "name", Type: registry.ColText, NotNull: true},
		},
	},
}

// TestDynamicLifecycle_DefineAndDrop proves the runtime dynamic-entity
// lifecycle facade end-to-end against a real Postgres: DefineDynamic makes a
// brand-new type immediately writable/queryable with no restart, and
// DropDynamic tears it back down so the type no longer resolves.
func TestDynamicLifecycle_DefineAndDrop(t *testing.T) {
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)

	// Boot registry does NOT know about runtime_widget; it is defined live.
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()

	// DefineDynamic/DropDynamic issue real DDL (CREATE/DROP TABLE), which the
	// unprivileged fabriq_app role (see fabriqtest.CreateAppRole) cannot run —
	// by design, dynamic schema management is an owner/migrator-level
	// operation, mirroring how dynamic_projection_e2e_integration_test.go's
	// ensureWidgets runs EnsureDynamic against the superuser DSN. Open against
	// superDSN so this test proves the DefineDynamic/DropDynamic orchestration
	// (register+ensure+guard, drop+unguard+unregister) against a real
	// Postgres, followed by live read/write resolution via the normal
	// command/query path.
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: superDSN},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	_ = stores

	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Live-define the type: registers it and creates its table with no restart.
	if err := f.DefineDynamic(ctx, runtimeWidgetSpec); err != nil {
		t.Fatalf("DefineDynamic: %v", err)
	}

	// Prove live resolution: create then list through the normal
	// command/query path immediately after definition.
	res, err := f.Exec(tctx, command.Command{
		Entity:  "runtime_widget",
		Op:      command.OpCreate,
		Payload: map[string]any{"name": "Sprocket"},
	})
	if err != nil {
		t.Fatalf("Exec(runtime_widget): %v", err)
	}
	if res.AggID == "" {
		t.Fatal("expected a generated AggID")
	}

	var rows []map[string]any
	if err := f.Relational().List(tctx, "runtime_widget", query.ListQuery{}, &rows); err != nil {
		t.Fatalf("List(runtime_widget): %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 runtime_widget row, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != "Sprocket" {
		t.Errorf("row.name = %v, want Sprocket", rows[0]["name"])
	}

	// Drop the type: table + registry entry go away together.
	if err := f.DropDynamic(ctx, "runtime_widget"); err != nil {
		t.Fatalf("DropDynamic: %v", err)
	}

	// The type must no longer resolve through the query path.
	if err := f.Relational().List(tctx, "runtime_widget", query.ListQuery{}, &rows); err == nil {
		t.Fatal("expected List(runtime_widget) to fail after DropDynamic")
	}

	// Re-defining under the same name must succeed cleanly, proving no
	// dangling registry/guard state was left behind by the drop.
	if err := f.DefineDynamic(ctx, runtimeWidgetSpec); err != nil {
		t.Fatalf("re-DefineDynamic after drop: %v", err)
	}
	if err := f.DropDynamic(ctx, "runtime_widget"); err != nil {
		t.Fatalf("final DropDynamic: %v", err)
	}
}

//go:build integration

package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// openFsTest boots Postgres, migrates as owner, and opens fabriq as the app role.
func openFsTest(t *testing.T) (*fabriq.Fabriq, *fabriq.Stores, *registry.Registry) {
	t.Helper()
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = owner.Close()
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: appDSN}})
	if err != nil {
		t.Fatalf("fabriq.Open: %v", err)
	}
	t.Cleanup(func() { _ = stores.Close() })
	return f, stores, reg
}

func TestFsNodeEntityRoundTrip(t *testing.T) {
	ctx := context.Background()
	f, _, _ := openFsTest(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	// Create an fs_node directly via the command plane (facade comes in Task 2).
	res, err := f.Exec(tctx, command.Command{
		Entity:  "fs_node",
		Op:      command.OpCreate,
		Payload: &domain.FsNode{Name: "docs", Path: "/docs", NodeType: "folder", Metadata: map[string]any{"k": "v"}},
	})
	if err != nil {
		t.Fatalf("create fs_node: %v", err)
	}
	if res.AggID == "" || res.Version != 1 {
		t.Fatalf("unexpected result %+v", res)
	}

	// Read it back; metadata JSONB and fields round-trip.
	var got domain.FsNode
	if err := f.Relational().Get(tctx, "fs_node", res.AggID, &got); err != nil {
		t.Fatalf("get fs_node: %v", err)
	}
	if got.Name != "docs" || got.Path != "/docs" || got.NodeType != "folder" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if got.Metadata["k"] != "v" {
		t.Fatalf("metadata JSONB did not round-trip: %+v", got.Metadata)
	}
}

//go:build integration

package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestBlobObjectCreateRoundTrip(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
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
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	f, _, err := fabriq.Open(ctx, reg, fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: appDSN}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	res, err := f.Exec(tctx, command.Command{
		Entity:  "blob_object",
		Op:      command.OpCreate,
		Payload: &domain.BlobObject{Hash: "h1", Size: 5, ContentType: "text/plain"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != 1 || res.AggID == "" {
		t.Fatalf("unexpected result %+v", res)
	}
	var got domain.BlobObject
	if err := f.Relational().Get(tctx, "blob_object", res.AggID, &got); err != nil {
		t.Fatal(err)
	}
	if got.Hash != "h1" || got.Size != 5 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

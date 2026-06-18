//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestOpenWiresBlobStore(t *testing.T) {
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
	appDSN := fabriqtest.CreateAppRole(t, superDSN)

	f, _, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Storage:  fabriq.StorageConfig{StorageDriver: "file://" + t.TempDir(), DefaultBucket: "primary"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	tctx, _ := tenant.WithTenant(ctx, "t1")
	if _, err := f.Blob().Put(tctx, "doc/a", bytes.NewReader([]byte("xyz")), blob.PutOpts{Size: 3}); err != nil {
		t.Fatal(err)
	}
	rc, _, err := f.Blob().Get(tctx, "doc/a")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "xyz" {
		t.Fatalf("got %q want xyz", got)
	}
}

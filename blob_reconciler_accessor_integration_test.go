//go:build integration

package fabriq_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestStoresBlobReconcilerAccessor(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatal(err)
	}
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = owner.Close()
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)

	// CAS enabled → accessor returns a reconciler.
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Storage:  fabriq.StorageConfig{StorageDriver: "mem://", DefaultBucket: "c", EnableCas: true},
	})
	if err != nil {
		t.Fatalf("Open (cas on): %v", err)
	}
	defer func() { _ = stores.Close() }()
	_ = f
	if _, err := stores.BlobReconciler(time.Hour); err != nil {
		t.Fatalf("BlobReconciler with CAS on: %v", err)
	}

	// CAS disabled → accessor errors.
	f2, stores2, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
	})
	if err != nil {
		t.Fatalf("Open (cas off): %v", err)
	}
	defer func() { _ = stores2.Close() }()
	_ = f2
	if _, err := stores2.BlobReconciler(time.Hour); err == nil {
		t.Fatal("BlobReconciler without CAS should error")
	}
}

//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestBlobGCEndToEndCrossTenant(t *testing.T) {
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

	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Storage:  fabriq.StorageConfig{StorageDriver: "mem://", DefaultBucket: "c", EnableCas: true},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = stores.Close() }()

	tctxA, _ := tenant.WithTenant(ctx, "tenant-a")
	tctxB, _ := tenant.WithTenant(ctx, "tenant-b")

	// Both tenants store identical content (separate buckets via Task 1).
	refA, err := f.PutBlob(tctxA, bytes.NewReader([]byte("payload")), fabriq.PutBlobOpts{})
	if err != nil {
		t.Fatalf("A PutBlob: %v", err)
	}
	refB, err := f.PutBlob(tctxB, bytes.NewReader([]byte("payload")), fabriq.PutBlobOpts{})
	if err != nil {
		t.Fatalf("B PutBlob: %v", err)
	}

	// Delete A's catalog row → truth count for A drops to 0.
	if err := f.DeleteBlob(tctxA, refA.ID); err != nil {
		t.Fatalf("A DeleteBlob: %v", err)
	}

	// Reconcile tenant A with grace=0 → A's byte is GC'd.
	rec, err := stores.BlobReconciler(0)
	if err != nil {
		t.Fatalf("BlobReconciler: %v", err)
	}
	rep, err := rec.Reconcile(tctxA, true)
	if err != nil {
		t.Fatalf("Reconcile A: %v", err)
	}
	if rep.GCCount != 1 {
		t.Fatalf("A GCCount = %d, want 1", rep.GCCount)
	}

	// A's byte is gone.
	if _, err := stores.CAS.Retrieve(tctxA, refA.Hash); err == nil {
		t.Fatal("A byte should be GC'd")
	}

	// B's blob is untouched (different bucket, never reconciled).
	rc, _, err := f.GetBlob(tctxB, refB.ID)
	if err != nil {
		t.Fatalf("B GetBlob after A GC: %v", err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "payload" {
		t.Fatalf("B GetBlob = %q, want payload", got)
	}
}

//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestPutGetDeleteBlob(t *testing.T) {
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
		Storage: fabriq.StorageConfig{
			StorageDriver: "file://" + t.TempDir(),
			DefaultBucket: "primary",
			EnableCas:     true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	// PutBlob: stores bytes and creates a catalog row.
	ref, err := f.PutBlob(tctx, bytes.NewReader([]byte("hello")), fabriq.PutBlobOpts{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	if ref.ID == "" || ref.Size != 5 || ref.Version != 1 || ref.Hash == "" {
		t.Fatalf("PutBlob unexpected ref: %+v", ref)
	}

	// GetBlob: round-trips the bytes and returns a matching BlobRef.
	rc, ref2, err := f.GetBlob(tctx, ref.ID)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(b) != "hello" {
		t.Fatalf("GetBlob body: got %q, want \"hello\"", b)
	}
	if ref2.Hash != ref.Hash {
		t.Fatalf("GetBlob hash mismatch: got %q, want %q", ref2.Hash, ref.Hash)
	}

	// Dedup: identical bytes → same Hash, distinct blob_object ID.
	ref3, err := f.PutBlob(tctx, bytes.NewReader([]byte("hello")), fabriq.PutBlobOpts{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("PutBlob (dedup): %v", err)
	}
	if ref3.Hash != ref.Hash {
		t.Fatalf("dedup: hash mismatch: got %q, want %q", ref3.Hash, ref.Hash)
	}
	if ref3.ID == ref.ID {
		t.Fatalf("dedup: expected distinct blob_object IDs, both are %q", ref.ID)
	}

	// DeleteBlob: removes the catalog row (bytes remain until Phase 4 GC).
	if err := f.DeleteBlob(tctx, ref.ID); err != nil {
		t.Fatalf("DeleteBlob: %v", err)
	}
	var bo domain.BlobObject
	if err := f.Relational().Get(tctx, "blob_object", ref.ID, &bo); err == nil {
		t.Fatal("expected blob_object row to be gone after DeleteBlob, but Get succeeded")
	}
}

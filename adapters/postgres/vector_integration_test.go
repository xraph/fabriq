//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// newVectorHarness boots a container, runs all migrations (which install
// pgvector via 0006 if the extension is available), and returns an app-role
// Adapter (so RLS applies). If fabriq_embeddings does not exist after
// migrations (pgvector absent in the container), the test is skipped —
// mirrors the extension-guarded migration pattern used by newSpatialHarness.
func newVectorHarness(t *testing.T) *postgres.Adapter {
	t.Helper()

	superDSN := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	// Run migrations as superuser (same pattern as newHarness).
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

	// Check whether the pgvector migration actually created the table.
	// If pgvector is absent in the container, the migration skips it silently
	// and the table does not exist — skip the test cleanly.
	var tableExists bool
	row := owner.Driver().QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'fabriq_embeddings'
		)`)
	if err := row.Scan(&tableExists); err != nil {
		_ = owner.Close()
		t.Fatalf("check fabriq_embeddings: %v", err)
	}
	_ = owner.Close()
	if !tableExists {
		t.Skip("fabriq_embeddings not present: pgvector unavailable in this Postgres image")
	}

	// App-role adapter (RLS actually applies — superusers bypass it).
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg, postgres.WithGuardedTables(domain.ReadingsSeries))
	if err != nil {
		t.Fatalf("postgres.Open (app): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// vectorCtx returns a tenant-stamped context for the given tenant ID.
func vectorCtx(t testing.TB, tenantID string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("WithTenant(%q): %v", tenantID, err)
	}
	return ctx
}

// TestVector_Delete verifies that a deleted embedding no longer appears in
// Similar results, mirroring TestSpatial_Delete.
func TestVector_Delete(t *testing.T) {
	a := newVectorHarness(t)
	ctx := vectorCtx(t, "acme")

	// Upsert two embeddings: A at (1,0,…) and B at (0,1,…).
	if err := a.Upsert(ctx, "doc", "A", emb768(1, 0), nil); err != nil {
		t.Fatalf("Upsert A: %v", err)
	}
	if err := a.Upsert(ctx, "doc", "B", emb768(0, 1), nil); err != nil {
		t.Fatalf("Upsert B: %v", err)
	}

	// Delete B.
	if err := a.Delete(ctx, "doc", "B"); err != nil {
		t.Fatalf("Delete B: %v", err)
	}

	// Similar should return only A.
	var got []query.VectorMatch
	if err := a.Similar(ctx, query.VectorQuery{Entity: "doc", Embedding: emb768(1, 0), K: 5}, &got); err != nil {
		t.Fatalf("Similar after delete: %v", err)
	}
	for _, m := range got {
		if m.ID == "B" {
			t.Errorf("deleted embedding B still returned by Similar: %+v", got)
		}
	}
	if len(got) != 1 || got[0].ID != "A" {
		t.Errorf("After deleting B, want only [A], got %+v", got)
	}

	// Deleting a missing id must be a no-op (no error).
	if err := a.Delete(ctx, "doc", "nope"); err != nil {
		t.Fatalf("delete missing id should be no-op, got %v", err)
	}
}

// TestVector_SimilarFilter_Integration verifies that VectorQuery.Filter
// narrows Similar results to entries whose meta contains all filter key/value
// pairs.
func TestVector_SimilarFilter_Integration(t *testing.T) {
	a := newVectorHarness(t)
	ctx := vectorCtx(t, "acme")

	if err := a.Upsert(ctx, "doc", "a", emb768(1, 0), map[string]any{"kind": "note"}); err != nil {
		t.Fatalf("Upsert a: %v", err)
	}
	if err := a.Upsert(ctx, "doc", "b", emb768(1, 0), map[string]any{"kind": "task"}); err != nil {
		t.Fatalf("Upsert b: %v", err)
	}

	var got []query.VectorMatch
	if err := a.Similar(ctx, query.VectorQuery{
		Entity:    "doc",
		Embedding: emb768(1, 0),
		K:         10,
		Filter:    map[string]string{"kind": "note"},
	}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("filtered Similar = %+v, want only a", got)
	}
}

// TestVector_DeleteByMeta_Integration verifies that DeleteByMeta removes only
// entries whose meta matches the filter, leaving others intact.
func TestVector_DeleteByMeta_Integration(t *testing.T) {
	a := newVectorHarness(t)
	ctx := vectorCtx(t, "acme")
	vec := postgres.NewVectorAdapter(a)

	if err := a.Upsert(ctx, "doc", "a", emb768(1, 0), map[string]any{"kind": "note"}); err != nil {
		t.Fatalf("Upsert a: %v", err)
	}
	if err := a.Upsert(ctx, "doc", "b", emb768(1, 0), map[string]any{"kind": "task"}); err != nil {
		t.Fatalf("Upsert b: %v", err)
	}

	if err := a.DeleteByMeta(ctx, "doc", map[string]string{"kind": "note"}); err != nil {
		t.Fatal(err)
	}

	if _, err := vec.Get(ctx, "doc", "a"); err == nil {
		t.Fatalf("a should be gone after DeleteByMeta")
	}
	if _, err := vec.Get(ctx, "doc", "b"); err != nil {
		t.Fatalf("b should survive DeleteByMeta: %v", err)
	}
}

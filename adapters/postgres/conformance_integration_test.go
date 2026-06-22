//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/conformance"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// pgConformanceBackend adapts a migrated, app-role Postgres adapter to
// conformance.Backend. One container is shared across cases; each Setup mints
// a fresh tenant pair and RLS isolates them.
type pgConformanceBackend struct {
	reg       *registry.Registry
	a         *postgres.Adapter
	exec      *command.Executor
	n         atomic.Int64
	hasVector bool // true when fabriq_embeddings was created (pgvector available)
}

func newPGConformanceBackend(t *testing.T) conformance.Backend {
	t.Helper()
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatal(err)
	}

	// Run migrations as superuser; probe whether pgvector was installed.
	superA, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatal(err)
	}
	orch, err := migrations.NewOrchestrator(superA.Driver())
	if err != nil {
		_ = superA.Close()
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		_ = superA.Close()
		t.Fatalf("migrate: %v", err)
	}
	var tableExists bool
	row := superA.Driver().QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'fabriq_embeddings'
		)`)
	if err := row.Scan(&tableExists); err != nil {
		_ = superA.Close()
		t.Fatalf("probe fabriq_embeddings: %v", err)
	}
	_ = superA.Close()

	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg, postgres.WithGuardedTables(domain.ReadingsSeries))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	x, err := command.NewExecutor(reg, a)
	if err != nil {
		t.Fatal(err)
	}
	return &pgConformanceBackend{reg: reg, a: a, exec: x, hasVector: tableExists}
}

func (b *pgConformanceBackend) Name() string { return "postgres" }

func (b *pgConformanceBackend) Capabilities() conformance.CapabilitySet {
	return conformance.CapabilitySet{
		conformance.CapRawSQL:       true,
		conformance.CapConcurrentTx: true,
		conformance.CapPersistence:  true,
	}
}

func (b *pgConformanceBackend) Setup(t *testing.T) *conformance.Env {
	t.Helper()
	n := b.n.Add(1)
	env := &conformance.Env{
		Ctx:        b.tctx(t, fmt.Sprintf("conf_%d_a", n)),
		ForeignCtx: b.tctx(t, fmt.Sprintf("conf_%d_b", n)),
		Registry:   b.reg,
		Exec:       b.exec,
		Relational: b.a,
	}
	// Wire vector only when pgvector is present in this container; otherwise
	// RunVector skips cleanly (Env.Vector == nil).
	if b.hasVector {
		env.Vector = postgres.NewVectorAdapter(b.a)
		env.EmbeddingDim = 768 // schema-declared vector(768) column
	}
	return env
}

func (b *pgConformanceBackend) tctx(t *testing.T, id string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

// TestPostgresConformance runs the shared conformance suite against a real,
// migrated Postgres adapter. Ports that are not wired (nil Env field) skip
// cleanly; the vector suite skips when pgvector is absent in the container.
func TestPostgresConformance(t *testing.T) {
	conformance.RunAll(t, newPGConformanceBackend(t))
}

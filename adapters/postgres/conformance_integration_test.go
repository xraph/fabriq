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
	reg  *registry.Registry
	a    *postgres.Adapter
	exec *command.Executor
	n    atomic.Int64
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

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()

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
	return &pgConformanceBackend{reg: reg, a: a, exec: x}
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
	return &conformance.Env{
		Ctx:        b.tctx(t, fmt.Sprintf("conf_%d_a", n)),
		ForeignCtx: b.tctx(t, fmt.Sprintf("conf_%d_b", n)),
		Registry:   b.reg,
		Exec:       b.exec,
		Relational: b.a,
	}
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
// migrated Postgres adapter (relational port). Other ports are nil here and
// their suites skip; follow-on plans add them.
func TestPostgresConformance(t *testing.T) {
	conformance.RunAll(t, newPGConformanceBackend(t))
}

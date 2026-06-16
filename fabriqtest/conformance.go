package fabriqtest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/xraph/fabriq/conformance"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

// fakeBackend adapts the in-memory World to conformance.Backend. One World
// (one shared store) is created per backend; each Setup mints a fresh unique
// tenant pair, so cases are isolated without truncation and cross-tenant
// cases have a distinct ForeignCtx over the same store.
type fakeBackend struct {
	reg   *registry.Registry
	world *World
	exec  *command.Executor
	n     atomic.Int64
}

// NewConformanceBackend builds a fake-backed conformance.Backend over the
// domain registry. It is the fast (Docker-free) gate that keeps the fakes
// from drifting from real-adapter behavior.
func NewConformanceBackend(t testing.TB) conformance.Backend {
	t.Helper()
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatalf("fabriqtest: register domain: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("fabriqtest: validate registry: %v", err)
	}
	w := NewWorld(reg)
	x, err := command.NewExecutor(reg, w.Store)
	if err != nil {
		t.Fatalf("fabriqtest: executor: %v", err)
	}
	return &fakeBackend{reg: reg, world: w, exec: x}
}

func (b *fakeBackend) Name() string { return "fake" }

// Capabilities is empty: the fakes hold none of the divergent capabilities
// (see the conformance ledger). Capability-gated cases skip or assert-degrade.
func (b *fakeBackend) Capabilities() conformance.CapabilitySet {
	return conformance.CapabilitySet{}
}

func (b *fakeBackend) Setup(t *testing.T) *conformance.Env {
	t.Helper()
	n := b.n.Add(1)
	primary := fmt.Sprintf("conf_%d_a", n)
	foreign := fmt.Sprintf("conf_%d_b", n)
	return &conformance.Env{
		Ctx:         b.tenantCtx(t, primary),
		ForeignCtx:  b.tenantCtx(t, foreign),
		Registry:    b.reg,
		Exec:        b.exec,
		Relational:  b.world.Rel,
		Graph:       b.world.Graph,
		Search:      b.world.Search,
		Vector:      b.world.Vector,
		Spatial:     b.world.Spatial,
		TS:          b.world.TS,
		Projection:  b.world.Projections,
		GraphTarget: registry.GraphName(primary),
	}
}

func (b *fakeBackend) tenantCtx(t *testing.T, id string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), id)
	if err != nil {
		t.Fatalf("fabriqtest: tenant ctx: %v", err)
	}
	return ctx
}

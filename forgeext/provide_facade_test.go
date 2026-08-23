package forgeext

import (
	"testing"

	"github.com/xraph/vessel"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

// newStartedExtension returns an Extension whose facade is already assembled
// from fakes, standing in for a completed Start.
func newStartedExtension(t *testing.T) (*Extension, *fabriq.Fabriq) {
	t.Helper()
	reg := registry.New()
	w := fabriqtest.NewWorld(reg)
	fab, err := fabriq.New(reg, fabriq.Ports{Store: w.Store, Relational: w.Rel})
	if err != nil {
		t.Fatalf("fabriq.New: %v", err)
	}
	e := New(reg)
	e.fab = fab
	return e, fab
}

// TestProvideFacade_ByInterface is the point of the whole exercise: consumers
// must be able to resolve the facade by the query.Fabric port, so they never
// import the composition root (and every adapter it links) just to name a DI
// key. The same key is what lets a deployment swap in *remote.Fabric.
func TestProvideFacade_ByInterface(t *testing.T) {
	e, fab := newStartedExtension(t)
	c := vessel.New()
	if err := e.provideFacade(c); err != nil {
		t.Fatalf("provideFacade: %v", err)
	}

	got, err := vessel.Inject[query.Fabric](c)
	if err != nil {
		t.Fatalf("Inject[query.Fabric]: %v", err)
	}
	if got != query.Fabric(fab) {
		t.Fatalf("Inject[query.Fabric] = %v, want the started facade %v", got, fab)
	}
}

// TestProvideFacade_ByConcreteType pins the pre-existing keys so adding the
// interface registration stays backward compatible.
func TestProvideFacade_ByConcreteType(t *testing.T) {
	e, fab := newStartedExtension(t)
	c := vessel.New()
	if err := e.provideFacade(c); err != nil {
		t.Fatalf("provideFacade: %v", err)
	}

	byType, err := vessel.Inject[*fabriq.Fabriq](c)
	if err != nil {
		t.Fatalf("Inject[*fabriq.Fabriq]: %v", err)
	}
	if byType != fab {
		t.Fatalf("Inject by type = %v, want %v", byType, fab)
	}

	byName, err := vessel.InjectNamed[*fabriq.Fabriq](c, "fabriq")
	if err != nil {
		t.Fatalf("InjectNamed(\"fabriq\"): %v", err)
	}
	if byName != fab {
		t.Fatalf("InjectNamed = %v, want %v", byName, fab)
	}
}

// TestProvideFacade_Registry covers the companion key. Consumers need the
// entity registry alongside the port, and query.Fabric does not carry it.
func TestProvideFacade_Registry(t *testing.T) {
	e, _ := newStartedExtension(t)
	c := vessel.New()
	if err := e.provideFacade(c); err != nil {
		t.Fatalf("provideFacade: %v", err)
	}

	got, err := vessel.InjectNamed[*registry.Registry](c, RegistryServiceName)
	if err != nil {
		t.Fatalf("InjectNamed(%q): %v", RegistryServiceName, err)
	}
	if got != e.reg {
		t.Fatalf("registry = %v, want %v", got, e.reg)
	}
}

// TestProvideFacade_RegistryLeavesTypeKeyFree pins the name-only registration:
// a host app must still be able to provide its own *registry.Registry.
func TestProvideFacade_RegistryLeavesTypeKeyFree(t *testing.T) {
	e, _ := newStartedExtension(t)
	c := vessel.New()
	if err := e.provideFacade(c); err != nil {
		t.Fatalf("provideFacade: %v", err)
	}

	hostReg := registry.New()
	if err := vessel.Provide(c, func() (*registry.Registry, error) { return hostReg, nil }); err != nil {
		t.Fatalf("host registry provide conflicted with fabriq's: %v", err)
	}
	got, err := vessel.Inject[*registry.Registry](c)
	if err != nil {
		t.Fatalf("Inject[*registry.Registry]: %v", err)
	}
	if got != hostReg {
		t.Fatalf("type key resolved to %v, want the host registry %v", got, hostReg)
	}
}

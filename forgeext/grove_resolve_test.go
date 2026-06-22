package forgeext

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/grove"
	"github.com/xraph/vessel"
)

// fakeGroveDriver is a minimal grove.GroveDriver — enough to build a *grove.DB
// without dialing. grove/drivers/* may only be imported under fabriq/adapters,
// so this package constructs grove handles from a fake driver instead.
type fakeGroveDriver struct{}

func (fakeGroveDriver) Name() string                 { return "fake" }
func (fakeGroveDriver) Close() error                 { return nil }
func (fakeGroveDriver) Ping(_ context.Context) error { return nil }

func newGrove(t *testing.T) *grove.DB {
	t.Helper()
	gdb, err := grove.Open(fakeGroveDriver{})
	if err != nil {
		t.Fatalf("grove.Open: %v", err)
	}
	return gdb
}

// TestResolveGroveFrom_Default resolves the default (unnamed) *grove.DB from a
// container, mirroring authsome's silent auto-discovery.
func TestResolveGroveFrom_Default(t *testing.T) {
	gdb := newGrove(t)
	c := vessel.New()
	if err := vessel.Provide(c, func() (*grove.DB, error) { return gdb, nil }); err != nil {
		t.Fatalf("Provide: %v", err)
	}

	e := New(nil)
	if got := e.resolveGroveFrom(c); got != gdb {
		t.Fatalf("resolveGroveFrom default = %v, want %v", got, gdb)
	}
}

// TestResolveGroveFrom_Named resolves a named grove database when GroveDatabase
// is set (mirrors authsome's GroveDatabase config).
func TestResolveGroveFrom_Named(t *testing.T) {
	named := newGrove(t)
	c := vessel.New()
	if err := vessel.Provide(c, func() (*grove.DB, error) { return named, nil }, vessel.WithName("tenants")); err != nil {
		t.Fatalf("Provide named: %v", err)
	}

	e := New(nil, WithGroveDatabase("tenants"))
	if got := e.resolveGroveFrom(c); got != named {
		t.Fatalf("resolveGroveFrom named = %v, want %v", got, named)
	}
}

// TestResolveGroveFrom_None returns nil when no grove is registered or the
// container is nil — resolution is best-effort.
func TestResolveGroveFrom_None(t *testing.T) {
	e := New(nil)
	if got := e.resolveGroveFrom(nil); got != nil {
		t.Fatalf("nil container should yield nil grove, got %v", got)
	}
	if got := e.resolveGroveFrom(vessel.New()); got != nil {
		t.Fatalf("empty container should yield nil grove, got %v", got)
	}
}

// TestStart_NoSourceOfTruth_ErrorMentionsGrove proves the failure path now
// points users at grove resolution as well as DSN/shards. With no app wired,
// resolveGrove finds nothing and Start must error.
func TestStart_NoSourceOfTruth_ErrorMentionsGrove(t *testing.T) {
	e := New(nil)
	err := e.Start(context.Background())
	if err == nil {
		t.Fatal("Start with no source of truth must error")
	}
	if !strings.Contains(err.Error(), "grove") {
		t.Fatalf("error should mention grove resolution, got: %v", err)
	}
}

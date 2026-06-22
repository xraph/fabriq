package postgres

import (
	"context"
	"testing"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/registry"
)

// fakeGroveDriver is a minimal grove.GroveDriver whose Name() is not "pg",
// used to prove OpenWithGrove rejects a borrowed grove that is not backed by
// the pg driver.
type fakeGroveDriver struct{}

func (fakeGroveDriver) Name() string                 { return "fake" }
func (fakeGroveDriver) Close() error                 { return nil }
func (fakeGroveDriver) Ping(_ context.Context) error { return nil }

func TestOpenWithGrove_BuildsBorrowedAdapter(t *testing.T) {
	// pgdriver.New() yields a *PgDB without dialing; grove.Open just wraps it,
	// so this exercises OpenWithGrove as a pure constructor (no Postgres).
	gdb, err := grove.Open(pgdriver.New())
	if err != nil {
		t.Fatalf("grove.Open: %v", err)
	}
	a, err := OpenWithGrove(gdb, registry.New())
	if err != nil {
		t.Fatalf("OpenWithGrove: %v", err)
	}
	if a == nil {
		t.Fatal("OpenWithGrove returned nil adapter")
	}
	if a.Grove() != gdb {
		t.Fatal("adapter does not expose the borrowed grove")
	}
	// Borrowed grove: Close must NOT close the host's grove and must be
	// idempotent/no-op.
	if err := a.Close(); err != nil {
		t.Fatalf("Close on borrowed adapter: %v", err)
	}
	// The borrowed grove is still usable after the adapter's Close (proof we
	// did not tear it down).
	if cerr := gdb.Close(); cerr != nil {
		t.Fatalf("borrowed grove should still be open after adapter.Close: %v", cerr)
	}
}

func TestOpenWithGrove_RejectsNonPgDriver(t *testing.T) {
	gdb, err := grove.Open(fakeGroveDriver{})
	if err != nil {
		t.Fatalf("grove.Open: %v", err)
	}
	if _, err := OpenWithGrove(gdb, registry.New()); err == nil {
		t.Fatal("expected OpenWithGrove to reject a non-pg grove driver")
	}
}

func TestOpenWithGrove_NilArgs(t *testing.T) {
	if _, err := OpenWithGrove(nil, registry.New()); err == nil {
		t.Fatal("expected error for nil grove")
	}
	gdb, err := grove.Open(pgdriver.New())
	if err != nil {
		t.Fatalf("grove.Open: %v", err)
	}
	if _, err := OpenWithGrove(gdb, nil); err == nil {
		t.Fatal("expected error for nil registry")
	}
}

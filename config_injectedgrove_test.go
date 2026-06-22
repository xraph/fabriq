package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq"
)

// fakeGroveDriver is a minimal grove.GroveDriver — enough to build a *grove.DB
// without dialing. Drivers proper (grove/drivers/*) may only be imported under
// fabriq/adapters, so tests outside it construct grove handles this way.
type fakeGroveDriver struct{}

func (fakeGroveDriver) Name() string                 { return "fake" }
func (fakeGroveDriver) Close() error                 { return nil }
func (fakeGroveDriver) Ping(_ context.Context) error { return nil }

// TestConfig_WithInjectedGrove verifies that a borrowed grove.DB satisfies the
// source-of-truth requirement in Validate (DSN/shards then optional) and that
// WithInjectedGrove is a non-mutating copy builder.
func TestConfig_WithInjectedGrove(t *testing.T) {
	gdb, err := grove.Open(fakeGroveDriver{})
	if err != nil {
		t.Fatalf("grove.Open: %v", err)
	}

	base := fabriq.Config{} // no DSN, no shards
	if err := base.Validate(); err == nil {
		t.Fatal("empty config must fail (no source of truth)")
	}

	withGrove := base.WithInjectedGrove(gdb)
	if err := withGrove.Validate(); err != nil {
		t.Fatalf("injected grove should satisfy the source-of-truth requirement: %v", err)
	}
	if withGrove.InjectedGrove() != gdb {
		t.Fatal("InjectedGrove() should return the borrowed handle")
	}

	// Copy semantics: the receiver is unchanged.
	if err := base.Validate(); err == nil {
		t.Fatal("WithInjectedGrove must not mutate its receiver")
	}
}

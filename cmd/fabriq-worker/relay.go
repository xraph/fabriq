package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
)

// Advisory lock keys per singleton role. Stable across versions; never
// reuse a key for a different role.
const (
	lockKeyRelay      = int64(1001)
	lockKeyReconciler = int64(1002) // phase 6
)

// workerExtension is a forge.Extension + RunnableExtension supervising the
// background runners. Projection consumers (phase 4/5) will join Run with
// their consumer groups; they scale by replica count and need no election.
type workerExtension struct {
	reg *registry.Registry
	cfg fabriq.Config

	mu     sync.Mutex
	fab    *fabriq.Fabriq
	stores *fabriq.Stores
	cancel context.CancelFunc
	done   chan struct{}
}

func newWorkerExtension(reg *registry.Registry, cfg fabriq.Config) *workerExtension {
	return &workerExtension{reg: reg, cfg: cfg}
}

// Name implements forge.Extension.
func (e *workerExtension) Name() string { return "fabriq-worker" }

// Version implements forge.Extension.
func (e *workerExtension) Version() string { return "0.1.0" }

// Description implements forge.Extension.
func (e *workerExtension) Description() string {
	return "fabriq background plane: outbox relay, projection consumers, reconciler"
}

// Dependencies implements forge.Extension.
func (e *workerExtension) Dependencies() []string { return nil }

// Register implements forge.Extension.
func (e *workerExtension) Register(forge.App) error { return nil }

// Start implements forge.Extension: open the stores.
func (e *workerExtension) Start(ctx context.Context) error {
	fab, stores, err := fabriq.Open(ctx, e.reg, e.cfg)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.fab, e.stores = fab, stores
	e.mu.Unlock()
	return nil
}

// Stop implements forge.Extension.
func (e *workerExtension) Stop(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fab != nil {
		return e.fab.Close() // closes stores too
	}
	return nil
}

// Health implements forge.Extension.
func (e *workerExtension) Health(ctx context.Context) error {
	e.mu.Lock()
	stores := e.stores
	e.mu.Unlock()
	if stores == nil || stores.Postgres == nil {
		return fmt.Errorf("fabriq-worker: stores not open")
	}
	return stores.Postgres.Grove().Ping(ctx)
}

// Run implements forge.RunnableExtension: supervise the leader-elected
// relay until shutdown.
func (e *workerExtension) Run(ctx context.Context) error {
	e.mu.Lock()
	stores := e.stores
	e.mu.Unlock()
	if stores == nil {
		return fmt.Errorf("fabriq-worker: Run before Start")
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	e.mu.Lock()
	e.cancel, e.done = cancel, done
	e.mu.Unlock()

	relay := postgres.NewRelay(stores.Postgres, e.reg, stores.Redis)
	elector := postgres.NewElector(stores.Postgres, lockKeyRelay)
	go func() {
		defer close(done)
		_ = elector.Run(runCtx, relay.Run)
	}()
	_ = ctx
	return nil
}

// Shutdown implements forge.RunnableExtension: SIGTERM drain.
func (e *workerExtension) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	cancel, done := e.cancel, e.done
	e.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return fmt.Errorf("fabriq-worker: relay did not drain in time")
	}
}

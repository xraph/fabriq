package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/internal/metrics"
)

// Advisory lock keys per singleton role. Stable across versions; never
// reuse a key for a different role.
const (
	lockKeyRelay         = int64(1001)
	lockKeyReconciler    = int64(1002)
	lockKeyDocumentPlane = int64(1003)
)

// workerExtension is a forge.Extension + RunnableExtension supervising the
// background runners. Projection consumers (phase 4/5) will join Run with
// their consumer groups; they scale by replica count and need no election.
type workerExtension struct {
	reg               *registry.Registry
	cfg               fabriq.Config
	reconcileInterval time.Duration

	mu      sync.Mutex
	fab     *fabriq.Fabriq
	stores  *fabriq.Stores
	metrics *metrics.Metrics
	app     forge.App
	cancel  context.CancelFunc
	done    chan struct{}
}

func newWorkerExtension(reg *registry.Registry, cfg fabriq.Config) *workerExtension {
	interval := 5 * time.Minute
	if raw := os.Getenv("FABRIQ_RECONCILE_INTERVAL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			interval = d // "0" disables
		}
	}
	return &workerExtension{reg: reg, cfg: cfg, reconcileInterval: interval}
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
func (e *workerExtension) Register(app forge.App) error {
	e.mu.Lock()
	e.app = app
	e.mu.Unlock()
	return nil
}

// Start implements forge.Extension: open the stores. This is the serve
// path's first real I/O — the env guard the worker's main once held lives
// here now, so operator commands (which never Start) stay store-agnostic.
func (e *workerExtension) Start(ctx context.Context) error {
	if e.cfg.Postgres.DSN == "" || e.cfg.Redis.Addr == "" {
		return fmt.Errorf("fabriq-worker: FABRIQ_POSTGRES_DSN and FABRIQ_REDIS_ADDR are required to serve")
	}
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

	// Observability: /metrics + gauge pollers.
	e.mu.Lock()
	app := e.app
	e.mu.Unlock()
	var relayOpts []postgres.RelayOption
	if app != nil {
		if m, err := wireObservability(app, stores); err == nil {
			e.mu.Lock()
			e.metrics = m
			e.mu.Unlock()
			relayOpts = append(relayOpts, postgres.WithRelayOnPublish(func(n int) {
				m.RelayPublished.Add(float64(n))
			}))
			go pollGauges(runCtx, stores, m, 15*time.Second)
		}
	}

	var logger forge.Logger
	if app != nil {
		logger = app.Logger()
	}
	relay := postgres.NewRelay(stores.Postgres, e.reg, stores.Redis, relayOpts...)
	elector := postgres.NewElector(stores.Postgres, lockKeyRelay)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		supervise(runCtx, logger, "relay", func(c context.Context) error { return elector.Run(c, relay.Run) })
	}()

	// Document plane: quiet-window materializer + compactor (leader 1003).
	docElector := postgres.NewElector(stores.Postgres, lockKeyDocumentPlane)
	wg.Add(1)
	go func() {
		defer wg.Done()
		supervise(runCtx, logger, "document-plane", func(c context.Context) error {
			return docElector.Run(c, func(leadCtx context.Context) error {
				e.runDocumentPlane(leadCtx, time.Second)
				return leadCtx.Err()
			})
		})
	}()

	// Scheduled reconciler: leader-elected, one scanner across replicas.
	if e.reconcileInterval > 0 && (stores.Falkor != nil || stores.Elastic != nil) {
		reconElector := postgres.NewElector(stores.Postgres, lockKeyReconciler)
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "reconciler", func(c context.Context) error {
				return reconElector.Run(c, func(leadCtx context.Context) error {
					e.runReconciler(leadCtx, e.reconcileInterval)
					return leadCtx.Err()
				})
			})
		}()
	}

	// Projection consumers scale by replica count — no election needed.
	consumer := consumerName()
	if stores.Falkor != nil {
		engine, err := stores.GraphEngine(e.reg, e.fab.Upcasters())
		if err != nil {
			cancel()
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:graph", func(c context.Context) error { return engine.Run(c, consumer) })
		}()
	}
	if stores.Elastic != nil {
		engine, err := stores.SearchEngine(e.reg, e.fab.Upcasters())
		if err != nil {
			cancel()
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:search", func(c context.Context) error { return engine.Run(c, consumer) })
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()
	_ = ctx
	return nil
}

// consumerName identifies this replica within the consumer groups.
func consumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "fabriq-worker"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
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

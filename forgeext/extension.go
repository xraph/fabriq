package forgeext

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/forge"
	"github.com/xraph/vessel"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/internal/metrics"
)

// Version is the fabriq forge extension version.
const Version = "0.1.0"

// Extension exposes the fabriq data fabric as a first-class Forge extension:
// the facade as a DI service (alias "fabriq"), auto health, fabriq's migrations
// (Task 3), and an opt-in background worker (Task 4).
type Extension struct {
	forge.BaseExtension
	reg *registry.Registry
	cfg Config

	mu      sync.Mutex
	fab     *fabriq.Fabriq
	stores  *fabriq.Stores
	cancel  context.CancelFunc
	done    chan struct{}
	metrics *metrics.Metrics
}

// Compile-time interface assertions: if Extension drifts from any of these
// contracts the build fails immediately rather than at runtime.
var (
	_ forge.Extension           = (*Extension)(nil)
	_ forge.RunnableExtension   = (*Extension)(nil)
	_ forge.MigratableExtension = (*Extension)(nil)
)

// New creates a new Extension with the given registry and options.
func New(reg *registry.Registry, opts ...Option) *Extension {
	var cfg Config
	for _, o := range opts {
		o(&cfg)
	}
	return &Extension{reg: reg, cfg: cfg}
}

func (e *Extension) Name() string    { return "fabriq" }
func (e *Extension) Version() string { return Version }
func (e *Extension) Description() string {
	return "fabriq data fabric: command plane, query ports, projections, worker"
}
func (e *Extension) Dependencies() []string { return nil }

// Register implements forge.Extension. It MUST call e.BaseExtension.Register first.
func (e *Extension) Register(app forge.App) error {
	if err := e.BaseExtension.Register(app); err != nil {
		return err
	}

	// Overlay extensions.fabriq.* when no explicit datastore config was given.
	if cm := app.Config(); cm != nil && e.cfg.Fabriq.Postgres.DSN == "" && len(e.cfg.Fabriq.Shards) == 0 {
		loaded := LoadConfig(cm, "extensions.fabriq.")
		// keep custom appliers from options; only fill the data-fabric config.
		loaded.CustomAppliers = e.cfg.Fabriq.CustomAppliers
		e.cfg.Fabriq = loaded
	}

	// Lazy DI service: resolves the facade opened in Start.
	// vessel.Provide registers a zero-param constructor; alias "fabriq" allows
	// resolution by name in addition to type.
	c := app.Container()
	return vessel.Provide(c, func() (*fabriq.Fabriq, error) {
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.fab == nil {
			return nil, fmt.Errorf("fabriq: facade not started yet")
		}
		return e.fab, nil
	}, vessel.WithAliases("fabriq"))
}

// Start implements forge.Extension. Opens the fabriq facade.
func (e *Extension) Start(ctx context.Context) error {
	if e.cfg.Fabriq.Postgres.DSN == "" && len(e.cfg.Fabriq.Shards) == 0 {
		return fmt.Errorf("fabriq: a Postgres source of truth is required to serve (set postgres.dsn / FABRIQ_POSTGRES_DSN, or shards)")
	}
	if e.cfg.RunWorker && e.cfg.Fabriq.Redis.Addr == "" {
		return fmt.Errorf("fabriq: a Redis address is required to serve (set redis.addr / FABRIQ_REDIS_ADDR)")
	}
	fab, stores, err := fabriq.Open(ctx, e.reg, e.cfg.Fabriq)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.fab, e.stores = fab, stores
	e.mu.Unlock()
	e.MarkStarted()
	return nil
}

// Stop implements forge.Extension. Closes the fabriq facade.
func (e *Extension) Stop(_ context.Context) error {
	e.mu.Lock()
	fab := e.fab
	e.mu.Unlock()
	var err error
	if fab != nil {
		err = fab.Close()
	}
	e.MarkStopped()
	return err
}

// Health implements forge.Extension. Pings the Postgres store.
func (e *Extension) Health(ctx context.Context) error {
	e.mu.Lock()
	stores := e.stores
	e.mu.Unlock()
	if stores == nil || stores.Postgres == nil {
		return fmt.Errorf("fabriq: stores not open")
	}
	return stores.Postgres.Grove().Ping(ctx)
}

// Fabriq returns the opened facade (nil before Start).
func (e *Extension) Fabriq() *fabriq.Fabriq {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.fab
}

// Stores returns the opened adapters (nil before Start). The gateway extension
// reads Stores().Redis to build the live-query cluster transport.
func (e *Extension) Stores() *fabriq.Stores {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stores
}

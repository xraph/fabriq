package fabriq

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/subscribe"
	"github.com/xraph/fabriq/core/tenant"
)

// Ports bundles the port implementations a Fabriq is assembled from.
// Open() fills them from configured adapters; tests and embedders may
// supply fabriqtest fakes or custom implementations directly. Store and
// Relational are mandatory (Postgres is the source of truth); every other
// port degrades to a typed ErrStoreNotConfigured.
type Ports struct {
	Store           command.Store
	Relational      query.RelationalQuerier
	Graph           query.GraphQuerier
	Search          query.SearchQuerier
	Timeseries      query.TSQuerier
	Vector          query.VectorQuerier
	Documents       document.Store
	ProjectionState projection.StateReader
}

// Fabriq is the facade implementing query.Fabric: the single object
// application code holds to reach every datastore.
type Fabriq struct {
	reg      *registry.Registry
	exec     *command.Executor
	ports    Ports
	hub      *subscribe.Hub
	gate     *subscribe.Gate
	settings settings
}

var _ query.Fabric = (*Fabriq)(nil)

// New assembles a Fabriq from explicit ports. Most services use Open
// (config-driven adapters) instead; New is the seam for tests, embedding
// and partial deployments.
func New(reg *registry.Registry, ports Ports, opts ...Option) (*Fabriq, error) {
	if reg == nil {
		return nil, fmt.Errorf("fabriq: registry is required")
	}
	if ports.Store == nil {
		return nil, fmt.Errorf("fabriq: command store is required (postgres is the source of truth)")
	}
	if ports.Relational == nil {
		return nil, fmt.Errorf("fabriq: relational querier is required (postgres is the source of truth)")
	}
	if err := reg.Validate(); err != nil {
		return nil, err
	}

	s := defaultSettings()
	for _, opt := range opts {
		opt(&s)
	}

	exec, err := command.NewExecutor(reg, ports.Store, s.executorOptions...)
	if err != nil {
		return nil, err
	}

	return &Fabriq{
		reg:      reg,
		exec:     exec,
		ports:    ports,
		hub:      subscribe.NewHub(subscribe.WithConflationWindow(s.conflationWindow)),
		gate:     subscribe.NewGate(reg, s.authz),
		settings: s,
	}, nil
}

// Registry exposes the schema registry (read-only use).
func (f *Fabriq) Registry() *registry.Registry { return f.reg }

// Hub exposes the subscription hub for the delta pump (fabriq-worker / the
// redis stream bridge) and for shutdown draining. Application code
// subscribes through Subscribe, never directly.
func (f *Fabriq) Hub() *subscribe.Hub { return f.hub }

// Close drains and stops the subscription hub.
func (f *Fabriq) Close() error {
	f.hub.Flush()
	f.hub.Close()
	return nil
}

// Exec implements query.Fabric.
func (f *Fabriq) Exec(ctx context.Context, cmd command.Command) (command.Result, error) {
	return f.exec.Exec(ctx, cmd)
}

// ExecBatch implements query.Fabric.
func (f *Fabriq) ExecBatch(ctx context.Context, cmds []command.Command) ([]command.Result, error) {
	return f.exec.ExecBatch(ctx, cmds)
}

// Relational implements query.Fabric.
func (f *Fabriq) Relational() query.RelationalQuerier { return f.ports.Relational }

// Graph implements query.Fabric.
func (f *Fabriq) Graph() query.GraphQuerier {
	if f.ports.Graph == nil {
		return notConfiguredGraph{}
	}
	return f.ports.Graph
}

// Search implements query.Fabric.
func (f *Fabriq) Search() query.SearchQuerier {
	if f.ports.Search == nil {
		return notConfiguredSearch{}
	}
	return f.ports.Search
}

// Timeseries implements query.Fabric.
func (f *Fabriq) Timeseries() query.TSQuerier {
	if f.ports.Timeseries == nil {
		return notConfiguredTS{}
	}
	return f.ports.Timeseries
}

// Vector implements query.Fabric.
func (f *Fabriq) Vector() query.VectorQuerier {
	if f.ports.Vector == nil {
		return notConfiguredVector{}
	}
	return f.ports.Vector
}

// Document implements query.Fabric.
func (f *Fabriq) Document() document.Store {
	if f.ports.Documents == nil {
		return notConfiguredDocs{}
	}
	return f.ports.Documents
}

// Subscribe implements query.Fabric: authz hook, server-side channel
// resolution, conflated delivery.
func (f *Fabriq) Subscribe(ctx context.Context, scope query.SubscribeScope) (<-chan query.Delta, error) {
	channel, err := f.gate.Resolve(ctx, scope)
	if err != nil {
		return nil, err
	}
	ch, _, err := f.hub.Subscribe(ctx, channel, f.settings.subscribeBuffer)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// WaitForProjection implements query.Fabric by polling the projection
// state port until the aggregate reaches version or ctx expires.
func (f *Fabriq) WaitForProjection(ctx context.Context, proj, aggregate, aggID string, version int64) error {
	tenantID, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	if f.ports.ProjectionState == nil {
		return fmt.Errorf("fabriq: projection state port: %w", ErrStoreNotConfigured)
	}
	ticker := time.NewTicker(f.settings.waitPollInterval)
	defer ticker.Stop()
	for {
		applied, err := f.ports.ProjectionState.AppliedVersion(ctx, tenantID, proj, aggregate, aggID)
		if err != nil {
			return err
		}
		if applied >= version {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("fabriq: projection %s at v%d for %s/%s, wanted v%d: %w",
				proj, applied, aggregate, aggID, version, ErrProjectionLag)
		case <-ticker.C:
		}
	}
}

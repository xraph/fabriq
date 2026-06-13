package fabriq

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/event"
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
	stores   *Stores // set by Open; nil when assembled from explicit ports
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

	hubOpts := []subscribe.HubOption{subscribe.WithConflationWindow(s.conflationWindow)}
	if s.tailer != nil {
		hubOpts = append(hubOpts, subscribe.WithTailer(s.tailer))
	}
	return &Fabriq{
		reg:      reg,
		exec:     exec,
		ports:    ports,
		hub:      subscribe.NewHub(hubOpts...),
		gate:     subscribe.NewGate(reg, s.authz),
		settings: s,
	}, nil
}

// Registry exposes the schema registry (read-only use).
func (f *Fabriq) Registry() *registry.Registry { return f.reg }

// RepoFor returns a type-safe repository over the entity whose grove model
// is T — the typed counterpart to f.Relational(). The entity is resolved
// from T (no string), and reads return *T / []*T:
//
//	repo, _ := fabriq.RepoFor[domain.Asset](f)
//	asset, err := repo.Get(ctx, id)          // *domain.Asset
//	pump, err := repo.One(ctx, query.ListQuery{Where: []query.Cond{query.Eq("serial", sn)}})
//
// It is a free function, not a method, because Go methods cannot introduce
// type parameters.
func RepoFor[T any](f *Fabriq) (*query.Repo[T], error) {
	return query.For[T](f.reg, f.Relational())
}

// Upcasters exposes the registered payload upcaster chain (nil when none)
// — the worker hands it to projection engines.
func (f *Fabriq) Upcasters() *event.UpcasterChain { return f.settings.upcasters }

// Hub exposes the subscription hub for the delta pump (fabriq-worker / the
// redis stream bridge) and for shutdown draining. Application code
// subscribes through Subscribe, never directly.
func (f *Fabriq) Hub() *subscribe.Hub { return f.hub }

// Close drains the subscription hub and, when this Fabriq was built by
// Open, closes the underlying stores.
func (f *Fabriq) Close() error {
	f.hub.Flush()
	f.hub.Close()
	if f.stores != nil {
		return f.stores.Close()
	}
	return nil
}

// CatchUp reads the deltas a reconnecting client missed on a scope's
// channel since afterID (its SSE Last-Event-ID), through the same authz
// gate as Subscribe. An empty slice with no error means the client is
// current; channels are short (MAXLEN~), so callers must treat a full
// page as "refetch instead". Delivery overlap with a live Subscribe is
// possible — consumers dedupe by StreamID.
func (f *Fabriq) CatchUp(ctx context.Context, scope query.SubscribeScope, afterID string, limit int) ([]query.Delta, error) {
	if f.settings.tailer == nil {
		return nil, fmt.Errorf("fabriq: catch-up: %w", ErrStoreNotConfigured)
	}
	channel, err := f.gate.Resolve(ctx, scope)
	if err != nil {
		return nil, err
	}
	return f.settings.tailer.ReadRange(ctx, channel, afterID, limit)
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

package fabriq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xraph/fabriq/adapters/elastic"
	"github.com/xraph/fabriq/adapters/falkordb"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/adapters/redis"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/subscribe"
)

// Open dials the configured stores and assembles a Fabriq:
//
//   - Postgres (required): command store, relational/timeseries/vector
//     ports, projection bookkeeping.
//   - Redis (optional but required for live subscriptions and
//     projections): change-channel tailer feeding the hub.
//   - FalkorDB / Elasticsearch ports come with phases 4–5; until then
//     Graph()/Search() return ErrStoreNotConfigured.
//
// The returned Stores handle also exposes the adapters the worker plane
// needs (relay, elector, consumer groups) — application services should
// only ever hold the *Fabriq.
func Open(ctx context.Context, reg *registry.Registry, cfg Config, opts ...Option) (*Fabriq, *Stores, error) {
	if reg == nil {
		return nil, nil, fmt.Errorf("fabriq: Open needs a registry (register your domain pack first)")
	}
	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}

	pg, err := postgres.Open(ctx, cfg.Postgres.DSN, reg,
		postgres.WithPoolSize(cfg.Postgres.PoolSize),
		postgres.WithGuardedTables(cfg.guardedTables()...),
	)
	if err != nil {
		return nil, nil, err
	}

	stores := &Stores{Postgres: pg}
	ports := Ports{
		Store:           pg,
		Relational:      pg,
		Timeseries:      pg,
		Vector:          pg,
		Documents:       pg.Documents(),
		ProjectionState: pg.ProjectionState(),
	}

	allOpts := append(cfg.Options(), opts...)

	if cfg.Redis.Addr != "" {
		rd, rerr := redis.Open(ctx, redis.Config{
			Addr: cfg.Redis.Addr, DB: cfg.Redis.DB,
			Username: cfg.Redis.Username, Password: cfg.Redis.Password,
		}, redis.WithChannelMaxLen(cfg.Subscriptions.StreamMaxLen))
		if rerr != nil {
			_ = pg.Close()
			return nil, nil, rerr
		}
		stores.Redis = rd
		allOpts = append(allOpts, withTailer(rd))
		// With a transport available, document updates fan out live.
		ports.Documents = &syncingDocStore{DocStore: pg.Documents(), pub: rd, reg: reg}
	}

	if cfg.FalkorDB.Addr != "" {
		fk, ferr := falkordb.Open(ctx, falkordb.Config{
			Addr: cfg.FalkorDB.Addr, Username: cfg.FalkorDB.Username, Password: cfg.FalkorDB.Password,
		}, reg, pg, falkordb.WithLiveTargetResolver(liveGraphResolver(pg.ProjectionState())))
		if ferr != nil {
			_ = stores.Close()
			return nil, nil, ferr
		}
		stores.Falkor = fk
		ports.Graph = fk
	}

	if len(cfg.Elasticsearch.Addrs) > 0 {
		es, eerr := elastic.Open(ctx, elastic.Config{
			Addrs: cfg.Elasticsearch.Addrs, Username: cfg.Elasticsearch.Username, Password: cfg.Elasticsearch.Password,
		}, reg, elastic.WithModelVersionResolver(liveSearchModelVersion(pg.ProjectionState())))
		if eerr != nil {
			_ = stores.Close()
			return nil, nil, eerr
		}
		stores.Elastic = es
		ports.Search = es
	}

	f, err := New(reg, ports, allOpts...)
	if err != nil {
		_ = stores.Close()
		return nil, nil, err
	}
	f.stores = stores
	return f, stores, nil
}

// Stores exposes the opened adapters for worker-plane wiring (relay,
// electors, projection consumers) and shutdown.
type Stores struct {
	Postgres *postgres.Adapter
	Redis    *redis.Adapter
	Falkor   *falkordb.Adapter
	Elastic  *elastic.Adapter
}

// Close releases every opened adapter.
func (s *Stores) Close() error {
	var firstErr error
	if s.Falkor != nil {
		if err := s.Falkor.Close(); err != nil {
			firstErr = err
		}
	}
	if s.Redis != nil {
		if err := s.Redis.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.Postgres != nil {
		if err := s.Postgres.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// GraphEngine assembles the graph projection consumer over the opened
// stores: Redis consumer group -> registry-derived applier -> FalkorDB
// sink, with projection_state bookkeeping and rebuild-aware dual targets.
// Run one per worker replica (consumer groups scale without election).
func (s *Stores) GraphEngine(reg *registry.Registry, upcasters *event.UpcasterChain) (*projection.Engine, error) {
	if s.Redis == nil || s.Falkor == nil || s.Postgres == nil {
		return nil, fmt.Errorf("fabriq: graph engine needs postgres, redis and falkordb configured")
	}
	repo := s.Postgres.ProjectionState()
	return &projection.Engine{
		Projection: "graph",
		Group:      "proj:graph",
		Source:     s.Redis,
		Sink:       s.Falkor,
		Applier:    projection.GraphApplier(reg),
		Upcasters:  upcasters,
		State:      repo,
		TargetsFor: func(ctx context.Context, tenantID string) ([]string, error) {
			st, err := repo.Get(ctx, tenantID, "graph")
			if err != nil {
				return nil, err
			}
			if st.Status == "building" {
				// Live catch-up: feed the building target alongside live.
				return []string{"", registry.GraphNameVersioned(tenantID, st.ModelVersion+1)}, nil
			}
			return []string{""}, nil
		},
	}, nil
}

// GraphRebuilder assembles the blue-green rebuilder for the graph
// projection (used by `fabriq rebuild` and tests).
func (s *Stores) GraphRebuilder(reg *registry.Registry) (*projection.Rebuilder, error) {
	if s.Falkor == nil || s.Postgres == nil {
		return nil, fmt.Errorf("fabriq: graph rebuilder needs postgres and falkordb configured")
	}
	return &projection.Rebuilder{
		Projection: "graph",
		State:      s.Postgres.ProjectionState(),
		Sink:       s.Falkor,
		Applier:    projection.GraphApplier(reg),
		Snapshot:   s.Postgres,
		TargetName: registry.GraphNameVersioned,
	}, nil
}

// SearchEngine assembles the search projection consumer: Redis consumer
// group -> registry-derived applier -> Elasticsearch sink with external
// version gating. Run one per worker replica.
func (s *Stores) SearchEngine(reg *registry.Registry, upcasters *event.UpcasterChain) (*projection.Engine, error) {
	if s.Redis == nil || s.Elastic == nil || s.Postgres == nil {
		return nil, fmt.Errorf("fabriq: search engine needs postgres, redis and elasticsearch configured")
	}
	repo := s.Postgres.ProjectionState()
	return &projection.Engine{
		Projection: "search",
		Group:      "proj:search",
		Source:     s.Redis,
		Sink:       s.Elastic,
		Applier:    projection.SearchApplier(reg),
		Upcasters:  upcasters,
		State:      repo,
		TargetsFor: func(ctx context.Context, tenantID string) ([]string, error) {
			st, err := repo.Get(ctx, tenantID, "search")
			if err != nil {
				return nil, err
			}
			if st.Status == "building" {
				return []string{"", elastic.SearchTargetName(tenantID, st.ModelVersion+1)}, nil
			}
			return []string{""}, nil
		},
	}, nil
}

// SearchRebuilder assembles the blue-green rebuilder for the search
// projection; the alias swap rides the flip (OnFlip).
func (s *Stores) SearchRebuilder(reg *registry.Registry) (*projection.Rebuilder, error) {
	if s.Elastic == nil || s.Postgres == nil {
		return nil, fmt.Errorf("fabriq: search rebuilder needs postgres and elasticsearch configured")
	}
	return &projection.Rebuilder{
		Projection: "search",
		State:      s.Postgres.ProjectionState(),
		Sink:       s.Elastic,
		Applier:    projection.SearchApplier(reg),
		Snapshot:   s.Postgres,
		TargetName: elastic.SearchTargetName,
		OnFlip:     s.Elastic.FlipAliases,
	}, nil
}

// GraphReconciler assembles drift detection + repair for the graph
// projection.
func (s *Stores) GraphReconciler(reg *registry.Registry) (*projection.Reconciler, error) {
	if s.Falkor == nil || s.Postgres == nil {
		return nil, fmt.Errorf("fabriq: graph reconciler needs postgres and falkordb configured")
	}
	return &projection.Reconciler{
		Projection: "graph",
		Registry:   reg,
		Include:    func(ent *registry.Entity) bool { return ent.Spec.GraphNode != "" },
		Truth:      s.Postgres.AggregateVersions,
		Projected:  s.Falkor.AggregateVersions,
		Repair:     s.Postgres.Repair,
	}, nil
}

// SearchReconciler assembles drift detection + repair for the search
// projection.
func (s *Stores) SearchReconciler(reg *registry.Registry) (*projection.Reconciler, error) {
	if s.Elastic == nil || s.Postgres == nil {
		return nil, fmt.Errorf("fabriq: search reconciler needs postgres and elasticsearch configured")
	}
	return &projection.Reconciler{
		Projection: "search",
		Registry:   reg,
		Include:    func(ent *registry.Entity) bool { return ent.Spec.Search.Index != "" },
		Truth:      s.Postgres.AggregateVersions,
		Projected:  s.Elastic.AggregateVersions,
		Repair:     s.Postgres.Repair,
	}, nil
}

// liveSearchModelVersion resolves the live search model version through
// projection_state, with a small TTL cache (same pattern as the graph
// resolver).
func liveSearchModelVersion(repo *postgres.StateRepo) elastic.ModelVersionResolver {
	type entry struct {
		version int
		exp     time.Time
	}
	var mu sync.Mutex
	cache := map[string]entry{}
	const ttl = 2 * time.Second

	return func(ctx context.Context, tenantID string) (int, error) {
		mu.Lock()
		if e, ok := cache[tenantID]; ok && time.Now().Before(e.exp) {
			mu.Unlock()
			return e.version, nil
		}
		mu.Unlock()

		st, err := repo.Get(ctx, tenantID, "search")
		if err != nil {
			return 0, err
		}
		v := st.ModelVersion
		if v < 1 {
			v = 1
		}
		mu.Lock()
		cache[tenantID] = entry{version: v, exp: time.Now().Add(ttl)}
		mu.Unlock()
		return v, nil
	}
}

// liveGraphResolver resolves a tenant's live graph through
// projection_state (blue-green pointer), with a small TTL cache so graph
// reads don't pay a Postgres round-trip each.
func liveGraphResolver(repo *postgres.StateRepo) falkordb.TargetResolver {
	type entry struct {
		name string
		exp  time.Time
	}
	var mu sync.Mutex
	cache := map[string]entry{}
	const ttl = 2 * time.Second

	return func(ctx context.Context, tenantID string) (string, error) {
		mu.Lock()
		if e, ok := cache[tenantID]; ok && time.Now().Before(e.exp) {
			mu.Unlock()
			return e.name, nil
		}
		mu.Unlock()

		st, err := repo.Get(ctx, tenantID, "graph")
		if err != nil {
			return "", err
		}
		name := st.TargetName
		if name == "" {
			name = registry.GraphName(tenantID)
		}
		mu.Lock()
		cache[tenantID] = entry{name: name, exp: time.Now().Add(ttl)}
		mu.Unlock()
		return name, nil
	}
}

// guardedTables lists tenant tables outside RLS that the raw-SQL guard
// must cover. The Timescale readings table is the standing member.
func (c Config) guardedTables() []string {
	return []string{"tag_readings"}
}

// withTailer is the internal option wiring the hub pump (kept off the
// public Option surface; transports are chosen by Open).
func withTailer(t subscribe.Tailer) Option {
	return func(s *settings) { s.tailer = t }
}

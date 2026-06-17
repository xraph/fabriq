package fabriq

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	fcache "github.com/xraph/fabriq/adapters/cache"
	"github.com/xraph/fabriq/adapters/elastic"
	"github.com/xraph/fabriq/adapters/falkordb"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/adapters/redis"
	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/cachequery"
	corecache "github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/subscribe"
)

// Open dials the configured stores and assembles a Fabriq:
//
//   - Postgres (required): command store, relational/timeseries/vector/spatial
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

	// Dial the source-of-truth shard(s). Config.Shards routes tenants across
	// several Postgres databases (ADR 0007); a bare Postgres.DSN is the
	// degenerate one-shard deployment. The facade ports route through the
	// resulting shard.Set, so call sites are identical for 1 or N shards.
	shardCfgs := cfg.Shards
	if len(shardCfgs) == 0 {
		shardCfgs = []ShardConfig{{ID: "0", DSN: cfg.Postgres.DSN, PoolSize: cfg.Postgres.PoolSize}}
	}
	shardPG := make(map[string]*postgres.Adapter, len(shardCfgs))
	closeDialed := func() {
		for _, a := range shardPG {
			_ = a.Close()
		}
	}
	ids := make([]string, 0, len(shardCfgs))
	shardList := make([]shard.Shard, 0, len(shardCfgs))
	for _, sc := range shardCfgs {
		a, oerr := postgres.Open(ctx, sc.DSN, reg,
			postgres.WithPoolSize(sc.PoolSize),
			postgres.WithGuardedTables(cfg.guardedTables()...),
		)
		if oerr != nil {
			closeDialed()
			return nil, nil, oerr
		}
		shardPG[sc.ID] = a
		ids = append(ids, sc.ID)
		shardList = append(shardList, shard.Shard{ID: sc.ID, Store: a, Relational: a, Vector: a, Timeseries: a, Spatial: postgres.NewSpatialAdapter(a)})
	}
	sort.Strings(ids)
	pg := shardPG[ids[0]] // primary: health, migrations CLI, document plane

	var set *shard.Set
	if len(shardList) == 1 {
		set = shard.Single(shardList[0])
	} else {
		var serr error
		set, serr = shard.New(shard.Cached(shard.HashDirectory(ids...), 30*time.Second), shardList...)
		if serr != nil {
			closeDialed()
			return nil, nil, serr
		}
	}

	stores := &Stores{Postgres: pg, Shards: set, shardPG: shardPG, customAppliers: cfg.CustomAppliers}
	stores.state = routingState{stores: stores}
	ports := Ports{
		Store:           shard.NewStore(set),
		Relational:      shard.NewRelational(set),
		Timeseries:      shard.NewTimeseries(set),
		Vector:          shard.NewVector(set),
		Spatial:         shard.NewSpatial(set),
		Documents:       pg.Documents(),
		ProjectionState: stores.state,
	}
	if len(shardList) == 1 {
		// Live queries read snapshots/refills straight from the single shard's
		// adapter (Postgres is the exact-top-N oracle). Multi-shard live
		// routing lands in a later phase; until then LiveQuery returns a typed
		// not-configured error on sharded deployments rather than wrong reads.
		ports.Live = pg.NewLiveStore()
	}

	allOpts := append(cfg.Options(), opts...)

	if cfg.Redis.Addr != "" {
		rd, rerr := redis.Open(ctx, redis.Config{
			Addr: cfg.Redis.Addr, DB: cfg.Redis.DB,
			Username: cfg.Redis.Username, Password: cfg.Redis.Password,
		}, redis.WithChannelMaxLen(cfg.Subscriptions.StreamMaxLen))
		if rerr != nil {
			closeDialed()
			return nil, nil, rerr
		}
		stores.Redis = rd
		allOpts = append(allOpts, withTailer(rd))
		// With a transport available, document updates fan out live. The
		// document plane stays on the primary shard (ADR 0007 step 2).
		ports.Documents = &syncingDocStore{DocStore: pg.Documents(), pub: rd, reg: reg}

		ca, cerr := fcache.Open(ctx, fcache.Config{
			Addr:     cfg.Redis.Addr,
			DB:       cfg.Redis.DB,
			Username: cfg.Redis.Username,
			Password: cfg.Redis.Password,
		})
		if cerr != nil {
			closeDialed()
			return nil, nil, fmt.Errorf("fabriq: open cache: %w", cerr)
		}
		stores.Cache = ca
		// Route relational reads through the opt-in row cache.
		ports.Relational = cachequery.New(ports.Relational, ca, reg)
		// Bust cached reads of any entity a committed write touches.
		allOpts = append(allOpts, func(s *settings) {
			s.executorOptions = append(s.executorOptions,
				command.WithPostCommitHooks(newCacheInvalidator(ca)))
		})
	}

	if cfg.FalkorDB.Addr != "" {
		// Graph hydration (TraverseAndHydrate) routes by tenant, so it reads
		// each tenant's rows from its own shard; the live-target resolver
		// reads projection_state from the tenant's shard too.
		fk, ferr := falkordb.Open(ctx, falkordb.Config{
			Addr: cfg.FalkorDB.Addr, Username: cfg.FalkorDB.Username, Password: cfg.FalkorDB.Password,
		}, reg, ports.Relational, falkordb.WithLiveTargetResolver(liveGraphResolver(stores.state)))
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
		}, reg, elastic.WithModelVersionResolver(liveSearchModelVersion(stores.state)))
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
	if ds, ok := ports.Documents.(*syncingDocStore); ok {
		ds.authz = f.settings.docAuthz
	}
	return f, stores, nil
}

// Stores exposes the opened adapters for worker-plane wiring (relay,
// electors, projection consumers) and shutdown.
type Stores struct {
	// Postgres is the PRIMARY shard: health checks, the migrations CLI, and
	// the document plane (which stays single-shard in step 2) use it. For
	// per-tenant work use Shards / ShardPGs.
	Postgres *postgres.Adapter
	Redis    *redis.Adapter
	Falkor   *falkordb.Adapter
	Elastic  *elastic.Adapter
	// Cache is the engine cache (nil when Redis is not configured).
	Cache corecache.Cache
	// Shards is the tenant -> source-of-truth routing table backing the
	// facade's relational/command/vector/timeseries/spatial ports (ADR 0007).
	Shards *shard.Set

	// shardPG is the concrete per-shard adapter behind each Shards id, for
	// the worker plane (per-shard relay, tenant-routed reconcile/snapshot).
	shardPG map[string]*postgres.Adapter
	// state is the tenant-routed projection bookkeeping the engines,
	// rebuilder and WaitForProjection read through. The concrete type
	// satisfies StateReader, StateRepo AND AppliedRecorder, which the
	// various consumers each require a different subset of.
	state routingState
	// customAppliers are consumer-supplied projection appliers passed in via
	// Config.CustomAppliers. They are forwarded to all four engine/rebuilder
	// constructors so live and rebuilt projections stay identical.
	customAppliers []projection.CustomApplier
}

// Close releases every opened adapter (every shard, plus the projections).
func (s *Stores) Close() error {
	var firstErr error
	if s.Falkor != nil {
		if err := s.Falkor.Close(); err != nil {
			firstErr = err
		}
	}
	if s.Cache != nil {
		if err := s.Cache.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.Redis != nil {
		if err := s.Redis.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, a := range s.shardPG {
		if err := a.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if len(s.shardPG) == 0 && s.Postgres != nil {
		if err := s.Postgres.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// cachedState wraps projection_state lookups with a small TTL cache —
// the engines consult state per consumed EVENT, which must not cost a
// Postgres round-trip each. 2s staleness only widens the dual-apply
// window at a rebuild's edges; version gating keeps that safe.
func cachedState(repo projection.StateRepo, proj string) func(ctx context.Context, tenantID string) (projection.State, error) {
	type entry struct {
		st  projection.State
		exp time.Time
	}
	var mu sync.Mutex
	cache := map[string]entry{}
	const ttl = 2 * time.Second

	return func(ctx context.Context, tenantID string) (projection.State, error) {
		mu.Lock()
		if e, ok := cache[tenantID]; ok && time.Now().Before(e.exp) {
			mu.Unlock()
			return e.st, nil
		}
		mu.Unlock()
		st, err := repo.Get(ctx, tenantID, proj)
		if err != nil {
			return projection.State{}, err
		}
		mu.Lock()
		cache[tenantID] = entry{st: st, exp: time.Now().Add(ttl)}
		mu.Unlock()
		return st, nil
	}
}

// GraphEngine assembles the graph projection consumer over the opened
// stores: Redis consumer group -> registry-derived applier -> FalkorDB
// sink, with projection_state bookkeeping and rebuild-aware dual targets.
// Run one per worker replica (consumer groups scale without election).
func (s *Stores) GraphEngine(reg *registry.Registry, upcasters *event.UpcasterChain) (*projection.Engine, error) {
	if s.Redis == nil || s.Falkor == nil || s.Postgres == nil {
		return nil, fmt.Errorf("fabriq: graph engine needs postgres, redis and falkordb configured")
	}
	repo := s.state
	stateFor := cachedState(repo, "graph")
	return &projection.Engine{
		Projection: "graph",
		Group:      "proj:graph",
		Source:     s.Redis,
		Sink:       s.Falkor,
		Applier:    projection.GraphApplier(reg),
		Upcasters:  upcasters,
		State:      repo,
		Custom:     s.customAppliers,
		TargetsFor: func(ctx context.Context, tenantID string) ([]string, error) {
			st, err := stateFor(ctx, tenantID)
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
		State:      s.state,
		Sink:       s.Falkor,
		Applier:    projection.GraphApplier(reg),
		Snapshot:   routingSnapshot{stores: s},
		TargetName: registry.GraphNameVersioned,
		Custom:     s.customAppliers,
	}, nil
}

// SearchEngine assembles the search projection consumer: Redis consumer
// group -> registry-derived applier -> Elasticsearch sink with external
// version gating. Run one per worker replica.
func (s *Stores) SearchEngine(reg *registry.Registry, upcasters *event.UpcasterChain) (*projection.Engine, error) {
	if s.Redis == nil || s.Elastic == nil || s.Postgres == nil {
		return nil, fmt.Errorf("fabriq: search engine needs postgres, redis and elasticsearch configured")
	}
	repo := s.state
	stateFor := cachedState(repo, "search")
	return &projection.Engine{
		Projection: "search",
		Group:      "proj:search",
		Source:     s.Redis,
		Sink:       s.Elastic,
		Applier:    projection.SearchApplier(reg),
		Upcasters:  upcasters,
		State:      repo,
		Custom:     s.customAppliers,
		TargetsFor: func(ctx context.Context, tenantID string) ([]string, error) {
			st, err := stateFor(ctx, tenantID)
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
		State:      s.state,
		Sink:       s.Elastic,
		Applier:    projection.SearchApplier(reg),
		Snapshot:   routingSnapshot{stores: s},
		TargetName: elastic.SearchTargetName,
		OnFlip:     s.Elastic.FlipAliases,
		Custom:     s.customAppliers,
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
		Truth:      s.truthVersions, // tenant-routed to the owning shard
		Projected:  s.Falkor.AggregateVersions,
		Repair:     s.repair, // synthetic event lands on the tenant's shard outbox
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
		Truth:      s.truthVersions, // tenant-routed to the owning shard
		Projected:  s.Elastic.AggregateVersions,
		Repair:     s.repair, // synthetic event lands on the tenant's shard outbox
	}, nil
}

// liveSearchModelVersion resolves the live search model version through
// projection_state, with a small TTL cache (same pattern as the graph
// resolver).
func liveSearchModelVersion(repo projection.StateRepo) elastic.ModelVersionResolver {
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
func liveGraphResolver(repo projection.StateRepo) falkordb.TargetResolver {
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

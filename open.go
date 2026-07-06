package fabriq

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"encoding/base64"

	fcache "github.com/xraph/fabriq/adapters/cache"
	"github.com/xraph/fabriq/adapters/elastic"
	"github.com/xraph/fabriq/adapters/falkordb"
	"github.com/xraph/fabriq/adapters/pganalytics"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/adapters/redis"
	"github.com/xraph/fabriq/adapters/shard"
	trovestore "github.com/xraph/fabriq/adapters/trove"
	"github.com/xraph/fabriq/cachequery"
	"github.com/xraph/fabriq/core/analytics"
	corecache "github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/crypto"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/subscribe"
	"github.com/xraph/fabriq/core/sweep"
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

	// Catalog mode (db-per-tenant) is a separate assembly: no boot-time
	// shard list — the catalog is the routing authority and tenant
	// databases dial lazily.
	if cfg.Catalog.Enabled() {
		return openCatalogMode(ctx, reg, cfg, opts...)
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
		var a *postgres.Adapter
		var oerr error
		if cfg.primaryGrove != nil && len(cfg.Shards) == 0 {
			// Single-shard deployment backed by a grove.DB resolved from a host
			// DI container: borrow it instead of dialing a DSN. fabriq never
			// closes a borrowed handle (the host owns its lifecycle).
			a, oerr = postgres.OpenWithGrove(cfg.primaryGrove, reg,
				postgres.WithGuardedTables(cfg.guardedTables()...),
			)
		} else {
			a, oerr = postgres.Open(ctx, sc.DSN, reg,
				postgres.WithPoolSize(sc.PoolSize),
				postgres.WithGuardedTables(cfg.guardedTables()...),
			)
		}
		if oerr != nil {
			closeDialed()
			return nil, nil, oerr
		}
		shardPG[sc.ID] = a
		ids = append(ids, sc.ID)
		shardList = append(shardList, shard.Shard{ID: sc.ID, Store: a, Relational: a, Vector: postgres.NewVectorAdapter(a), Timeseries: a, Spatial: postgres.NewSpatialAdapter(a)})
	}
	sort.Strings(ids)
	pg := shardPG[ids[0]] // primary: health, migrations CLI, document plane

	var set *shard.Set
	if len(shardList) == 1 {
		set = shard.Single(shardList[0])
	} else {
		var serr error
		set, serr = shard.New(shard.Cached(shardDirectory(ids, cfg.ShardPins), 30*time.Second), shardList...)
		if serr != nil {
			closeDialed()
			return nil, nil, serr
		}
	}

	if err := validateArchiveConfig(cfg, reg); err != nil {
		closeDialed()
		return nil, nil, err
	}

	stores := &Stores{Postgres: pg, Shards: set, shardPG: shardPG, customAppliers: cfg.CustomAppliers}
	stores.state = routingState{stores: stores}
	docStore := pg.Documents()
	stores.Docs = docStore
	ports := Ports{
		Store:           shard.NewStore(set),
		Relational:      shard.NewRelational(set),
		Timeseries:      shard.NewTimeseries(set),
		Vector:          shard.NewVector(set),
		Spatial:         shard.NewSpatial(set),
		Documents:       docStore,
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

	if cfg.Encryption.Key != "" {
		keyBytes, derr := base64.StdEncoding.DecodeString(cfg.Encryption.Key)
		if derr != nil {
			_ = stores.Close()
			return nil, nil, fmt.Errorf("fabriq: decode encryption key: %w", derr)
		}
		enc, eerr := crypto.NewAESGCM(keyBytes)
		if eerr != nil {
			_ = stores.Close()
			return nil, nil, fmt.Errorf("fabriq: encryption: %w", eerr)
		}
		allOpts = append(allOpts, WithEncryptor(enc))
	}

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
		ports.Documents = &syncingDocStore{seqDocStore: docStore, pub: rd, reg: reg}

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
		// Wrap the shared L2 cache with an optional per-node L1 LRU tier.
		// When disabled (default), engineCache == ca and behaviour is P1-P3.
		var engineCache corecache.Cache = ca
		if cfg.Cache.L1Enabled {
			size := cfg.Cache.L1Size
			if size <= 0 {
				size = 10000
			}
			ttl := cfg.Cache.L1TTL
			if ttl <= 0 {
				ttl = 5 * time.Minute
			}
			l1 := fcache.NewL1(ca, reg, size, ttl)
			engineCache = l1
			// Per-node broadcast eviction: tails the event stream and calls
			// l1.EvictLocal for each committed change. The goroutine is
			// detached from the request ctx (WithoutCancel) so it outlives
			// individual requests, but remains cancellable via tctx so that
			// Stores.Close() can stop it before tearing down the connection.
			tctx, cancel := context.WithCancel(context.WithoutCancel(ctx)) // #nosec G118 -- cancel is retained in cancelFns and invoked by Stores.Close()
			// Cold-start window: commits between Open() returning and the tailer's
			// first XRead attach are missed on this node and bounded by L1TTL (not
			// stream lag). L1 is opt-in and TTL-backstopped; acceptable for P4a.
			go func() { _ = fcache.RunL1EvictTailer(tctx, rd, l1) }()
			stores.cancelFns = append(stores.cancelFns, cancel)
		}
		stores.Cache = engineCache
		ports.Cache = engineCache
		// Route relational reads through the opt-in row cache.
		ports.Relational = cachequery.New(ports.Relational, engineCache, reg)
		// Bust cached reads of any entity a committed write touches.
		allOpts = append(allOpts, func(s *settings) {
			s.executorOptions = append(s.executorOptions,
				command.WithPostCommitHooks(newCacheInvalidator(engineCache)))
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

	if err := openAnalytics(ctx, cfg, stores); err != nil {
		_ = stores.Close()
		return nil, nil, err
	}

	if cfg.Storage.StorageDriver != "" {
		ba, berr := trovestore.Open(ctx, trovestore.Config{
			StorageDriver: cfg.Storage.StorageDriver,
			DefaultBucket: cfg.Storage.DefaultBucket,
		})
		if berr != nil {
			_ = stores.Close()
			return nil, nil, fmt.Errorf("fabriq: open storage: %w", berr)
		}
		stores.Blob = ba
		ports.Blob = ba
		docStore.EnableArchive(ba, cfg.Documents.ArchiveHistory)
		if cfg.Storage.EnableCas {
			cs := trovestore.NewCASStore(ba.Driver(), trovestore.NewCASIndex(pg), cfg.Storage.DefaultBucket)
			stores.CAS = cs
			ports.CAS = cs
		}
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

// openAnalytics validates and dials the analytics sink into stores when
// configured. Shared by Open and openCatalogMode so the sink is available
// in every tenancy mode. No-op (nil sink) when analytics is disabled.
func openAnalytics(ctx context.Context, cfg Config, stores *Stores) error {
	if !cfg.Analytics.Enabled() {
		return nil
	}
	if err := ValidateAnalyticsConfig(cfg); err != nil {
		return err
	}
	as, err := pganalytics.Open(ctx, cfg.Analytics.DSN)
	if err != nil {
		return err
	}
	stores.Analytics = as
	return nil
}

// validateArchiveConfig fails fast when CRDT history offload is requested —
// globally or by any document entity — but no blob storage is configured.
// Silent degradation would keep the DB bloating exactly when offload was asked
// for, so this is a hard error at Open.
func validateArchiveConfig(cfg Config, reg *registry.Registry) error {
	requested := cfg.Documents.ArchiveHistory
	if !requested {
		for _, ent := range reg.All() {
			if ent.Spec.CRDT != nil && ent.Spec.CRDT.ArchiveHistory != nil && *ent.Spec.CRDT.ArchiveHistory {
				requested = true
				break
			}
		}
	}
	if requested && cfg.Storage.StorageDriver == "" {
		return fmt.Errorf("fabriq: Documents.ArchiveHistory requires Storage to be configured (no storage driver set)")
	}
	return nil
}

// shardDirectory builds the tenant→shard directory for a multi-shard set:
// hash placement, with config-pinned tenants (Config.ShardPins) overriding.
// Validate has already checked every pin against the configured shard ids, so
// the pins are trusted here. The caller keeps the Cached wrapper outermost.
func shardDirectory(ids []string, pins map[string]string) shard.Directory {
	dir := shard.HashDirectory(ids...)
	if len(pins) > 0 {
		dir = shard.PinnedDirectory(pins, dir)
	}
	return dir
}

// Stores exposes the opened adapters for worker-plane wiring (relay,
// electors, projection consumers) and shutdown.
type Stores struct {
	// Postgres is the PRIMARY shard: health checks, the migrations CLI, and
	// the document plane (which stays single-shard in step 2) use it. For
	// per-tenant work use Shards / ShardPGs.
	Postgres *postgres.Adapter
	// Catalog is the db-per-tenant control plane (nil outside catalog mode).
	Catalog *postgres.CatalogStore
	// pool owns catalog-mode tenant pools (nil outside catalog mode).
	pool *shard.PoolManager
	// router is the catalog-mode DynamicSet the sweeper acquires tenant
	// shards through (nil outside catalog mode).
	router shard.Router
	// Docs is the document-plane store on the primary shard, with history
	// archiving wired when Storage is configured. The worker plane must use
	// this instance — Postgres.Documents() mints a fresh store without the
	// blob handle, which would silently skip history sealing on compaction.
	Docs    *postgres.DocStore
	Redis   *redis.Adapter
	Falkor  *falkordb.Adapter
	Elastic *elastic.Adapter
	// Analytics is the opt-in cross-tenant analytics sink (nil unless
	// Config.Analytics is configured).
	Analytics analytics.Sink
	Blob      *trovestore.Adapter // nil when Storage not configured
	// CAS is the content-addressable store (nil when EnableCas is false).
	CAS *trovestore.CASStore
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
	// cancelFns stop background workers (e.g. the L1 evict tailer) on Close.
	cancelFns []func()
}

// TenantSweeper returns the catalog-mode per-tenant maintenance seam for
// the sweep engine: acquire the tenant's shard through the router (which
// enforces the catalog's active-state and version gates), run one
// claim-guarded pass, release. Nil outside catalog mode.
func (s *Stores) TenantSweeper() sweep.TenantSweeper {
	if s.router == nil {
		return nil
	}
	return func(ctx context.Context, tenantID string, compact bool) (sweep.Result, error) {
		sh, release, err := s.router.AcquireFor(ctx, tenantID)
		if err != nil {
			return sweep.Result{}, err
		}
		defer release()
		if sh.Maintenance == nil {
			return sweep.Result{}, nil
		}
		return sh.Maintenance.Sweep(ctx, compact)
	}
}

// PoolStats reports the catalog-mode shard pool's live counters
// (observability). ok is false outside catalog mode.
func (s *Stores) PoolStats() (open, held int, ok bool) {
	if s.pool == nil {
		return 0, 0, false
	}
	open, held = s.pool.Stats()
	return open, held, true
}

// Close releases every opened adapter (every shard, plus the projections).
func (s *Stores) Close() error {
	// Cancel background workers first so they stop blocking on their
	// connections before we close those connections below.
	for _, cancel := range s.cancelFns {
		cancel()
	}
	var firstErr error
	if s.Blob != nil {
		if err := s.Blob.Close(context.Background()); err != nil {
			firstErr = err
		}
	}
	if s.Falkor != nil {
		if err := s.Falkor.Close(); err != nil {
			firstErr = err
		}
	}
	if s.Analytics != nil {
		if err := s.Analytics.Close(); err != nil && firstErr == nil {
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
	if s.pool != nil {
		if err := s.pool.CloseAll(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.Catalog != nil {
		if err := s.Catalog.Close(); err != nil && firstErr == nil {
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
	if s.Redis == nil || s.Falkor == nil || (s.Postgres == nil && s.router == nil) {
		return nil, fmt.Errorf("fabriq: graph engine needs postgres (or a tenant catalog), redis and falkordb configured")
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

// AnalyticsConsumer assembles the proj:analytics consumer: the shared Redis
// stream -> registry-driven applier -> analytics sink. Run one per worker
// replica. Requires redis and a configured analytics sink.
func (s *Stores) AnalyticsConsumer(reg *registry.Registry, upcasters *event.UpcasterChain) (*analytics.Consumer, error) {
	if s.Redis == nil || s.Analytics == nil {
		return nil, fmt.Errorf("fabriq: analytics consumer needs redis and an analytics sink configured")
	}
	return &analytics.Consumer{
		Group:     "proj:analytics",
		Source:    s.Redis,
		Applier:   analytics.NewApplier(reg),
		Sink:      s.Analytics,
		Upcasters: upcasters,
	}, nil
}

// AnalyticsBackfiller assembles a backfiller that replays each tenant's
// current-state snapshot (routed per tenant, all tenancy modes) through the
// analytics applier into the sink. Requires a configured analytics sink.
func (s *Stores) AnalyticsBackfiller(reg *registry.Registry) (*analytics.Backfiller, error) {
	if s.Analytics == nil {
		return nil, fmt.Errorf("fabriq: analytics backfiller needs an analytics sink configured")
	}
	return &analytics.Backfiller{
		Snapshot: routingSnapshot{stores: s}.SnapshotEntities,
		Applier:  analytics.NewApplier(reg),
		Sink:     s.Analytics,
	}, nil
}

// GraphRebuilder assembles the blue-green rebuilder for the graph
// projection (used by `fabriq rebuild` and tests).
func (s *Stores) GraphRebuilder(reg *registry.Registry) (*projection.Rebuilder, error) {
	if s.Falkor == nil || (s.Postgres == nil && s.router == nil) {
		return nil, fmt.Errorf("fabriq: graph rebuilder needs postgres (or a tenant catalog) and falkordb configured")
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
	if s.Redis == nil || s.Elastic == nil || (s.Postgres == nil && s.router == nil) {
		return nil, fmt.Errorf("fabriq: search engine needs postgres (or a tenant catalog), redis and elasticsearch configured")
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
	if s.Elastic == nil || (s.Postgres == nil && s.router == nil) {
		return nil, fmt.Errorf("fabriq: search rebuilder needs postgres (or a tenant catalog) and elasticsearch configured")
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
	if s.Falkor == nil || (s.Postgres == nil && s.router == nil) {
		return nil, fmt.Errorf("fabriq: graph reconciler needs postgres (or a tenant catalog) and falkordb configured")
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
	if s.Elastic == nil || (s.Postgres == nil && s.router == nil) {
		return nil, fmt.Errorf("fabriq: search reconciler needs postgres (or a tenant catalog) and elasticsearch configured")
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

// BlobReconciler assembles the per-tenant blob CAS reconciler (ref-count
// recompute, byte GC, broken-row + orphan detection). Returns an error when
// the CAS layer is not configured (Storage.EnableCas false).
func (s *Stores) BlobReconciler(grace time.Duration) (*trovestore.BlobReconciler, error) {
	if s.CAS == nil || s.Postgres == nil {
		return nil, fmt.Errorf("fabriq: blob reconciler needs storage with enableCas and postgres configured")
	}
	return trovestore.NewBlobReconciler(s.CAS, s.Postgres, grace), nil
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

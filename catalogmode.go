package fabriq

import (
	"context"
	"fmt"
	"strings"

	"github.com/xraph/fabriq/adapters/elastic"
	"github.com/xraph/fabriq/adapters/falkordb"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/adapters/redis"
	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/migrations"
)

// openCatalogMode assembles the db-per-tenant deployment (spec 2026-07-03):
// every port routes through a DynamicSet whose directory is the tenant
// catalog and whose shards are lazily-dialed per-tenant databases. The
// worker plane is the catalog sweeper (forgeext), fed by Stores.TenantSweeper
// and woken by the write path's Redis nudges.
//
// Projections (graph/search) serve too: the sinks are shared, the
// bookkeeping routes per tenant, the sweeper relays every tenant's outbox
// into the shared stream the engines consume.
//
// v1 serving scope (each gap fails fast in validateCatalogMode and is
// recorded in the spec's deviation log):
//   - document history archiving (blob offload) is not wired yet;
//   - live queries return the same typed not-configured error as static
//     multi-shard deployments (routed live stores are a later phase);
//   - blue-green rebuilds and the drift reconciler stay static-mode-only
//     (they need routed snapshot/truth surfaces).
func openCatalogMode(ctx context.Context, reg *registry.Registry, cfg Config, opts ...Option) (*Fabriq, *Stores, error) {
	if err := validateCatalogMode(cfg); err != nil {
		return nil, nil, err
	}

	catStore, err := postgres.OpenCatalog(ctx, cfg.Catalog.DSN)
	if err != nil {
		return nil, nil, err
	}
	clusterOps := postgres.NewClusterOps(cfg.Catalog.ClusterDSNs)

	dir := shard.CatalogDirectory(catStore, cfg.Catalog.CacheTTL,
		shard.WithMinVersion(migrations.HeadVersion()))

	stores := &Stores{Catalog: catStore, customAppliers: cfg.CustomAppliers}
	stores.state = routingState{stores: stores}

	// Redis opens before the dialer: each tenant shard's maintenance
	// surface relays its outbox through this one shared transport.
	var rd *redis.Adapter
	if cfg.Redis.Addr != "" {
		rd, err = redis.Open(ctx, redis.Config{
			Addr: cfg.Redis.Addr, DB: cfg.Redis.DB,
			Username: cfg.Redis.Username, Password: cfg.Redis.Password,
		}, redis.WithChannelMaxLen(cfg.Subscriptions.StreamMaxLen))
		if err != nil {
			_ = catStore.Close()
			return nil, nil, err
		}
		stores.Redis = rd
	}

	// The dialer opens one tenant database's full adapter stack; the pool
	// manager owns its lifetime (LRU idle eviction, breaker).
	dialer := func(dctx context.Context, shardID string) (shard.Shard, func() error, error) {
		clusterID, database, ok := strings.Cut(shardID, "/")
		if !ok {
			return shard.Shard{}, nil, fabriqerr.New(fabriqerr.CodeInvalidInput,
				"malformed shard id.", fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"shard": shardID}}))
		}
		dsn, derr := clusterOps.TenantDSN(clusterID, database)
		if derr != nil {
			return shard.Shard{}, nil, derr
		}
		a, oerr := postgres.Open(dctx, dsn, reg,
			postgres.WithPoolSize(cfg.Postgres.PoolSize),
			postgres.WithGuardedTables(cfg.guardedTables()...),
		)
		if oerr != nil {
			return shard.Shard{}, nil, oerr
		}
		var pub event.Publisher // stays a nil interface when Redis is off
		if rd != nil {
			pub = rd
		}
		return shard.Shard{
			ID: shardID, Store: a, Relational: a,
			Vector: postgres.NewVectorAdapter(a), Timeseries: a,
			Spatial:     postgres.NewSpatialAdapter(a),
			Documents:   a.Documents(),
			Maintenance: postgres.NewMaintenance(a, reg, pub),
			Projection:  a.ProjectionState(),
		}, a.Close, nil
	}

	pm := shard.NewPoolManager(dialer, shard.PoolManagerConfig{
		MaxActive: cfg.Catalog.MaxActiveShards,
	})
	dset := shard.NewDynamicSet(dir, pm)
	stores.pool = pm
	stores.router = dset

	docRouter := shard.NewDocuments(dset)
	ports := Ports{
		Store:      shard.NewStore(dset),
		Relational: shard.NewRelational(dset),
		Timeseries: shard.NewTimeseries(dset),
		Vector:     shard.NewVector(dset),
		Spatial:    shard.NewSpatial(dset),
		Documents:  docRouter,
		// Projection bookkeeping routes on its tenant argument to the
		// tenant's own database (WaitForProjection, engine state).
		ProjectionState: stores.state,
	}

	// Projection sinks are SHARED infrastructure (one graph store / search
	// cluster, tenant-scoped by naming) — only the bookkeeping is
	// per-tenant. Hydration and live-target resolution take routed ports,
	// so the identical wiring serves 1 database or 10k.
	if cfg.FalkorDB.Addr != "" {
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

	allOpts := append(cfg.Options(), opts...)

	if rd != nil {
		allOpts = append(allOpts, withTailer(rd))
		// The write path nudges the sweeper so a busy tenant's outbox is
		// relayed within one pass, not one backoff window. Best-effort:
		// the sweep cadence is the delivery guarantee.
		wake := func(wctx context.Context, tenantID string) {
			_ = rd.PublishWake(wctx, tenantID)
		}
		ports.Store = &nudgingStore{Store: ports.Store, wake: wake}
		// Per-tenant documents fan out live exactly like the static plane,
		// and nudge the materializer.
		ports.Documents = &syncingDocStore{seqDocStore: docRouter, pub: rd, reg: reg, wake: wake}
	}

	f, ferr := New(reg, ports, allOpts...)
	if ferr != nil {
		_ = stores.Close()
		return nil, nil, ferr
	}
	if ds, ok := ports.Documents.(*syncingDocStore); ok {
		ds.authz = f.settings.docAuthz
	}
	f.stores = stores
	return f, stores, nil
}

// nudgingStore wakes the catalog-mode sweeper after every committed
// command transaction — the outbox has rows exactly when a tx commits.
type nudgingStore struct {
	command.Store
	wake func(ctx context.Context, tenantID string)
}

func (s *nudgingStore) InTenantTx(ctx context.Context, fn func(ctx context.Context, tx command.Tx) error) error {
	err := s.Store.InTenantTx(ctx, fn)
	if err == nil {
		if tid, terr := tenant.Require(ctx); terr == nil {
			s.wake(ctx, tid)
		}
	}
	return err
}

// validateCatalogMode rejects configuration the v1 serving path cannot
// honor yet — failing fast beats silently-dead subsystems.
func validateCatalogMode(cfg Config) error {
	var missing []string
	if cfg.Documents.ArchiveHistory {
		missing = append(missing, "document history archiving")
	}
	if (cfg.Projections.Graph || cfg.Projections.Search) && cfg.Redis.Addr == "" {
		missing = append(missing, "projections without Redis (the sweeper relays through it)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("fabriq: catalog mode does not support %s yet", strings.Join(missing, ", "))
	}
	return nil
}

package fabriq

import (
	"context"
	"fmt"
	"strings"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/adapters/redis"
	"github.com/xraph/fabriq/adapters/shard"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/migrations"
)

// openCatalogMode assembles the db-per-tenant deployment (spec 2026-07-03):
// every port routes through a DynamicSet whose directory is the tenant
// catalog and whose shards are lazily-dialed per-tenant databases.
//
// v1 serving scope (each gap fails fast in validateCatalogMode and is
// recorded in the spec's deviation log):
//   - projections (graph/search), the outbox relay and live queries wait
//     for the catalog-mode sweeper — commands still write their outbox
//     rows transactionally, so history is intact when the relay lands;
//   - document history archiving (blob offload) is not wired yet;
//   - the worker plane refuses to start (forgeext guards on a nil
//     Stores.Postgres).
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
		return shard.Shard{
			ID: shardID, Store: a, Relational: a,
			Vector: postgres.NewVectorAdapter(a), Timeseries: a,
			Spatial:   postgres.NewSpatialAdapter(a),
			Documents: a.Documents(),
		}, a.Close, nil
	}

	pm := shard.NewPoolManager(dialer, shard.PoolManagerConfig{
		MaxActive: cfg.Catalog.MaxActiveShards,
	})
	dset := shard.NewDynamicSet(dir, pm)

	stores := &Stores{Catalog: catStore, pool: pm, customAppliers: cfg.CustomAppliers}
	stores.state = routingState{stores: stores}

	docRouter := shard.NewDocuments(dset)
	ports := Ports{
		Store:      shard.NewStore(dset),
		Relational: shard.NewRelational(dset),
		Timeseries: shard.NewTimeseries(dset),
		Vector:     shard.NewVector(dset),
		Spatial:    shard.NewSpatial(dset),
		Documents:  docRouter,
		// ProjectionState reads route like everything else once the
		// sweeper lands; until then the not-configured default applies.
	}

	allOpts := append(cfg.Options(), opts...)

	if cfg.Redis.Addr != "" {
		rd, rerr := redis.Open(ctx, redis.Config{
			Addr: cfg.Redis.Addr, DB: cfg.Redis.DB,
			Username: cfg.Redis.Username, Password: cfg.Redis.Password,
		}, redis.WithChannelMaxLen(cfg.Subscriptions.StreamMaxLen))
		if rerr != nil {
			_ = catStore.Close()
			return nil, nil, rerr
		}
		stores.Redis = rd
		allOpts = append(allOpts, withTailer(rd))
		// Per-tenant documents fan out live exactly like the static plane.
		ports.Documents = &syncingDocStore{seqDocStore: docRouter, pub: rd, reg: reg}
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

// validateCatalogMode rejects configuration the v1 serving path cannot
// honor yet — failing fast beats silently-dead subsystems.
func validateCatalogMode(cfg Config) error {
	var missing []string
	if cfg.Projections.Graph || cfg.Projections.Search {
		missing = append(missing, "projections (graph/search need the catalog-mode sweeper)")
	}
	if cfg.Documents.ArchiveHistory {
		missing = append(missing, "document history archiving")
	}
	if len(missing) > 0 {
		return fmt.Errorf("fabriq: catalog mode does not support %s yet", strings.Join(missing, ", "))
	}
	return nil
}

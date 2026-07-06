package fabriq

import (
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/projection"
)

// catalogSharedSchemaPattern bounds the consolidation shared-schema name to a
// safe bare Postgres identifier (it is interpolated into DDL at bootstrap).
var catalogSharedSchemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Config is fabriq's declarative configuration: which stores exist and
// which projections run. It is the same schema a future standalone
// `fabriqd` loads from YAML, hence the yaml tags. Entities are not
// configured here — they are registered in code via the registry.
type Config struct {
	Postgres PostgresConfig `yaml:"postgres" json:"postgres"`
	Shards   []ShardConfig  `yaml:"shards" json:"shards"`
	// ShardPins pins specific tenants to specific shards (tenant id → shard
	// id), overriding hash placement — for residency / high-value tenants that
	// must live on a known shard. Unpinned tenants keep hashing. Requires
	// Shards; every pin must name a configured shard id (Validate enforces
	// both). Like Shards it is config.yaml-only: the FABRIQ_* env overlay
	// cannot express a map. Note the document plane stays on the primary shard
	// regardless of pinning (ADR 0007 step 2).
	ShardPins map[string]string `yaml:"shardPins" json:"shardPins"`
	// Catalog enables db-per-tenant CATALOG MODE (spec 2026-07-03): each
	// tenant owns a dedicated database, routed by the control-plane catalog
	// instead of hash placement. Mutually exclusive with Shards/ShardPins.
	// Like Shards it is config.yaml-only (the env overlay cannot express
	// the cluster map).
	Catalog CatalogConfig `yaml:"catalog" json:"catalog"`
	// Analytics enables the opt-in cross-tenant analytics sink (spec
	// 2026-07-03). Empty DSN = disabled. The DSN MUST point at a database
	// separate from tenant DBs and the catalog control DB.
	Analytics     AnalyticsConfig     `yaml:"analytics" json:"analytics"`
	Redis         RedisConfig         `yaml:"redis" json:"redis"`
	FalkorDB      FalkorDBConfig      `yaml:"falkordb" json:"falkordb"`
	Elasticsearch ElasticsearchConfig `yaml:"elasticsearch" json:"elasticsearch"`
	Storage       StorageConfig       `yaml:"storage" json:"storage"`
	Documents     DocumentsConfig     `yaml:"documents" json:"documents"`
	Projections   ProjectionsConfig   `yaml:"projections" json:"projections"`
	Subscriptions SubscriptionsConfig `yaml:"subscriptions" json:"subscriptions"`
	Cache         CacheConfig         `yaml:"cache" json:"cache"`
	Encryption    EncryptionConfig    `yaml:"encryption" json:"encryption"`
	// CustomAppliers are consumer-supplied projection appliers, unioned after
	// the built-in declarative applier for their Target. They MUST be pure (see
	// projection.CustomApplier). The same set feeds both the live engines and
	// the rebuilders, so live and rebuilt projections stay identical.
	CustomAppliers []projection.CustomApplier `yaml:"-" json:"-"`

	// primaryGrove, when set, backs the single primary shard with a borrowed
	// *grove.DB (resolved from a host DI container by the forge extension)
	// instead of dialing Postgres.DSN — the same way xraph/authsome borrows the
	// shared grove. Postgres.DSN/Shards then become optional. Set it via
	// WithInjectedGrove; it is never serialized and never closed by fabriq (the
	// host owns the connection lifecycle). It applies only to single-shard
	// deployments (Shards must be empty).
	primaryGrove *grove.DB
}

// WithInjectedGrove returns a copy of c whose primary shard is backed by the
// given borrowed grove.DB rather than a dialed DSN. The forge extension calls
// this after resolving a *grove.DB from the host's DI container. The handle is
// borrowed: fabriq never closes it. Injection is single-shard only — when
// Shards is set the explicit shard DSNs win and the grove is ignored.
func (c Config) WithInjectedGrove(db *grove.DB) Config {
	c.primaryGrove = db
	return c
}

// InjectedGrove reports the borrowed grove.DB set via WithInjectedGrove (nil
// when none). It lets a host (e.g. the forge extension) tell whether grove
// resolution succeeded without re-reading the container.
func (c Config) InjectedGrove() *grove.DB { return c.primaryGrove }

// CatalogConfig configures db-per-tenant catalog mode.
type CatalogConfig struct {
	// DSN locates the CONTROL database holding fabriq_tenant_catalog.
	// Setting it turns catalog mode on.
	DSN string `yaml:"dsn" json:"dsn"`
	// ClusterDSNs maps cluster ids to server-level DSNs (no database path
	// or a maintenance database); a tenant's database lives on the cluster
	// its catalog entry names. At least one cluster is required.
	ClusterDSNs map[string]string `yaml:"clusterDsns" json:"clusterDsns"`
	// ReplicaDSNs are optional read-replica endpoints (hot standbys) for the
	// control database. When set, routing reads fall through to them if the
	// primary (DSN) is unreachable, so a primary outage no longer blocks
	// routing (spec 2026-07-03 HA). Writes and reconciler election stay on
	// DSN. Empty = single-Postgres default, unchanged.
	ReplicaDSNs []string `yaml:"replicaDsns" json:"replicaDsns"`
	// CacheTTL bounds route freshness (suspension / new-tenant visibility).
	// Zero falls back to 30s.
	CacheTTL time.Duration `yaml:"cacheTtl" json:"cacheTtl"`
	// MaxActiveShards caps concurrently-open tenant database pools (LRU
	// evicted, idle first). Zero falls back to 128.
	MaxActiveShards int `yaml:"maxActiveShards" json:"maxActiveShards"`
	// AllowSuperuser skips the boot refusal of superuser cluster
	// credentials — dev/test ONLY. RLS inside a tenant database does not
	// bind superusers, and the serving tier must not hold rights it does
	// not need; provisioning (the CLI) is a separate, privileged concern.
	AllowSuperuser bool `yaml:"allowSuperuser" json:"allowSuperuser"`
	// Adaptive turns on autoscaling of MaxActiveShards (opt-in). When
	// disabled (default) the static MaxActiveShards cap is used.
	Adaptive AdaptivePoolConfig `yaml:"adaptive" json:"adaptive"`
	// Isolation selects catalog-mode tenant isolation: "" / "database" (a
	// dedicated database per tenant) or "schema" (schema-per-tenant
	// consolidation: many tenants share a database, isolated by search_path).
	// See ADR 0012.
	Isolation string `yaml:"isolation" json:"isolation"`
	// SharedSchema (schema isolation only) holds the shared extensions
	// (pgvector/postgis) and is appended to every tenant's search_path so
	// their types resolve. Empty defaults to "fabriq_shared".
	SharedSchema string `yaml:"sharedSchema" json:"sharedSchema"`
}

// Enabled reports whether catalog mode is configured.
func (c CatalogConfig) Enabled() bool { return c.DSN != "" }

// SchemaMode reports whether catalog mode uses schema-per-tenant isolation.
func (c CatalogConfig) SchemaMode() bool { return c.Isolation == "schema" }

// sharedSchemaOrDefault returns the configured shared schema, or the default.
func (c CatalogConfig) sharedSchemaOrDefault() string {
	if c.SharedSchema != "" {
		return c.SharedSchema
	}
	return "fabriq_shared"
}

// AnalyticsConfig configures the cross-tenant analytics sink.
type AnalyticsConfig struct {
	// DSN locates the shared analytics database.
	DSN string `yaml:"dsn" json:"dsn"`
	// Batch bounds the backfill write batch size (default 128 when 0).
	Batch int `yaml:"batch" json:"batch"`
}

// Enabled reports whether analytics is configured.
func (c AnalyticsConfig) Enabled() bool { return c.DSN != "" }

// ValidateAnalyticsConfig rejects an analytics DSN that collides with a
// tenant, shard, or catalog control DSN — the analytics store MUST be a
// separate database (the one deliberate cross-tenant co-location).
func ValidateAnalyticsConfig(cfg Config) error {
	if !cfg.Analytics.Enabled() {
		return nil
	}
	adsn := cfg.Analytics.DSN
	if adsn == cfg.Postgres.DSN || adsn == cfg.Catalog.DSN {
		return fmt.Errorf("fabriq: analytics DSN must differ from the tenant and catalog control DSNs")
	}
	for _, sh := range cfg.Shards {
		if adsn == sh.DSN {
			return fmt.Errorf("fabriq: analytics DSN must differ from every shard DSN")
		}
	}
	for _, dsn := range cfg.Catalog.ClusterDSNs {
		if adsn == dsn {
			return fmt.Errorf("fabriq: analytics DSN must differ from every catalog cluster DSN")
		}
	}
	return nil
}

// AdaptivePoolConfig is the user-facing autoscaling surface. Policy
// constants (grow factor, thresholds, cooldown) are NOT exposed here; they
// default inside the shard autoscaler. Operators set only what they must own.
//
// Shrink is lazy: lowering the cap (idle slack, or heap pressure) does not
// force-close held pools; idle pools above a freshly-shrunk cap are reclaimed
// on the NEXT new-shard dial, not proactively. Under total quiescence they
// persist until traffic resumes — this is intentional (see the design's
// non-goals), not a leak.
type AdaptivePoolConfig struct {
	Enabled       bool          `yaml:"enabled" json:"enabled"`
	Min           int           `yaml:"min" json:"min"`                     // floor; 0 -> 8
	Max           int           `yaml:"max" json:"max"`                     // ceiling; 0 -> MaxActiveShards (or 128)
	Interval      time.Duration `yaml:"interval" json:"interval"`           // 0 -> 5s
	ConnBudget    int           `yaml:"connBudget" json:"connBudget"`       // 0 -> no budget clamp
	PerShardConns int           `yaml:"perShardConns" json:"perShardConns"` // 0 -> 4
	HeapSoftLimit uint64        `yaml:"heapSoftLimit" json:"heapSoftLimit"` // bytes; 0 -> heap signal off
}

// PostgresConfig locates the source of truth. Required unless Shards is set
// (it is the one-shard shorthand).
type PostgresConfig struct {
	DSN      string `yaml:"dsn" json:"dsn"`
	PoolSize int    `yaml:"pool_size" json:"pool_size"`
}

// ShardConfig locates one source-of-truth shard. When Config.Shards is
// non-empty, tenants are routed across these by the directory (ADR 0007);
// each shard should be its own Postgres database so its advisory-lock
// leadership (relay) is independent. Leaving Shards empty and setting
// Postgres is the degenerate one-shard deployment.
type ShardConfig struct {
	ID       string `yaml:"id" json:"id"`
	DSN      string `yaml:"dsn" json:"dsn"`
	PoolSize int    `yaml:"pool_size" json:"pool_size"`
}

// RedisConfig locates the event fan-out / cache store.
type RedisConfig struct {
	Addr     string `yaml:"addr" json:"addr"`
	DB       int    `yaml:"db" json:"db"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

// FalkorDBConfig locates the graph projection engine.
type FalkorDBConfig struct {
	Addr     string `yaml:"addr" json:"addr"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

// ElasticsearchConfig locates the search projection engine.
type ElasticsearchConfig struct {
	Addrs    []string `yaml:"addrs" json:"addrs"`
	Username string   `yaml:"username" json:"username"`
	Password string   `yaml:"password" json:"password"`
}

// ProjectionsConfig switches projection planes on.
type ProjectionsConfig struct {
	Graph  bool `yaml:"graph" json:"graph"`
	Search bool `yaml:"search" json:"search"`
}

// SubscriptionsConfig tunes the delta plane.
type SubscriptionsConfig struct {
	ConflationWindow time.Duration `yaml:"conflation_window" json:"conflation_window"`
	StreamMaxLen     int64         `yaml:"stream_max_len" json:"stream_max_len"`
	SubscribeBuffer  int           `yaml:"subscribe_buffer" json:"subscribe_buffer"`
}

// CacheConfig controls the optional in-process L1 cache tier that sits in
// front of the shared Redis L2. When L1Enabled is false (the default) the
// engine uses the L2 adapter directly and behaviour is identical to P1-P3.
type CacheConfig struct {
	// L1Enabled gates the per-node LRU tier. Requires Redis.Addr to be set.
	L1Enabled bool `yaml:"l1_enabled" json:"l1_enabled"`
	// L1Size is the maximum number of entries the LRU holds (default 0 = no
	// entries, so always set a positive value when L1Enabled is true).
	L1Size int `yaml:"l1_size" json:"l1_size"`
	// L1TTL is the per-entry time-to-live in the in-process store. It also
	// bounds the cold-start cross-node staleness window: commits that land
	// between Open() returning and the L1 evict tailer's first XRead attach
	// are missed on this node and will remain stale until at most L1TTL
	// elapses. Defaults to 5 minutes when L1Enabled is true and this is <= 0.
	L1TTL time.Duration `yaml:"l1_ttl" json:"l1_ttl"`
}

// EncryptionConfig configures field-level encryption (blob_source credentials).
// Key is a base64-encoded 32-byte AES-256 data-encryption key; empty disables
// encryption (writes that carry credentials then fail closed).
type EncryptionConfig struct {
	Key string `yaml:"key" json:"key"`
}

// StorageConfig configures the object-store backend that fills f.Blob().
// Empty StorageDriver leaves the blob port unconfigured (shipped dark).
type StorageConfig struct {
	StorageDriver string `yaml:"storageDriver" json:"storageDriver"`
	DefaultBucket string `yaml:"defaultBucket" json:"defaultBucket"`
	// EnableCas gates the content-addressable store layer. When true, Open()
	// will wire a CASStore backed by the fabriq_blob_cas ledger (requires a Postgres
	// adapter). The open.go wiring that reads this field lands in Phase 3b.
	EnableCas bool `yaml:"enableCas" json:"enableCas"`
}

// DocumentsConfig configures the CRDT document plane. Empty is the default
// (history stays in Postgres).
type DocumentsConfig struct {
	// ArchiveHistory is the global default for offloading sealed CRDT update
	// history to the blob plane on Compact. Per-entity CRDTSpec.ArchiveHistory
	// overrides it. Requires Storage to be configured (Open fails fast
	// otherwise).
	ArchiveHistory bool `yaml:"archiveHistory" json:"archiveHistory"`
}

// Validate checks cross-field consistency. It does not dial anything.
func (c Config) Validate() error {
	if len(c.Shards) > 0 {
		seen := map[string]struct{}{}
		for i, s := range c.Shards {
			if s.ID == "" {
				return fmt.Errorf("fabriq: config: shards[%d].id is required", i)
			}
			if s.DSN == "" {
				return fmt.Errorf("fabriq: config: shards[%d].dsn is required (shard %q)", i, s.ID)
			}
			if _, dup := seen[s.ID]; dup {
				return fmt.Errorf("fabriq: config: duplicate shard id %q", s.ID)
			}
			seen[s.ID] = struct{}{}
		}
	} else if c.Postgres.DSN == "" && c.primaryGrove == nil && !c.Catalog.Enabled() {
		return fmt.Errorf("fabriq: config: postgres.dsn (or shards, catalog mode, or an injected grove.DB) is required (postgres is the source of truth)")
	}
	if len(c.ShardPins) > 0 {
		if len(c.Shards) == 0 {
			return fmt.Errorf("fabriq: config: shardPins requires shards (pinning is a multi-shard routing override)")
		}
		ids := make(map[string]struct{}, len(c.Shards))
		for _, s := range c.Shards {
			ids[s.ID] = struct{}{}
		}
		for tid, sid := range c.ShardPins {
			if tid == "" {
				return fmt.Errorf("fabriq: config: shardPins has an empty tenant id (pinned to shard %q)", sid)
			}
			if _, ok := ids[sid]; !ok {
				return fmt.Errorf("fabriq: config: shardPins: tenant %q pinned to unknown shard %q", tid, sid)
			}
		}
	}
	if c.Catalog.Enabled() {
		if len(c.Shards) > 0 || len(c.ShardPins) > 0 {
			return fmt.Errorf("fabriq: config: catalog mode is mutually exclusive with shards/shardPins (one routing authority)")
		}
		if c.Postgres.DSN != "" {
			return fmt.Errorf("fabriq: config: catalog mode is mutually exclusive with postgres.dsn (tenant databases come from the catalog, not a primary)")
		}
		if len(c.Catalog.ClusterDSNs) == 0 {
			return fmt.Errorf("fabriq: config: catalog.clusterDsns requires at least one cluster")
		}
		for id, dsn := range c.Catalog.ClusterDSNs {
			if id == "" || dsn == "" {
				return fmt.Errorf("fabriq: config: catalog.clusterDsns entries need both a cluster id and a dsn")
			}
		}
		if c.Catalog.MaxActiveShards < 0 {
			return fmt.Errorf("fabriq: config: catalog.maxActiveShards must be >= 0")
		}
		if c.Catalog.Adaptive.Enabled {
			if c.Catalog.Adaptive.Min < 1 {
				return fmt.Errorf("fabriq: config: catalog.adaptive.min must be >= 1")
			}
			if c.Catalog.Adaptive.Max != 0 && c.Catalog.Adaptive.Max < c.Catalog.Adaptive.Min {
				return fmt.Errorf("fabriq: config: catalog.adaptive.max must be >= min")
			}
			if c.Catalog.Adaptive.ConnBudget < 0 || c.Catalog.Adaptive.PerShardConns < 0 {
				return fmt.Errorf("fabriq: config: catalog.adaptive.connBudget/perShardConns must be >= 0")
			}
		}
		switch c.Catalog.Isolation {
		case "", "database", "schema":
		default:
			return fmt.Errorf("fabriq: config: catalog.isolation must be \"database\" or \"schema\" (got %q)", c.Catalog.Isolation)
		}
		if c.Catalog.SchemaMode() && !catalogSharedSchemaPattern.MatchString(c.Catalog.sharedSchemaOrDefault()) {
			return fmt.Errorf("fabriq: config: catalog.sharedSchema %q is not a valid bare identifier", c.Catalog.SharedSchema)
		}
		for i, rdsn := range c.Catalog.ReplicaDSNs {
			if rdsn == "" {
				return fmt.Errorf("fabriq: config: catalog.replicaDsns[%d] is empty", i)
			}
			if u, err := url.Parse(rdsn); err != nil || u.Scheme == "" {
				return fmt.Errorf("fabriq: config: catalog.replicaDsns[%d] must be a postgres:// URL", i)
			}
		}
	}
	if c.Projections.Graph && c.FalkorDB.Addr == "" {
		return fmt.Errorf("fabriq: config: projections.graph enabled but falkordb.addr is empty")
	}
	if c.Projections.Search && len(c.Elasticsearch.Addrs) == 0 {
		return fmt.Errorf("fabriq: config: projections.search enabled but elasticsearch.addrs is empty")
	}
	if (c.Projections.Graph || c.Projections.Search) && c.Redis.Addr == "" {
		return fmt.Errorf("fabriq: config: projections need redis.addr for the event stream")
	}
	if c.Subscriptions.ConflationWindow < 0 {
		return fmt.Errorf("fabriq: config: subscriptions.conflation_window must be >= 0")
	}
	return nil
}

// Options derives facade options from config tuning knobs.
func (c Config) Options() []Option {
	var opts []Option
	if c.Subscriptions.ConflationWindow > 0 {
		opts = append(opts, WithConflationWindow(c.Subscriptions.ConflationWindow))
	}
	if c.Subscriptions.StreamMaxLen > 0 {
		opts = append(opts, WithStreamMaxLen(c.Subscriptions.StreamMaxLen))
	}
	if c.Subscriptions.SubscribeBuffer > 0 {
		opts = append(opts, WithSubscribeBuffer(c.Subscriptions.SubscribeBuffer))
	}
	return opts
}

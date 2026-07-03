package fabriq

import (
	"fmt"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/projection"
)

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
	ShardPins     map[string]string   `yaml:"shardPins" json:"shardPins"`
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
	} else if c.Postgres.DSN == "" && c.primaryGrove == nil {
		return fmt.Errorf("fabriq: config: postgres.dsn (or shards, or an injected grove.DB) is required (postgres is the source of truth)")
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

package fabriq

import (
	"fmt"
	"time"
)

// Config is fabriq's declarative configuration: which stores exist and
// which projections run. It is the same schema a future standalone
// `fabriqd` loads from YAML, hence the yaml tags. Entities are not
// configured here — they are registered in code via the registry.
type Config struct {
	Postgres      PostgresConfig      `yaml:"postgres" json:"postgres"`
	Shards        []ShardConfig       `yaml:"shards" json:"shards"`
	Redis         RedisConfig         `yaml:"redis" json:"redis"`
	FalkorDB      FalkorDBConfig      `yaml:"falkordb" json:"falkordb"`
	Elasticsearch ElasticsearchConfig `yaml:"elasticsearch" json:"elasticsearch"`
	Projections   ProjectionsConfig   `yaml:"projections" json:"projections"`
	Subscriptions SubscriptionsConfig `yaml:"subscriptions" json:"subscriptions"`
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
	} else if c.Postgres.DSN == "" {
		return fmt.Errorf("fabriq: config: postgres.dsn (or shards) is required (postgres is the source of truth)")
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

package fabriq

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/adapters/redis"
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
}

// Close releases every opened adapter.
func (s *Stores) Close() error {
	var firstErr error
	if s.Redis != nil {
		if err := s.Redis.Close(); err != nil {
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

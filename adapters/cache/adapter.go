// Package cache is fabriq's caching adapter: the core/cache port implemented
// over grove kv's Store (redis driver). It is the place grove kv "earns its
// keep" per docs/decisions/0003-redis-client.md. Generation counters use the
// raw redis client (INCR) via grove's documented Unwrap escape hatch.
package cache

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/drivers/redisdriver"

	corecache "github.com/xraph/fabriq/core/cache"
)

// Config locates the Redis/Valkey instance backing the cache.
type Config struct {
	Addr     string
	DB       int
	Username string
	Password string
}

// Adapter implements core/cache.Cache over a grove kv Store.
type Adapter struct {
	store  *kv.Store
	client redis.UniversalClient // for atomic generation counters (INCR/GET)
	flight *flightGroup
	stats  cacheStats
}

// cacheStats holds lock-free activity counters for hit-rate observability.
type cacheStats struct {
	hits          atomic.Int64
	misses        atomic.Int64
	sets          atomic.Int64
	invalidations atomic.Int64
}

// Stats implements corecache.StatsReader — a point-in-time counter snapshot.
func (a *Adapter) Stats() corecache.Stats {
	return corecache.Stats{
		Hits:          a.stats.hits.Load(),
		Misses:        a.stats.misses.Load(),
		Sets:          a.stats.sets.Load(),
		Invalidations: a.stats.invalidations.Load(),
	}
}

// Open dials Redis through grove kv and pings it.
func Open(ctx context.Context, cfg Config) (*Adapter, error) {
	rdb := redisdriver.New()
	dsn := dsnFromConfig(cfg)
	if err := rdb.Open(ctx, dsn); err != nil {
		return nil, fmt.Errorf("fabriq/cache: open redis driver: %w", err)
	}
	store, err := kv.Open(rdb)
	if err != nil {
		return nil, fmt.Errorf("fabriq/cache: open kv store: %w", err)
	}
	if err := store.Ping(ctx); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("fabriq/cache: ping: %w", err)
	}
	return &Adapter{
		store:  store,
		client: redisdriver.UnwrapClient(store),
		flight: newFlightGroup(),
	}, nil
}

// Close releases the store.
func (a *Adapter) Close() error { return a.store.Close() }

func dsnFromConfig(cfg Config) string {
	auth := ""
	if cfg.Username != "" || cfg.Password != "" {
		auth = cfg.Username + ":" + cfg.Password + "@"
	}
	return fmt.Sprintf("redis://%s%s/%d", auth, cfg.Addr, cfg.DB)
}

var _ corecache.Cache = (*Adapter)(nil)

// errRedisNil is go-redis's "key does not exist" sentinel, used for generation reads.
var errRedisNil = redis.Nil

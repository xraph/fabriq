// Package cache is fabriq's caching adapter: the core/cache port implemented
// over grove kv's Store (redis driver). It is the place grove kv "earns its
// keep" per docs/decisions/0003-redis-client.md. Generation counters use the
// raw redis client (INCR) via grove's documented Unwrap escape hatch.
package cache

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	gometrics "github.com/xraph/go-utils/metrics"
	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
	"github.com/xraph/grove/kv/drivers/redisdriver"

	corecache "github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/internal/enginetrust"
)

// Config locates the Redis/Valkey instance backing the cache. TLSMode and
// CACert work as they do for the stream transport (adapters/redis).
type Config struct {
	Addr     string
	DB       int
	Username string
	Password string
	TLSMode  string
	CACert   []byte
	// Metrics, when set, receives grove kv's
	// db_client_operation_duration_seconds for every command (pass a Forge
	// app's Metrics()).
	Metrics gometrics.Metrics
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
	tlsConfig, err := enginetrust.RedisTLS(cfg.Addr, cfg.TLSMode, cfg.CACert)
	if err != nil {
		return nil, err
	}
	var driverOpts []driver.Option
	if tlsConfig != nil {
		driverOpts = append(driverOpts, driver.WithTLSConfig(tlsConfig), driver.WithDialTimeout(5*time.Second))
	}
	rdb := redisdriver.New()
	if err = rdb.Open(ctx, dsnFromConfig(cfg), driverOpts...); err != nil {
		return nil, fmt.Errorf("fabriq/cache: open redis driver: %w", err)
	}
	var kvOpts []kv.Option
	if cfg.Metrics != nil {
		kvOpts = append(kvOpts, kv.WithMetrics(cfg.Metrics))
	}
	store, err := kv.Open(rdb, kvOpts...)
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
	u := url.URL{Scheme: "redis", Host: cfg.Addr, Path: "/" + strconv.Itoa(cfg.DB)}
	if cfg.Username != "" || cfg.Password != "" {
		// Escaped, so a password holding @, / or : stays a password.
		u.User = url.UserPassword(cfg.Username, cfg.Password)
	}
	return u.String()
}

var _ corecache.Cache = (*Adapter)(nil)

// errRedisNil is go-redis's "key does not exist" sentinel, used for generation reads.
var errRedisNil = redis.Nil

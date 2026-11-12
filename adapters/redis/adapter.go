// Package redis is fabriq's Redis adapter: event fan-out over Streams
// (publisher for the relay, tailer for the hub, consumer groups for
// projections), versioned-prefix caching, and ephemeral presence pub/sub.
//
// It deliberately uses go-redis directly (not grove's kv driver): the
// stream paths need MAXLEN~ trimming, XAUTOCLAIM and blocking group reads
// that the kv abstraction does not expose — see
// docs/decisions/0003-redis-client.md. The import is fenced to adapters/
// by depguard.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xraph/fabriq/internal/enginetrust"
)

// Config locates the Redis instance. TLSMode is empty, disable or
// verify-full. CACert supplies optional PEM trust roots for verify-full;
// without it the system roots verify the server, which is what managed
// Redis and Valkey services need.
type Config struct {
	Addr     string
	DB       int
	Username string
	Password string
	TLSMode  string
	CACert   []byte
}

// Adapter wraps one Redis client with fabriq's stream/cache/pubsub
// surfaces.
type Adapter struct {
	client       *redis.Client
	eventsMaxLen int64
	chanMaxLen   int64
}

// Option tunes the adapter.
type Option func(*Adapter)

// WithChannelMaxLen sets the approximate per-channel stream cap (default
// 500): the catch-up depth before clients must fall back to refetch.
func WithChannelMaxLen(n int64) Option {
	return func(a *Adapter) {
		if n > 0 {
			a.chanMaxLen = n
		}
	}
}

// WithEventsMaxLen caps the main event stream (default 1,000,000 —
// projections that fall further behind rebuild from Postgres, which is
// always possible by design).
func WithEventsMaxLen(n int64) Option {
	return func(a *Adapter) {
		if n > 0 {
			a.eventsMaxLen = n
		}
	}
}

// Open checks the TLS settings, then dials Redis and pings it. Settings
// that can't be right fail before anything is dialled.
func Open(ctx context.Context, cfg Config, opts ...Option) (*Adapter, error) {
	tlsConfig, err := enginetrust.RedisTLS(cfg.Addr, cfg.TLSMode, cfg.CACert)
	if err != nil {
		return nil, err
	}
	redisOpts := &redis.Options{
		Addr:      cfg.Addr,
		DB:        cfg.DB,
		Username:  cfg.Username,
		Password:  cfg.Password,
		TLSConfig: tlsConfig,
	}
	if tlsConfig != nil {
		const dialTimeout = 5 * time.Second
		redisOpts.DialTimeout = dialTimeout
		redisOpts.Dialer = enginetrust.TLSDialer(tlsConfig, dialTimeout)
	}
	a := &Adapter{
		client:       redis.NewClient(redisOpts),
		eventsMaxLen: 1_000_000,
		chanMaxLen:   500,
	}
	for _, opt := range opts {
		opt(a)
	}
	if err := a.client.Ping(ctx).Err(); err != nil {
		_ = a.client.Close()
		return nil, fmt.Errorf("fabriq: redis ping: %w", err)
	}
	return a, nil
}

// Ping reports Redis reachability, bounded by ctx. It backs the adminapi
// connection-info health probe.
func (a *Adapter) Ping(ctx context.Context) error { return a.client.Ping(ctx).Err() }

// Close releases the client.
func (a *Adapter) Close() error { return a.client.Close() }

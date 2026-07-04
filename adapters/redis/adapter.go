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

	"github.com/redis/go-redis/v9"
)

// Config locates the Redis instance.
type Config struct {
	Addr     string
	DB       int
	Username string
	Password string
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

// Open dials Redis and pings it.
func Open(ctx context.Context, cfg Config, opts ...Option) (*Adapter, error) {
	a := &Adapter{
		client: redis.NewClient(&redis.Options{
			Addr:     cfg.Addr,
			DB:       cfg.DB,
			Username: cfg.Username,
			Password: cfg.Password,
		}),
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

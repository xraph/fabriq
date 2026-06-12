package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xraph/fabriq/core/tenant"
)

// Cache is a tenant-scoped, version-prefixed byte cache. Keys look like
// fabriq:v3:{tenant}:{entity}:{id}; bumping the model version makes every
// stale entry invisible at once (no mass deletes), and the tenant segment
// comes exclusively from the context.
type Cache struct {
	client       *redis.Client
	modelVersion int
}

// Cache returns a cache view for the given projection model version.
func (a *Adapter) Cache(modelVersion int) *Cache {
	return &Cache{client: a.client, modelVersion: modelVersion}
}

func (c *Cache) key(ctx context.Context, entity, id string) (string, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("fabriq:v%d:%s:%s:%s", c.modelVersion, tid, entity, id), nil
}

// Get reads a cached value; ok=false on miss.
func (c *Cache) Get(ctx context.Context, entity, id string) (val []byte, ok bool, err error) {
	key, err := c.key(ctx, entity, id)
	if err != nil {
		return nil, false, err
	}
	raw, err := c.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("fabriq: cache get: %w", err)
	}
	return raw, true, nil
}

// Set stores a value with a TTL.
func (c *Cache) Set(ctx context.Context, entity, id string, val []byte, ttl time.Duration) error {
	key, err := c.key(ctx, entity, id)
	if err != nil {
		return err
	}
	if err := c.client.Set(ctx, key, val, ttl).Err(); err != nil {
		return fmt.Errorf("fabriq: cache set: %w", err)
	}
	return nil
}

// Delete removes a value.
func (c *Cache) Delete(ctx context.Context, entity, id string) error {
	key, err := c.key(ctx, entity, id)
	if err != nil {
		return err
	}
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("fabriq: cache delete: %w", err)
	}
	return nil
}

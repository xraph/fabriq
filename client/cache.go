package client

import (
	"context"
	"net/http"
)

// CacheKeyspace describes one entity's read-through cache keyspace, derived
// from its declarative CacheSpec. It mirrors adminapi's cacheKeyspaceItem
// JSON exactly: {entity, name, partition, mode, ttlSeconds, scoped}.
type CacheKeyspace struct {
	Entity string `json:"entity"`
	Name   string `json:"name"`
	// Partition is "tenant" or "tenant+scope".
	Partition string `json:"partition"`
	// Mode is the invalidation mode, e.g. "versioned".
	Mode       string `json:"mode"`
	TTLSeconds int64  `json:"ttlSeconds"`
	Scoped     bool   `json:"scoped"`
}

// CacheInfo is the payload for GetCache. It mirrors adminapi's
// cacheResponse JSON exactly: {configured, keyspaces}.
type CacheInfo struct {
	// Configured reports whether an engine cache is wired (Redis present).
	Configured bool            `json:"configured"`
	Keyspaces  []CacheKeyspace `json:"keyspaces"`
}

// CacheStats is the payload for GetCacheStats. It mirrors adminapi's
// cacheStatsResponse JSON exactly:
// {available, hits, misses, sets, invalidations, hitRate}.
type CacheStats struct {
	// Available is false when a cache is configured but exposes no counters.
	Available     bool  `json:"available"`
	Hits          int64 `json:"hits"`
	Misses        int64 `json:"misses"`
	Sets          int64 `json:"sets"`
	Invalidations int64 `json:"invalidations"`
	// HitRate is Hits/(Hits+Misses), 0 when there have been no lookups.
	HitRate float64 `json:"hitRate"`
}

// CacheInvalidateResult is the payload for CacheInvalidate. It mirrors
// adminapi's response JSON exactly: {invalidated}.
type CacheInvalidateResult struct {
	Invalidated bool `json:"invalidated"`
}

// GetCache reports whether an engine cache is wired and which registered
// entities opt into the read-through row cache. It calls
// GET {BasePath}/cache.
func (c *Client) GetCache(ctx context.Context) (CacheInfo, error) {
	var out CacheInfo
	if err := c.do(ctx, http.MethodGet, "/cache", nil, nil, &out); err != nil {
		return CacheInfo{}, err
	}
	return out, nil
}

// GetCacheStats reports cache activity counters and the derived hit-rate.
// It calls GET {BasePath}/cache/stats. Returns an *APIError with Status 501
// when no cache is configured.
func (c *Client) GetCacheStats(ctx context.Context) (CacheStats, error) {
	var out CacheStats
	if err := c.do(ctx, http.MethodGet, "/cache/stats", nil, nil, &out); err != nil {
		return CacheStats{}, err
	}
	return out, nil
}

// CacheInvalidate bumps an entity's cache generation, orphaning every
// cached read of it for the tenant (and scope) in one O(1) operation. It
// calls POST {BasePath}/cache/invalidate with body {entity}. Returns an
// *APIError with Status 501 when no cache is configured, or Status 400 when
// the entity is unknown or not cached.
func (c *Client) CacheInvalidate(ctx context.Context, entity string) (CacheInvalidateResult, error) {
	var out CacheInvalidateResult
	body := map[string]string{"entity": entity}
	if err := c.do(ctx, http.MethodPost, "/cache/invalidate", nil, body, &out); err != nil {
		return CacheInvalidateResult{}, err
	}
	return out, nil
}

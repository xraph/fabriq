// Package cachequery decorates the relational read port with a per-id,
// opt-in, read-through row cache. Only entities whose EntitySpec.Cache is set
// are cached; all others pass straight through. Rows round-trip as JSON.
package cachequery

import (
	"context"
	"encoding/json"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// EntityRowKeyspace returns the canonical per-entity row keyspace (shared by
// the decorator's reads and the invalidator's per-id eviction). EventEvict +
// keyspace-name generation (NOT entity-keyed), so a sibling-row write never
// busts it — only an explicit per-id Invalidate does.
func EntityRowKeyspace(ent *registry.Entity) cache.Keyspace {
	part := cache.Tenant
	if ent.Spec.Cache.Scoped {
		part = cache.TenantScope
	}
	return cache.Keyspace{
		Name:      ent.Spec.Name + ":row",
		Version:   1,
		Partition: part,
		Policy: cache.Policy{
			Mode: cache.EventEvict,
			TTL:  ent.Spec.Cache.TTL,
		},
	}
}

// CachedRelational wraps a RelationalQuerier with the row cache.
type CachedRelational struct {
	inner query.RelationalQuerier
	c     cache.Cache
	reg   *registry.Registry
}

// New builds the decorator. inner, c, and reg must be non-nil.
func New(inner query.RelationalQuerier, c cache.Cache, reg *registry.Registry) *CachedRelational {
	return &CachedRelational{inner: inner, c: c, reg: reg}
}

var _ query.RelationalQuerier = (*CachedRelational)(nil)

// cacheable returns the entity and true iff it opts into caching.
func (cr *CachedRelational) cacheable(entity string) (*registry.Entity, bool) {
	ent, ok := cr.reg.Get(entity)
	if !ok || ent.Spec.Cache == nil {
		return nil, false
	}
	return ent, true
}

// Get serves one row from cache (JSON round-trip), or loads and caches it.
// A successful load is cached; a not-found / error is returned as-is and
// nothing is cached. A cache.Get error or corrupt entry falls through to the
// inner querier; a cache.Set error is ignored.
func (cr *CachedRelational) Get(ctx context.Context, entity, id string, into any) error {
	ent, ok := cr.cacheable(entity)
	if !ok {
		return cr.inner.Get(ctx, entity, id, into)
	}
	ks := EntityRowKeyspace(ent)
	if raw, hit, err := cr.c.Get(ctx, ks, id); err == nil && hit {
		if uerr := json.Unmarshal(raw, into); uerr == nil {
			return nil
		}
		// Corrupt cache entry: fall through to a fresh load.
	}
	if err := cr.inner.Get(ctx, entity, id, into); err != nil {
		return err
	}
	if b, merr := json.Marshal(into); merr == nil {
		_ = cr.c.Set(ctx, ks, id, b) // best-effort; ignore error
	}
	return nil
}

// GetMany delegates for now (cached in the next task).
func (cr *CachedRelational) GetMany(ctx context.Context, entity string, ids []string, into any) error {
	return cr.inner.GetMany(ctx, entity, ids, into)
}

// List passes through (result-set caching is P3b).
func (cr *CachedRelational) List(ctx context.Context, entity string, q query.ListQuery, into any) error {
	return cr.inner.List(ctx, entity, q, into)
}

// Query (raw SQL) passes through.
func (cr *CachedRelational) Query(ctx context.Context, into any, sql string, args ...any) error {
	return cr.inner.Query(ctx, into, sql, args...)
}

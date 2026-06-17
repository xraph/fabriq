// Package cachequery decorates the relational read port with a per-id,
// opt-in, read-through row cache. Only entities whose EntitySpec.Cache is set
// are cached; all others pass straight through. Rows round-trip as JSON.
package cachequery

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// EntityRowKeyspace returns the canonical per-entity row keyspace (shared by
// the decorator's reads and the invalidator's per-id eviction). EventEvict +
// keyspace-name generation (NOT entity-keyed), so a sibling-row write never
// busts it — only an explicit per-id Invalidate does.
// Precondition: ent.Spec.Cache must be non-nil (call only for entities that opted into caching).
func EntityRowKeyspace(ent *registry.Entity) cache.Keyspace {
	if ent.Spec.Cache == nil {
		panic("cachequery: EntityRowKeyspace called for entity without a CacheSpec: " + ent.Spec.Name)
	}
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

// idHolder extracts the id from a cached/loaded row's JSON.
type idHolder struct {
	ID string `json:"id"`
}

// GetMany serves rows per id from cache, batch-loads only the misses, caches
// each loaded row, and assembles the result into `into` in requested-id order
// (missing rows skipped). Pass-through for non-opted entities.
func (cr *CachedRelational) GetMany(ctx context.Context, entity string, ids []string, into any) error {
	ent, ok := cr.cacheable(entity)
	if !ok {
		return cr.inner.GetMany(ctx, entity, ids, into)
	}
	ks := EntityRowKeyspace(ent)

	byID := make(map[string]json.RawMessage, len(ids))
	miss := make([]string, 0, len(ids))
	for _, id := range ids {
		if raw, hit, err := cr.c.Get(ctx, ks, id); err == nil && hit {
			byID[id] = raw
		} else {
			miss = append(miss, id)
		}
	}

	if len(miss) > 0 {
		// Fresh slice of into's element type (into is *[]T).
		tmpPtr := reflect.New(reflect.TypeOf(into).Elem())
		if err := cr.inner.GetMany(ctx, entity, miss, tmpPtr.Interface()); err != nil {
			return err
		}
		loaded, merr := json.Marshal(tmpPtr.Interface())
		if merr != nil {
			return merr
		}
		var rawRows []json.RawMessage
		if uerr := json.Unmarshal(loaded, &rawRows); uerr != nil {
			return uerr
		}
		for _, rr := range rawRows {
			var h idHolder
			if json.Unmarshal(rr, &h) == nil && h.ID != "" {
				byID[h.ID] = rr
				_ = cr.c.Set(ctx, ks, h.ID, rr) // best-effort
			}
		}
	}

	ordered := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		if rr, present := byID[id]; present {
			ordered = append(ordered, rr)
		}
	}
	arr, err := json.Marshal(ordered)
	if err != nil {
		return err
	}
	return json.Unmarshal(arr, into)
}

// List passes through (result-set caching is P3b).
func (cr *CachedRelational) List(ctx context.Context, entity string, q query.ListQuery, into any) error {
	return cr.inner.List(ctx, entity, q, into)
}

// Query (raw SQL) passes through.
func (cr *CachedRelational) Query(ctx context.Context, into any, sql string, args ...any) error {
	return cr.inner.Query(ctx, into, sql, args...)
}

// Package cache is fabriq's first-class caching port: a byte-level, scope-aware
// cache with per-keyspace freshness policy. It is pure (no driver imports):
// adapters/cache implements it over grove kv; fabriqtest provides FakeCache.
// Tenant/scope are derived EXCLUSIVELY from context — never caller-supplied.
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/tenant"
)

// Partition is the isolation dimension declared per keyspace. The concrete
// tenant/scope is resolved from context, never passed by callers.
type Partition int

const (
	// Global is engine-wide: no tenant segment. Use for cross-tenant
	// reference data (schema descriptors, dynamic-entity defs).
	Global Partition = iota
	// Tenant isolates by the context tenant.
	Tenant
	// TenantScope isolates by tenant + scope_id (the feat/native-scope axis).
	TenantScope
)

// Resolve returns the key segment for this partition under ctx. Global never
// requires a tenant; Tenant/TenantScope require one (and error if absent).
func (p Partition) Resolve(ctx context.Context) (string, error) {
	switch p {
	case Global:
		return "g", nil
	case Tenant:
		tid, err := tenant.FromContext(ctx)
		if err != nil {
			return "", fmt.Errorf("fabriq/cache: tenant partition needs a tenant: %w", err)
		}
		return "t:" + tid, nil
	case TenantScope:
		tid, err := tenant.FromContext(ctx)
		if err != nil {
			return "", fmt.Errorf("fabriq/cache: tenant-scope partition needs a tenant: %w", err)
		}
		return "t:" + tid + ":s:" + tenant.ScopeOrEmpty(ctx), nil
	default:
		return "", fmt.Errorf("fabriq/cache: unknown partition %d", int(p))
	}
}

// Mode is the per-keyspace invalidation mechanism (the precision seam).
type Mode int

const (
	// TTLOnly: bounded staleness, no active eviction.
	TTLOnly Mode = iota
	// Versioned: generation-bump invalidation (mass-orphan via InvalidateKeyspace;
	// in P2, auto-bumped per entity-type on write).
	Versioned
	// EventEvict: targeted per-id eviction. The cross-node consumer lands in P2;
	// in P1, explicit Invalidate(keys...) is the working path.
	EventEvict
	// PredicateEvict (FUTURE, P4): surgical eviction on the live-query predicate
	// index. Declared for forward-compatibility; not implemented in P1.
)

// Policy is a keyspace's freshness contract.
type Policy struct {
	Mode Mode
	TTL  time.Duration // required for TTLOnly; backstop otherwise (0 = no expiry)
}

// Keyspace is a named family of cached values, declared once and reused.
// Version is a code-level epoch: bump it in source to invalidate on deploy.
type Keyspace struct {
	Name      string
	Version   int
	Partition Partition
	Policy    Policy
	// Entity, when set, names the registry entity this keyspace caches reads
	// of. The generation is then keyed by entity, so a write to that entity
	// (via Cache.InvalidateEntity) orphans this keyspace. Empty for keyspaces
	// not tied to a single entity (their generation is keyed by Name).
	Entity string
}

// Cache is the byte-level caching port. Typed access is via Typed[T].
type Cache interface {
	// GetOrLoad returns the cached value for key, or calls load exactly once
	// (single-flight per key) on a miss, stores the result under the keyspace
	// policy, and returns it.
	GetOrLoad(ctx context.Context, ks Keyspace, key string,
		load func(context.Context) ([]byte, error)) ([]byte, error)
	// Get returns the cached value; ok=false on miss.
	Get(ctx context.Context, ks Keyspace, key string) (val []byte, ok bool, err error)
	// Set stores a value under the keyspace policy.
	Set(ctx context.Context, ks Keyspace, key string, val []byte) error
	// Invalidate removes specific keys from the keyspace (targeted eviction).
	Invalidate(ctx context.Context, ks Keyspace, keys ...string) error
	// InvalidateKeyspace bumps the keyspace generation, orphaning every entry
	// under it in the current partition at once (no mass deletes).
	InvalidateKeyspace(ctx context.Context, ks Keyspace) error
	// InvalidateEntity bumps the generation of `entity` for the Global, Tenant,
	// and (if a scope is in context) TenantScope partitions — orphaning every
	// cached entry whose keyspace declares Entity == entity in those partitions.
	// This is the write-driven invalidation the command plane calls post-commit.
	InvalidateEntity(ctx context.Context, entity string) error
	// Close releases resources.
	Close() error
}

// Stats is a point-in-time snapshot of cache activity counters. It is optional
// observability: only adapters that implement StatsReader expose it.
type Stats struct {
	// Hits/Misses count read-through lookups (GetOrLoad) that were served from
	// cache vs. had to load. HitRate = Hits / (Hits + Misses).
	Hits   int64 `json:"hits"`
	Misses int64 `json:"misses"`
	// Sets counts values stored; Invalidations counts eviction/generation-bump ops.
	Sets          int64 `json:"sets"`
	Invalidations int64 `json:"invalidations"`
}

// StatsReader is an optional interface a Cache may implement to expose activity
// counters for hit-rate observability. Callers type-assert for it; a Cache that
// does not implement it simply has no stats surface.
type StatsReader interface {
	Stats() Stats
}

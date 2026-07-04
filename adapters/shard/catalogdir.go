package shard

import (
	"context"
	"sync"
	"time"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// CatalogDirectory routes tenants by the db-per-tenant control plane
// (spec 2026-07-03, D1/D2): each ACTIVE catalog entry resolves to its own
// shard, addressed "{clusterId}/{database}". Non-active states resolve to
// typed errors the gateway maps to honest statuses instead of 500s:
//
//	unknown tenant            → CodeNotFound
//	pending/creating/migrating → CodeUnavailable (provisioning)
//	suspended / failed        → CodeUnavailable
//
// Both positive and negative answers are cached for ttl. Placement is
// stable, so the TTL is a freshness bound: a suspension or a
// just-provisioned tenant takes effect within one TTL — and repeated
// lookups of unknown tenants cannot storm the control database.
func CatalogDirectory(cat catalog.Catalog, ttl time.Duration, opts ...CatalogDirOption) Directory {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	d := &catalogDirectory{
		cat:   cat,
		ttl:   ttl,
		now:   time.Now,
		cache: map[string]catalogCacheEntry{},
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// CatalogDirOption configures a CatalogDirectory.
type CatalogDirOption func(*catalogDirectory)

// WithCatalogClock injects the clock (TTL tests).
func WithCatalogClock(now func() time.Time) CatalogDirOption {
	return func(d *catalogDirectory) { d.now = now }
}

// WithMinVersion fails routing CLOSED for tenants whose database is
// recorded below the binary's migration floor (spec D7): serving a schema
// older than the code expects corrupts, so the tenant gets a typed
// CodeUnavailable until the fleet roller catches it up.
func WithMinVersion(v string) CatalogDirOption {
	return func(d *catalogDirectory) { d.minVersion = v }
}

// CatalogDirectoryWithClock is retained for tests predating the option
// form.
func CatalogDirectoryWithClock(cat catalog.Catalog, ttl time.Duration, now func() time.Time) Directory {
	return CatalogDirectory(cat, ttl, WithCatalogClock(now))
}

type catalogCacheEntry struct {
	id  string
	err error
	exp time.Time
}

type catalogDirectory struct {
	cat        catalog.Catalog
	ttl        time.Duration
	now        func() time.Time
	minVersion string

	mu    sync.RWMutex
	cache map[string]catalogCacheEntry
}

func (d *catalogDirectory) Shard(ctx context.Context, tenantID string) (string, error) {
	d.mu.RLock()
	if e, ok := d.cache[tenantID]; ok && d.now().Before(e.exp) {
		d.mu.RUnlock()
		return e.id, e.err
	}
	d.mu.RUnlock()

	id, resolveErr := d.resolve(ctx, tenantID)

	// Cache the answer either way — but only cache ERRORS that are stable
	// catalog answers (not-found / not-active) AND not degraded (a replica-
	// sourced answer served while the primary was unreachable; H3). Transport
	// failures and degraded answers must not be pinned for a TTL.
	if resolveErr == nil || (isCatalogAnswer(resolveErr) && !catalog.IsDegraded(resolveErr)) {
		d.mu.Lock()
		d.cache[tenantID] = catalogCacheEntry{id: id, err: resolveErr, exp: d.now().Add(d.ttl)}
		d.mu.Unlock()
	}
	return id, resolveErr
}

func (d *catalogDirectory) resolve(ctx context.Context, tenantID string) (string, error) {
	entry, err := d.cat.Get(ctx, tenantID)
	if err != nil {
		return "", err
	}
	if entry.State != catalog.StateActive {
		return "", fabriqerr.New(fabriqerr.CodeUnavailable,
			"tenant is not routable.",
			fabriqerr.WithEntity("tenant", tenantID),
			fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{"state": string(entry.State)}}))
	}
	// Version gate (D7): grove migration versions are zero-padded numeric
	// strings, so lexicographic comparison is version order.
	if d.minVersion != "" && entry.Version < d.minVersion {
		return "", fabriqerr.New(fabriqerr.CodeUnavailable,
			"tenant database is below the binary's migration floor.",
			fabriqerr.WithEntity("tenant", tenantID),
			fabriqerr.WithMeta(fabriqerr.Meta{Detail: map[string]string{
				"version": entry.Version, "floor": d.minVersion,
			}}))
	}
	return entry.ShardID(), nil
}

// isCatalogAnswer reports whether err is a definitive catalog answer
// (cacheable) rather than a transport failure.
func isCatalogAnswer(err error) bool {
	code := fabriqerr.CodeOf(err)
	return code == fabriqerr.CodeNotFound || code == fabriqerr.CodeUnavailable
}

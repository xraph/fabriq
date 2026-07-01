package adminapi

import (
	"encoding/json"
	"net/http"

	"github.com/xraph/forge"

	corecache "github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/registry"
)

// cacheKeyspaceItem describes one entity's read-through cache keyspace, derived
// from its declarative registry.CacheSpec (mirrors cachequery.EntityQueryKeyspace
// without depending on it).
type cacheKeyspaceItem struct {
	Entity     string `json:"entity"`
	Name       string `json:"name"`
	Partition  string `json:"partition"`
	Mode       string `json:"mode"`
	TTLSeconds int64  `json:"ttlSeconds"`
	Scoped     bool   `json:"scoped"`
}

// cacheResponse is the payload for GET {BasePath}/cache.
type cacheResponse struct {
	// Configured reports whether an engine cache is wired (Redis present).
	Configured bool                `json:"configured"`
	Keyspaces  []cacheKeyspaceItem `json:"keyspaces"`
}

// cacheInvalidateRequest is the body for POST {BasePath}/cache/invalidate.
type cacheInvalidateRequest struct {
	Entity string `json:"entity"`
}

// registerCacheRoutes wires the cache introspection + invalidation routes.
func (c *adminController) registerCacheRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	listOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.cache"),
		forge.WithSummary("Report cache status + the entities' read-through keyspaces"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/cache", c.handleCache, listOpts...); err != nil {
		return err
	}

	statsOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.cache.stats"),
		forge.WithSummary("Report cache activity counters (hit-rate)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/cache/stats", c.handleCacheStats, statsOpts...); err != nil {
		return err
	}

	invOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.cache.invalidate"),
		forge.WithSummary("Invalidate an entity's cached reads (body: {entity})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/cache/invalidate", c.handleCacheInvalidate, invOpts...)
}

// cacheStatsResponse is the payload for GET {BasePath}/cache/stats.
type cacheStatsResponse struct {
	// Available is false when a cache is configured but exposes no counters
	// (e.g. an L1-wrapped cache that doesn't implement StatsReader).
	Available     bool    `json:"available"`
	Hits          int64   `json:"hits"`
	Misses        int64   `json:"misses"`
	Sets          int64   `json:"sets"`
	Invalidations int64   `json:"invalidations"`
	// HitRate is Hits/(Hits+Misses), 0 when there have been no lookups.
	HitRate float64 `json:"hitRate"`
}

// handleCacheStats serves GET {BasePath}/cache/stats — activity counters and the
// derived hit-rate. 501 when no cache is configured; {available:false} when the
// cache is configured but doesn't expose counters.
func (c *adminController) handleCacheStats(ctx forge.Context) error {
	cache := c.ext.resolveCache()
	if cache == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "cache not configured"})
	}
	sr, ok := cache.(corecache.StatsReader)
	if !ok {
		return ctx.JSON(http.StatusOK, cacheStatsResponse{Available: false})
	}
	s := sr.Stats()
	rate := 0.0
	if total := s.Hits + s.Misses; total > 0 {
		rate = float64(s.Hits) / float64(total)
	}
	return ctx.JSON(http.StatusOK, cacheStatsResponse{
		Available:     true,
		Hits:          s.Hits,
		Misses:        s.Misses,
		Sets:          s.Sets,
		Invalidations: s.Invalidations,
		HitRate:       rate,
	})
}

// handleCache serves GET {BasePath}/cache — whether a cache is configured and
// which registered entities opt into the read-through row cache.
func (c *adminController) handleCache(ctx forge.Context) error {
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}

	configured := c.ext.resolveCache() != nil

	items := make([]cacheKeyspaceItem, 0)
	for _, ent := range reg.All() {
		if ent.Spec.Cache == nil {
			continue
		}
		items = append(items, cacheKeyspaceItemFor(ent.Spec))
	}
	return ctx.JSON(http.StatusOK, cacheResponse{Configured: configured, Keyspaces: items})
}

// cacheKeyspaceItemFor derives the keyspace descriptor from an entity's
// CacheSpec, matching cachequery.EntityQueryKeyspace (name "<entity>:q",
// tenant/tenant-scope partition, Versioned mode, spec TTL).
func cacheKeyspaceItemFor(spec registry.EntitySpec) cacheKeyspaceItem {
	partition := "tenant"
	if spec.Cache.Scoped {
		partition = "tenant+scope"
	}
	return cacheKeyspaceItem{
		Entity:     spec.Name,
		Name:       spec.Name + ":q",
		Partition:  partition,
		Mode:       "versioned",
		TTLSeconds: int64(spec.Cache.TTL.Seconds()),
		Scoped:     spec.Cache.Scoped,
	}
}

// handleCacheInvalidate serves POST {BasePath}/cache/invalidate — bumps the
// entity's cache generation, orphaning every cached read of it for the tenant
// (and scope) in one O(1) operation. 501 when no cache is configured, 400 when
// the entity is unknown or not cached.
func (c *adminController) handleCacheInvalidate(ctx forge.Context) error {
	cache := c.ext.resolveCache()
	if cache == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "cache not configured"})
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}

	var req cacheInvalidateRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Entity == "" {
		return forge.BadRequest("field 'entity' is required")
	}
	ent, ok := reg.Get(req.Entity)
	if !ok {
		return forge.BadRequest("unknown entity: " + req.Entity)
	}
	if ent.Spec.Cache == nil {
		return forge.BadRequest("entity is not cached: " + req.Entity)
	}

	if invErr := cache.InvalidateEntity(ctx.Request().Context(), req.Entity); invErr != nil {
		return renderError(ctx, invErr)
	}
	return ctx.JSON(http.StatusOK, map[string]bool{"invalidated": true})
}

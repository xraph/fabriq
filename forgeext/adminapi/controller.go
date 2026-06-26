package adminapi

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

// defaultLimit is the default page size for entity listing.
const defaultLimit = 50

// maxLimit is the maximum page size the caller may request.
const maxLimit = 200

// capabilities lists the static feature set this admin API supports.
var capabilities = []string{"entities.read", "entities.write", "schema.read", "plugins.crud", "capabilities.read", "search.read", "vector.read", "graph.read", "files.read", "files.write", "crdt.read"}

// metaResponse is the payload for GET {BasePath}/meta.
type metaResponse struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	Capabilities []string `json:"capabilities"`
	// Tenant is the resolved tenant id from the request context. It is omitted
	// when no tenant has been stamped (e.g. unauthenticated or health-check callers).
	Tenant string `json:"tenant,omitempty"`
}

// entityItem is a single entity record in the list and detail responses.
type entityItem struct {
	ID   string         `json:"id"`
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
}

// entityListResponse is the payload for GET {BasePath}/entities.
type entityListResponse struct {
	Items      []entityItem `json:"items"`
	NextCursor string       `json:"nextCursor"`
}

// adminController registers all admin HTTP routes.
type adminController struct{ ext *Extension }

func newAdminController(e *Extension) *adminController { return &adminController{ext: e} }

func (c *adminController) Name() string { return "fabriq:admin" }

func (c *adminController) Routes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	metaOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.meta"),
		forge.WithSummary("Admin API metadata and capabilities"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/meta", c.handleMeta, metaOpts...); err != nil {
		return err
	}

	listOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.entities.list"),
		forge.WithSummary("List entities by type (requires ?type=)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/entities", c.handleList, listOpts...); err != nil {
		return err
	}

	// Register the schema/types introspection routes before the dynamic
	// /entities/:id detail route so the static /entities/types segment is not
	// captured as an :id by routers that match in registration order.
	if err := c.registerSchemaRoutes(r); err != nil {
		return err
	}

	detailOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.entities.get"),
		forge.WithSummary("Get a single entity by type and id (requires ?type=)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/entities/:id", c.handleGet, detailOpts...); err != nil {
		return err
	}

	if err := c.registerEntityWriteRoutes(r); err != nil {
		return err
	}

	if err := c.registerCapabilityRoutes(r); err != nil {
		return err
	}

	if err := c.registerSearchRoutes(r); err != nil {
		return err
	}

	if err := c.registerGraphRoutes(r); err != nil {
		return err
	}

	if err := c.registerFileRoutes(r); err != nil {
		return err
	}

	if err := c.registerCrdtRoutes(r); err != nil {
		return err
	}

	return c.registerPluginRoutes(r)
}

// handleMeta serves GET {BasePath}/meta.
func (c *adminController) handleMeta(ctx forge.Context) error {
	resp := metaResponse{
		Name:         "fabriq-admin-api",
		Version:      Version,
		Capabilities: capabilities,
	}
	// Populate the resolved tenant when one is present. ErrNoTenant is the
	// expected sentinel for unauthenticated or tenant-agnostic callers; all
	// other errors are also non-fatal here — we simply omit the field.
	if tid, err := tenant.FromContext(ctx.Request().Context()); err == nil {
		resp.Tenant = tid
	}
	return ctx.JSON(http.StatusOK, resp)
}

// handleList serves GET {BasePath}/entities.
//
// Required query params:
//
//	type   entity type name (e.g. "asset", "site")
//
// Optional query params:
//
//	limit  page size (default 50, max 200)
//	cursor offset expressed as an integer position (simple numeric cursor)
func (c *adminController) handleList(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	entityType := ctx.Query("type")
	if entityType == "" {
		return forge.BadRequest("query param 'type' is required")
	}

	limit := defaultLimit
	if lStr := ctx.Query("limit"); lStr != "" {
		l, parseErr := strconv.Atoi(lStr)
		if parseErr != nil || l < 1 {
			return forge.BadRequest("query param 'limit' must be a positive integer")
		}
		if l > maxLimit {
			l = maxLimit
		}
		limit = l
	}

	offset := 0
	if cStr := ctx.Query("cursor"); cStr != "" {
		o, parseErr := strconv.Atoi(cStr)
		if parseErr != nil || o < 0 {
			return forge.BadRequest("query param 'cursor' must be a non-negative integer")
		}
		offset = o
	}

	reqCtx := ctx.Request().Context()
	q := query.ListQuery{Limit: limit + 1, Offset: offset} // fetch one extra to detect next page
	var rows []map[string]any
	if listErr := fab.Relational().List(reqCtx, entityType, q, &rows); listErr != nil {
		return mapQueryError(listErr)
	}

	nextCursor := ""
	if len(rows) > limit {
		rows = rows[:limit]
		nextCursor = strconv.Itoa(offset + limit)
	}

	items := make([]entityItem, 0, len(rows))
	for _, row := range rows {
		id, _ := row["id"].(string)
		items = append(items, entityItem{ID: id, Type: entityType, Data: row})
	}

	return ctx.JSON(http.StatusOK, entityListResponse{
		Items:      items,
		NextCursor: nextCursor,
	})
}

// handleGet serves GET {BasePath}/entities/:id.
//
// Required query params:
//
//	type  entity type name (e.g. "asset", "site")
func (c *adminController) handleGet(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	entityType := ctx.Query("type")
	if entityType == "" {
		return forge.BadRequest("query param 'type' is required")
	}

	id := ctx.Param("id")
	if id == "" {
		return forge.BadRequest("path param 'id' is required")
	}

	reqCtx := ctx.Request().Context()
	// List is the map-native hydration path. Use Get with a map target:
	// RelationalQuerier.Get scans into any, and for dynamic/map targets we
	// use list-then-filter. However, because Get accepts 'any' and our
	// hydration target for a type we don't know at compile time must be a
	// map, we leverage List with an id equality filter instead.
	q := query.ListQuery{
		Where: query.Where{query.Eq("id", id)},
		Limit: 1,
	}
	var rows []map[string]any
	if listErr := fab.Relational().List(reqCtx, entityType, q, &rows); listErr != nil {
		return mapQueryError(listErr)
	}
	if len(rows) == 0 {
		return forge.NotFound("entity not found")
	}

	row := rows[0]
	return ctx.JSON(http.StatusOK, entityItem{ID: id, Type: entityType, Data: row})
}

// mapQueryError translates fabriq domain errors to forge HTTP errors.
func mapQueryError(err error) error {
	if errors.Is(err, fabriqerr.ErrNotFound) {
		return forge.NotFound("entity not found")
	}
	// Surface unknown entity errors as 400 — the type param was invalid.
	if isUnknownEntityErr(err) {
		return forge.BadRequest(err.Error())
	}
	return forge.InternalError(err)
}

// isUnknownEntityErr reports whether err is the "unknown entity" sentinel
// produced by FakeRelational, the real adapter, and the command executor when
// the entity type name is not registered. The command plane wraps the sentinel
// with a "command N (verb entity): ..." prefix, so a substring match is used
// rather than a prefix check to cover both the read (unwrapped) and write
// (wrapped) paths.
func isUnknownEntityErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "fabriq: unknown entity \"")
}

var _ forge.Controller = (*adminController)(nil)

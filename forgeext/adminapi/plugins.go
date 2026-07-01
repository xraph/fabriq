package adminapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// PluginRemoteEntityType is the well-known dynamic entity type name for remote
// plugin specs. The host application must register a matching EntitySpec (with
// a DynamicSchema) before using these endpoints; see PluginRemoteSpec.
const PluginRemoteEntityType = "admin_plugin_remote"

// PluginRemoteSpec returns the canonical EntitySpec for admin_plugin_remote.
// The host application calls reg.Register(adminapi.PluginRemoteSpec()) during
// startup so the entity is available to the command plane and the relational
// querier. In unit tests, register it via buildPluginWorld or equivalent.
func PluginRemoteSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: PluginRemoteEntityType,
		Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_admin_plugin_remote",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "url", Type: registry.ColText, NotNull: true},
				{Name: "scope", Type: registry.ColText, NotNull: true},
				{Name: "module", Type: registry.ColText, NotNull: true},
			},
		},
	}
}

// pluginRemote is the JSON representation of one remote plugin spec.
// Fields use camelCase JSON tags per the fabriq camelCase-JSON convention.
type pluginRemote struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	URL    string `json:"url"`
	Scope  string `json:"scope"`
	Module string `json:"module"`
}

// pluginListResponse is the payload for GET {BasePath}/plugins.
type pluginListResponse struct {
	Items []pluginRemote `json:"items"`
}

// pluginCreateRequest is the request body for POST {BasePath}/plugins.
type pluginCreateRequest struct {
	Name   string `json:"name"`
	URL    string `json:"url"`
	Scope  string `json:"scope"`
	Module string `json:"module"`
}

// registerPluginRoutes wires the plugin CRUD routes onto the given router.
// It is called from adminController.Routes so all plugin routes share the same
// route options (auth middleware) as the entity-read routes.
func (c *adminController) registerPluginRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	listOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.plugins.list"),
		forge.WithSummary("List remote plugin specs"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/plugins", c.handleListPlugins, listOpts...); err != nil {
		return err
	}

	createOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.plugins.create"),
		forge.WithSummary("Register a remote plugin spec"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.POST(base+"/plugins", c.handleCreatePlugin, createOpts...); err != nil {
		return err
	}

	deleteOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.plugins.delete"),
		forge.WithSummary("Delete a remote plugin spec by id"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.DELETE(base+"/plugins/:id", c.handleDeletePlugin, deleteOpts...)
}

// handleListPlugins serves GET {BasePath}/plugins.
// Returns the tenant-scoped list of remote plugin specs, or an empty items
// array when none are registered.
func (c *adminController) handleListPlugins(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	reqCtx := ctx.Request().Context()
	q := query.ListQuery{Limit: maxLimit}
	var rows []map[string]any
	if listErr := fab.Relational().List(reqCtx, PluginRemoteEntityType, q, &rows); listErr != nil {
		if unknownEntityCode(listErr) {
			// Entity type not registered on this host — return empty list rather
			// than 400, because the admin UI polls this before any plugin is added.
			return ctx.JSON(http.StatusOK, pluginListResponse{Items: []pluginRemote{}})
		}
		return renderError(ctx, listErr)
	}

	items := make([]pluginRemote, 0, len(rows))
	for _, row := range rows {
		items = append(items, rowToPlugin(row))
	}

	return ctx.JSON(http.StatusOK, pluginListResponse{Items: items})
}

// handleCreatePlugin serves POST {BasePath}/plugins.
// Validates url/scope/module; mints a server-side ULID; persists via Exec.
func (c *adminController) handleCreatePlugin(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req pluginCreateRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}

	if req.URL == "" {
		return forge.BadRequest("field 'url' is required")
	}
	if req.Scope == "" {
		return forge.BadRequest("field 'scope' is required")
	}
	if req.Module == "" {
		return forge.BadRequest("field 'module' is required")
	}

	// Mint a ULID as the aggregate id. The command executor accepts an explicit
	// AggID for creates, which is preserved as the stored id column value.
	id := event.NewID()

	reqCtx := ctx.Request().Context()
	res, execErr := fab.Exec(reqCtx, command.Command{
		Entity: PluginRemoteEntityType,
		Op:     command.OpCreate,
		AggID:  id,
		Payload: map[string]any{
			"name":   req.Name,
			"url":    req.URL,
			"scope":  req.Scope,
			"module": req.Module,
		},
	})
	if execErr != nil {
		return forge.InternalError(execErr)
	}

	return ctx.JSON(http.StatusCreated, pluginRemote{
		ID:     res.AggID,
		Name:   req.Name,
		URL:    req.URL,
		Scope:  req.Scope,
		Module: req.Module,
	})
}

// handleDeletePlugin serves DELETE {BasePath}/plugins/{id}.
// Returns 204 on success, 404 if the plugin does not exist.
func (c *adminController) handleDeletePlugin(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	id := ctx.Param("id")
	if id == "" {
		return forge.BadRequest("path param 'id' is required")
	}

	reqCtx := ctx.Request().Context()

	// Confirm existence before attempting delete so we can distinguish
	// a missing record (404) from other errors.
	q := query.ListQuery{
		Where: query.Where{query.Eq("id", id)},
		Limit: 1,
	}
	var rows []map[string]any
	if listErr := fab.Relational().List(reqCtx, PluginRemoteEntityType, q, &rows); listErr != nil {
		if unknownEntityCode(listErr) {
			return forge.NotFound("plugin not found")
		}
		return renderError(ctx, listErr)
	}
	if len(rows) == 0 {
		return forge.NotFound("plugin not found")
	}

	_, execErr := fab.Exec(reqCtx, command.Command{
		Entity: PluginRemoteEntityType,
		Op:     command.OpDelete,
		AggID:  id,
	})
	if execErr != nil {
		if errors.Is(execErr, fabriqerr.ErrNotFound) {
			return forge.NotFound("plugin not found")
		}
		return forge.InternalError(execErr)
	}

	ctx.Response().WriteHeader(http.StatusNoContent)
	return nil
}

// rowToPlugin converts a map-native dynamic entity row to a pluginRemote.
func rowToPlugin(row map[string]any) pluginRemote {
	return pluginRemote{
		ID:     stringVal(row["id"]),
		Name:   stringVal(row["name"]),
		URL:    stringVal(row["url"]),
		Scope:  stringVal(row["scope"]),
		Module: stringVal(row["module"]),
	}
}

// stringVal extracts a string from a map value, returning "" for nil or non-string.
func stringVal(v any) string {
	s, _ := v.(string)
	return s
}

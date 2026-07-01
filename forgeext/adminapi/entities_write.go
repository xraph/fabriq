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
)

// entityWriteRequest is the request body for POST and PUT {BasePath}/entities.
// It is generic over the dynamic entity type: Type names the registered entity
// and Data carries the column-keyed payload. Fields use camelCase JSON tags per
// the fabriq camelCase-JSON convention.
type entityWriteRequest struct {
	// Type is the registered dynamic entity type name (e.g. "product").
	Type string `json:"type"`
	// Data is the column-keyed payload written to the row. For a create it is
	// the full set of domain columns; for an update it fully replaces the row's
	// domain columns (the command plane performs a full-row replace).
	Data map[string]any `json:"data"`
}

// registerEntityWriteRoutes wires the entity create/update/delete routes onto
// the given router. It is called from adminController.Routes so all write routes
// share the same route options (auth middleware) as the entity-read routes.
//
// The write path supports DYNAMIC entities only (those declared with a
// registry.DynamicSchema), consistent with the existing read path: dynamic
// entities accept map[string]any payloads natively on both the fakes and the
// real grove adapter, so no compile-time model type is required.
func (c *adminController) registerEntityWriteRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	createOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.entities.create"),
		forge.WithSummary("Create an entity row (body: {type, data})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.POST(base+"/entities", c.handleCreateEntity, createOpts...); err != nil {
		return err
	}

	updateOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.entities.update"),
		forge.WithSummary("Update an entity row by id (body: {type, data})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.PUT(base+"/entities/:id", c.handleUpdateEntity, updateOpts...); err != nil {
		return err
	}

	deleteOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.entities.delete"),
		forge.WithSummary("Delete an entity row by id (requires ?type=)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.DELETE(base+"/entities/:id", c.handleDeleteEntity, deleteOpts...)
}

// handleCreateEntity serves POST {BasePath}/entities.
//
// Request body:
//
//	{ "type": "<entityName>", "data": { ...columns } }
//
// The id is generated server-side (a ULID) when absent. On success it returns
// 201 with {id, type, data}. A missing type yields 400; an unknown type or a
// validation failure is surfaced from the command plane.
func (c *adminController) handleCreateEntity(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req entityWriteRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Type == "" {
		return forge.BadRequest("field 'type' is required")
	}

	// Mint a ULID as the aggregate id. The command executor preserves an
	// explicit AggID as the stored id column value.
	id := event.NewID()
	if existing, ok := req.Data["id"].(string); ok && existing != "" {
		id = existing
	}

	reqCtx := ctx.Request().Context()
	res, execErr := fab.Exec(reqCtx, command.Command{
		Entity:  req.Type,
		Op:      command.OpCreate,
		AggID:   id,
		Payload: sanitizePayload(req.Data),
	})
	if execErr != nil {
		return mapWriteError(execErr)
	}

	return ctx.JSON(http.StatusCreated, entityItem{
		ID:   res.AggID,
		Type: req.Type,
		Data: req.Data,
	})
}

// handleUpdateEntity serves PUT {BasePath}/entities/:id.
//
// Request body:
//
//	{ "type": "<entityName>", "data": { ...columns } }
//
// The update is a full-row replace of the domain columns (the command plane's
// OpUpdate semantics). On success it returns 200 with {id, type, data}. A
// missing type yields 400; an unknown id yields 404 (OpUpdate on an absent
// aggregate is fabriqerr.ErrNotFound).
func (c *adminController) handleUpdateEntity(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	id := ctx.Param("id")
	if id == "" {
		return forge.BadRequest("path param 'id' is required")
	}

	var req entityWriteRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Type == "" {
		return forge.BadRequest("field 'type' is required")
	}

	reqCtx := ctx.Request().Context()
	if _, execErr := fab.Exec(reqCtx, command.Command{
		Entity:  req.Type,
		Op:      command.OpUpdate,
		AggID:   id,
		Payload: sanitizePayload(req.Data),
	}); execErr != nil {
		return mapWriteError(execErr)
	}

	return ctx.JSON(http.StatusOK, entityItem{
		ID:   id,
		Type: req.Type,
		Data: req.Data,
	})
}

// handleDeleteEntity serves DELETE {BasePath}/entities/:id.
//
// Required query params:
//
//	type  entity type name (e.g. "product")
//
// On success it returns 204. A missing type yields 400; an unknown id yields
// 404. Existence is confirmed before the delete so a missing row is reported as
// 404 rather than a silent no-op.
func (c *adminController) handleDeleteEntity(ctx forge.Context) error {
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

	// Confirm existence before attempting delete so we can distinguish a missing
	// record (404) from other errors and stay consistent with the plugin delete.
	q := query.ListQuery{
		Where: query.Where{query.Eq("id", id)},
		Limit: 1,
	}
	var rows []map[string]any
	if listErr := fab.Relational().List(reqCtx, entityType, q, &rows); listErr != nil {
		if isUnknownEntityErr(listErr) {
			return forge.BadRequest(listErr.Error())
		}
		return mapQueryError(listErr)
	}
	if len(rows) == 0 {
		return forge.NotFound("entity not found")
	}

	if _, execErr := fab.Exec(reqCtx, command.Command{
		Entity: entityType,
		Op:     command.OpDelete,
		AggID:  id,
	}); execErr != nil {
		return mapWriteError(execErr)
	}

	ctx.Response().WriteHeader(http.StatusNoContent)
	return nil
}

// sanitizePayload returns a shallow copy of data with the structural "id" key
// removed. The id is carried via Command.AggID; leaving it in the column-keyed
// payload would shadow a structural field. A nil input yields a non-nil empty
// map so the command plane always receives a valid payload.
func sanitizePayload(data map[string]any) map[string]any {
	out := make(map[string]any, len(data))
	for k, v := range data {
		if k == "id" {
			continue
		}
		out[k] = v
	}
	return out
}

// mapWriteError translates command-plane write errors to forge HTTP errors:
// a missing aggregate (OpUpdate/OpDelete) is 404, an unknown entity type is
// 400, and everything else is 500.
func mapWriteError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, fabriqerr.ErrNotFound) {
		return forge.NotFound("entity not found")
	}
	if isUnknownEntityErr(err) {
		return forge.BadRequest(err.Error())
	}
	return forge.InternalError(err)
}

package adminapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// maxBatchCommands bounds a single all-or-nothing batch so one request cannot
// pin a transaction open over an unbounded number of writes.
const maxBatchCommands = 100

// commandRequest is one raw command against the write plane. It mirrors
// command.Command in camelCase JSON. Op is the string form of the verb.
type commandRequest struct {
	// Entity is the registered entity name (e.g. "product").
	Entity string `json:"entity"`
	// Op is the command verb: create | update | delete | upsert.
	Op string `json:"op"`
	// AggID identifies the aggregate. Required for update/delete/upsert; a ULID
	// is minted for create when omitted.
	AggID string `json:"aggId,omitempty"`
	// Payload is the column-keyed body (create/update/upsert). Ignored for delete.
	Payload map[string]any `json:"payload,omitempty"`
	// ExpectedVersion enables optimistic concurrency — a mismatch is a 409.
	ExpectedVersion *int64 `json:"expectedVersion,omitempty"`
}

// commandBatchRequest is the body for POST {BasePath}/commands/batch — an
// ordered set of commands applied all-or-nothing in one transaction.
type commandBatchRequest struct {
	Commands []commandRequest `json:"commands"`
}

// commandResultItem is the outcome of one command.
type commandResultItem struct {
	AggID   string `json:"aggId"`
	Version int64  `json:"version"`
	EventID string `json:"eventId"`
}

type commandResponse struct {
	Result commandResultItem `json:"result"`
}

type commandBatchResponse struct {
	Results []commandResultItem `json:"results"`
}

// parseCommandOp maps the string verb to a command.Op.
func parseCommandOp(s string) (command.Op, bool) {
	switch s {
	case "create":
		return command.OpCreate, true
	case "update":
		return command.OpUpdate, true
	case "delete":
		return command.OpDelete, true
	case "upsert":
		return command.OpUpsert, true
	}
	return 0, false
}

// toCommand validates a request and builds the command.Command. A create with
// no aggId mints a ULID; update/delete/upsert require an explicit aggId.
func toCommand(req commandRequest) (command.Command, error) {
	if req.Entity == "" {
		return command.Command{}, fmt.Errorf("field 'entity' is required")
	}
	op, ok := parseCommandOp(req.Op)
	if !ok {
		return command.Command{}, fmt.Errorf("field 'op' must be one of create|update|delete|upsert, got %q", req.Op)
	}
	aggID := req.AggID
	if aggID == "" {
		if op == command.OpCreate {
			aggID = event.NewID()
		} else {
			return command.Command{}, fmt.Errorf("field 'aggId' is required for op %q", req.Op)
		}
	}
	return command.Command{
		Entity:          req.Entity,
		Op:              op,
		AggID:           aggID,
		Payload:         sanitizePayload(req.Payload),
		ExpectedVersion: req.ExpectedVersion,
	}, nil
}

func toResultItem(r command.Result) commandResultItem {
	return commandResultItem{AggID: r.AggID, Version: r.Version, EventID: r.EventID}
}

// registerCommandRoutes wires the raw command / batch write routes.
func (c *adminController) registerCommandRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	// Register the more specific /commands/batch first so a router that matches
	// in registration order does not capture it under /commands.
	batchOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.commands.batch"),
		forge.WithSummary("Run N commands in one all-or-nothing transaction (body: {commands:[…]})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.POST(base+"/commands/batch", c.handleExecBatch, batchOpts...); err != nil {
		return err
	}

	execOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.commands.exec"),
		forge.WithSummary("Run one command (body: {entity, op, aggId?, payload, expectedVersion?})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/commands", c.handleExecCommand, execOpts...)
}

// handleExecCommand serves POST {BasePath}/commands — one command through the
// write plane. 400 on a malformed command, 404 on a missing aggregate
// (update/delete), 409 on a version conflict.
func (c *adminController) handleExecCommand(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req commandRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	cmd, buildErr := toCommand(req)
	if buildErr != nil {
		return forge.BadRequest(buildErr.Error())
	}

	res, execErr := fab.Exec(ctx.Request().Context(), cmd)
	if execErr != nil {
		return c.mapCommandError(ctx, execErr)
	}
	return ctx.JSON(http.StatusOK, commandResponse{Result: toResultItem(res)})
}

// handleExecBatch serves POST {BasePath}/commands/batch — N commands applied
// ordered and all-or-nothing in one transaction. Any command's failure rolls the
// whole batch back and is reported as that command's error (400/404/409/500).
func (c *adminController) handleExecBatch(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req commandBatchRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if len(req.Commands) == 0 {
		return forge.BadRequest("field 'commands' must be a non-empty array")
	}
	if len(req.Commands) > maxBatchCommands {
		return forge.BadRequest(fmt.Sprintf("batch too large: %d commands (max %d)", len(req.Commands), maxBatchCommands))
	}

	cmds := make([]command.Command, 0, len(req.Commands))
	for i, cr := range req.Commands {
		cmd, buildErr := toCommand(cr)
		if buildErr != nil {
			return forge.BadRequest(fmt.Sprintf("command %d: %s", i, buildErr.Error()))
		}
		cmds = append(cmds, cmd)
	}

	results, execErr := fab.ExecBatch(ctx.Request().Context(), cmds)
	if execErr != nil {
		return c.mapCommandError(ctx, execErr)
	}

	items := make([]commandResultItem, 0, len(results))
	for _, r := range results {
		items = append(items, toResultItem(r))
	}
	return ctx.JSON(http.StatusOK, commandBatchResponse{Results: items})
}

// mapCommandError maps command-plane errors to HTTP: a version conflict is 409;
// everything else defers to mapWriteError (404 missing aggregate, 400 unknown
// entity, 500 otherwise).
func (c *adminController) mapCommandError(ctx forge.Context, err error) error {
	if errors.Is(err, fabriqerr.ErrVersionConflict) {
		return ctx.JSON(http.StatusConflict, map[string]string{"error": err.Error()})
	}
	return mapWriteError(err)
}

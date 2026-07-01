package adminapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/command"
)

// rememberResponse is the payload for a successful POST {BasePath}/agent/remember.
type rememberResponse struct {
	Result commandResultItem `json:"result"`
}

// writePolicyResponse reports the agent write allowlist (deny-by-default): a map
// of entity name → permitted ops.
type writePolicyResponse struct {
	Allow map[string][]string `json:"allow"`
}

// registerAgentWriteRoutes wires the guarded-write (Remember) routes.
func (c *adminController) registerAgentWriteRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	policyOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.agent.writePolicy"),
		forge.WithSummary("Report the agent write allowlist (deny-by-default)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/agent/write-policy", c.handleWritePolicy, policyOpts...); err != nil {
		return err
	}

	rememberOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.agent.remember"),
		forge.WithSummary("Guarded agent write (body: {entity, op, aggId?, payload, expectedVersion?})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/agent/remember", c.handleRemember, rememberOpts...)
}

// handleWritePolicy serves GET {BasePath}/agent/write-policy — the configured
// allowlist. An empty allow map means every write is denied (the default).
func (c *adminController) handleWritePolicy(ctx forge.Context) error {
	allow := make(map[string][]string, len(c.ext.cfg.WritePolicy.Allow))
	for entity, ops := range c.ext.cfg.WritePolicy.Allow {
		ss := make([]string, 0, len(ops))
		for _, op := range ops {
			ss = append(ss, opString(op))
		}
		allow[entity] = ss
	}
	return ctx.JSON(http.StatusOK, writePolicyResponse{Allow: allow})
}

// handleRemember serves POST {BasePath}/agent/remember — a guarded write through
// the agent toolkit. Power is WritePolicy ∩ tenant scope ∩ lifecycle-hook rules.
// WriteError codes map to HTTP: validation_failed→400, not_allowed→403,
// version_conflict→409, not_found→404, else 500.
func (c *adminController) handleRemember(ctx forge.Context) error {
	tk, err := c.buildWriteToolkit()
	if err != nil {
		return forge.InternalError(err)
	}

	var req agent.RememberRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}

	res, remErr := tk.Remember(ctx.Request().Context(), req)
	if remErr != nil {
		return c.mapRememberError(ctx, remErr)
	}
	return ctx.JSON(http.StatusOK, rememberResponse{Result: toResultItem(res)})
}

// buildWriteToolkit constructs an agent.Toolkit carrying the configured
// WritePolicy. Remember never embeds, so no embedder is required (NewToolkit
// accepts a nil embedder). Shares the fabric + registry resolution used by
// recall/distill.
func (c *adminController) buildWriteToolkit() (*agent.Toolkit, error) {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return nil, err
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return nil, err
	}
	return agent.NewToolkit(fab, reg, nil, agent.Config{Write: c.ext.cfg.WritePolicy})
}

// mapRememberError maps a typed agent.WriteError code to an HTTP status, echoing
// the machine-readable code so the SPA can branch (e.g. render a policy-denied
// state distinctly from a validation error).
func (c *adminController) mapRememberError(ctx forge.Context, err error) error {
	var we *agent.WriteError
	if errors.As(err, &we) {
		status := http.StatusInternalServerError
		switch we.Code {
		case "validation_failed":
			status = http.StatusBadRequest
		case "not_allowed":
			status = http.StatusForbidden
		case "version_conflict":
			status = http.StatusConflict
		case "not_found":
			status = http.StatusNotFound
		}
		return ctx.JSON(status, map[string]string{"error": we.Error(), "code": we.Code})
	}
	return forge.InternalError(err)
}

// opString renders a command.Op as its lowercase verb (inverse of parseCommandOp).
func opString(op command.Op) string {
	switch op {
	case command.OpCreate:
		return "create"
	case command.OpUpdate:
		return "update"
	case command.OpDelete:
		return "delete"
	case command.OpUpsert:
		return "upsert"
	}
	return "unknown"
}

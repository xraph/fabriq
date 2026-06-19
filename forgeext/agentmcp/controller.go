package agentmcp

import (
	"io"

	"github.com/xraph/forge"
)

type mcpController struct{ ext *Extension }

func newMCPController(e *Extension) *mcpController { return &mcpController{ext: e} }

func (c *mcpController) Name() string { return "fabriq:agent:mcp" }

func (c *mcpController) Routes(r forge.Router) error {
	opts := append([]forge.RouteOption{
		forge.WithName("fabriq.agent.mcp"),
		forge.WithSummary("Agent toolkit MCP endpoint (JSON-RPC tools/list + tools/call)"),
		forge.WithTags("Fabriq", "Agent", "MCP"),
	}, c.ext.cfg.RouteOptions...)
	return r.POST(c.ext.cfg.BasePath, c.handle, opts...)
}

func (c *mcpController) handle(ctx forge.Context) error {
	tk, err := c.ext.resolveToolkit()
	if err != nil {
		return forge.InternalError(err)
	}
	body, err := io.ReadAll(ctx.Request().Body)
	if err != nil {
		return forge.BadRequest("read body: " + err.Error())
	}
	resp := Dispatch(ctx.Request().Context(), tk, body)
	ctx.Response().Header().Set("Content-Type", "application/json")
	_, _ = ctx.Response().Write(resp)
	return nil
}

var _ forge.Controller = (*mcpController)(nil)

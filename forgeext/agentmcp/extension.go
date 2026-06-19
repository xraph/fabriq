// Package agentmcp exposes the fabriq agent toolkit as a Forge extension over
// MCP (JSON-RPC 2.0). The Dispatch function (dispatch.go) is Forge-free;
// only the Extension and controller in this file touch Forge.
package agentmcp

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/forgeext"
)

// config holds the MCP extension's options.
type config struct {
	BasePath     string
	WatchPath    string
	Embedder     agent.Embedder
	Toolkit      agent.Config
	RouteOptions []forge.RouteOption
}

// Option configures the MCP extension.
type Option func(*config)

// WithBasePath sets the MCP POST endpoint path (default "/api/v1/agent/mcp").
func WithBasePath(p string) Option { return func(c *config) { c.BasePath = p } }

// WithEmbedder supplies the embedding model for semantic recall.
func WithEmbedder(e agent.Embedder) Option { return func(c *config) { c.Embedder = e } }

// WithWritePolicy sets the agent write allowlist (empty = no writes).
func WithWritePolicy(p agent.WritePolicy) Option {
	return func(c *config) { c.Toolkit.Write = p }
}

// WithConfig sets the full toolkit Config. Apply WithConfig BEFORE WithWritePolicy:
// a later WithConfig call replaces c.Toolkit entirely, clobbering any earlier
// WithWritePolicy; a later WithWritePolicy only sets c.Toolkit.Write.
func WithConfig(cfg agent.Config) Option { return func(c *config) { c.Toolkit = cfg } }

// WithRouteOptions forwards forge route options (auth middleware, OpenAPI) to
// the MCP route — fabriq stays auth-agnostic.
func WithRouteOptions(opts ...forge.RouteOption) Option {
	return func(c *config) { c.RouteOptions = append(c.RouteOptions, opts...) }
}

// Extension exposes the agent toolkit over MCP as a Forge extension. It depends
// on the "fabriq" extension and builds its toolkit in Start.
type Extension struct {
	forge.BaseExtension
	fab *forgeext.Extension
	cfg config

	mu sync.Mutex
	tk *agent.Toolkit
}

// NewMCP builds the MCP extension wired to a started fabriq Extension.
// The endpoint is auth-agnostic: the host MUST attach authentication via
// WithRouteOptions (e.g. a bearer-token middleware), otherwise recall, writes,
// and graph_traverse are exposed to any caller the router admits.
func NewMCP(fab *forgeext.Extension, opts ...Option) *Extension {
	cfg := config{BasePath: "/api/v1/agent/mcp"}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.BasePath == "" {
		cfg.BasePath = "/api/v1/agent/mcp"
	}
	if cfg.WatchPath == "" {
		cfg.WatchPath = cfg.BasePath + "/watch"
	}
	return &Extension{fab: fab, cfg: cfg}
}

func (e *Extension) Name() string        { return "fabriq-agent-mcp" }
func (e *Extension) Version() string     { return forgeext.Version }
func (e *Extension) Description() string { return "fabriq agent toolkit over MCP (JSON-RPC tools/list + tools/call)" }
func (e *Extension) Dependencies() []string { return []string{"fabriq"} }

// Register registers the MCP controller and watch SSE controller; the toolkit
// resolves lazily (built in Start).
func (e *Extension) Register(app forge.App) error {
	if err := e.BaseExtension.Register(app); err != nil {
		return err
	}
	if err := app.RegisterController(newMCPController(e)); err != nil {
		return err
	}
	return app.RegisterController(newWatchController(e))
}

// Start builds the toolkit over the started fabriq facade.
func (e *Extension) Start(_ context.Context) error {
	f := e.fab.Fabriq()
	if f == nil {
		return fmt.Errorf("fabriq-agent-mcp: requires the fabriq facade (started)")
	}
	tk, err := agent.NewToolkit(f, f.Registry(), e.cfg.Embedder, e.cfg.Toolkit)
	if err != nil {
		return fmt.Errorf("fabriq-agent-mcp: build toolkit: %w", err)
	}
	e.mu.Lock()
	e.tk = tk
	e.mu.Unlock()
	e.MarkStarted()
	return nil
}

// Stop stops the extension.
func (e *Extension) Stop(_ context.Context) error { e.MarkStopped(); return nil }

// resolveToolkit returns the toolkit, or an error before Start.
func (e *Extension) resolveToolkit() (*agent.Toolkit, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tk == nil {
		return nil, fmt.Errorf("fabriq-agent-mcp: not started")
	}
	return e.tk, nil
}

var _ forge.Extension = (*Extension)(nil)

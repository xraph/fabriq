// Package adminapi exposes a read-only admin HTTP surface over the fabriq
// query facade. It is designed for consumption by the fabriq-admin SPA and
// any operator tooling that needs a stable, auth-agnostic read API over the
// data fabric.
//
// The extension is auth-agnostic: the host MUST attach authentication and
// tenant-injection middleware via WithRouteOptions. Fabriq adminapi adds no
// auth of its own so that the host controls the security boundary entirely.
//
// Phase 1 endpoints:
//
//	GET {BasePath}/meta            → service metadata + capabilities
//	GET {BasePath}/entities        → paginated entity list (requires ?type=)
//	GET {BasePath}/entities/{id}   → single entity by type and id (requires ?type=)
//	GET {BasePath}/capabilities    → fabric subsystem capabilities (instance-level,
//	                                 or per-type with ?type=)
package adminapi

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/forgeext"
)

// Version is the adminapi extension version.
const Version = forgeext.Version

// config holds the adminapi extension options.
type config struct {
	BasePath     string
	RouteOptions []forge.RouteOption
	// Embedder turns query text into a vector for the text-mode vector search
	// endpoint (POST {BasePath}/search/vector with a {query} body). It is
	// optional: when nil, the text mode returns 501 and only the
	// similar-to-entity mode ({id} body) is available. The similar-to-entity
	// mode needs no embedder because it reuses a stored embedding.
	Embedder agent.Embedder
}

// Option configures the adminapi extension.
type Option func(*config)

// WithBasePath sets the admin API base path (default "/admin").
func WithBasePath(p string) Option { return func(c *config) { c.BasePath = p } }

// WithRouteOptions forwards forge route options (auth middleware, OpenAPI
// decorators) to all admin routes — the extension stays auth-agnostic.
func WithRouteOptions(opts ...forge.RouteOption) Option {
	return func(c *config) { c.RouteOptions = append(c.RouteOptions, opts...) }
}

// WithEmbedder supplies the embedding model used by the text-mode vector
// search endpoint (POST {BasePath}/search/vector with a {query} body). Fabriq
// stays model-agnostic: the host provides the implementation (Anthropic,
// OpenAI, a local model). When unset, text-mode vector search returns 501 and
// callers must use the similar-to-entity mode ({id} body) instead.
func WithEmbedder(e agent.Embedder) Option {
	return func(c *config) { c.Embedder = e }
}

// Extension exposes the fabriq data fabric as a read-only admin HTTP surface.
// It depends on the "fabriq" extension and resolves its query facade at Start.
type Extension struct {
	forge.BaseExtension
	parent *forgeext.Extension
	cfg    config

	mu     sync.Mutex
	fabric query.Fabric       // resolved in Start
	fab    *fabriq.Fabriq     // concrete facade, resolved in Start (powers the file-plane endpoints)
	reg    *registry.Registry // schema registry, resolved in Start (powers types/schema introspection)
}

// NewAdminAPI builds the adminapi extension wired to a started fabriq Extension.
// The endpoint is auth-agnostic: the host MUST attach authentication and
// tenant-injection middleware via WithRouteOptions.
func NewAdminAPI(fab *forgeext.Extension, opts ...Option) *Extension {
	cfg := config{BasePath: "/admin"}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.BasePath == "" {
		cfg.BasePath = "/admin"
	}
	return &Extension{parent: fab, cfg: cfg}
}

// Name implements forge.Extension.
func (e *Extension) Name() string { return "fabriq-admin-api" }

// Version implements forge.Extension.
func (e *Extension) Version() string { return Version }

// Description implements forge.Extension.
func (e *Extension) Description() string {
	return "fabriq admin HTTP API (meta, entities read; plugins CRUD)"
}

// Dependencies implements forge.Extension.
func (e *Extension) Dependencies() []string { return []string{"fabriq"} }

// Register registers the admin controller routes; the fabric resolves lazily in Start.
func (e *Extension) Register(app forge.App) error {
	if err := e.BaseExtension.Register(app); err != nil {
		return err
	}
	return app.RegisterController(newAdminController(e))
}

// Start resolves the fabriq facade from the started fabriq extension.
func (e *Extension) Start(_ context.Context) error {
	f := e.parent.Fabriq()
	if f == nil {
		return fmt.Errorf("fabriq-admin-api: requires the fabriq facade (started)")
	}
	e.mu.Lock()
	e.fabric = f
	// f is a *fabriq.Fabriq, which exposes the schema registry used by the
	// types/schema introspection endpoints AND the file-plane methods
	// (ListChildren/CreateFolder/CreateFile/GetNode/TrashNode/GetBlob) the
	// query.Fabric interface does not surface.
	e.fab = f
	e.reg = f.Registry()
	e.mu.Unlock()
	e.MarkStarted()
	return nil
}

// Stop stops the extension.
func (e *Extension) Stop(_ context.Context) error { e.MarkStopped(); return nil }

// resolveFabric returns the query.Fabric, or an error if Start has not been called.
func (e *Extension) resolveFabric() (query.Fabric, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fabric == nil {
		return nil, fmt.Errorf("fabriq-admin-api: not started")
	}
	return e.fabric, nil
}

// resolveFabriq returns the concrete *fabriq.Fabriq facade, or an error if
// Start has not been called. The file-plane endpoints need the concrete type
// because the fs_node tree and blob byte-plane methods are not part of the
// narrower query.Fabric interface.
func (e *Extension) resolveFabriq() (*fabriq.Fabriq, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fab == nil {
		return nil, fmt.Errorf("fabriq-admin-api: not started")
	}
	return e.fab, nil
}

// resolveRegistry returns the schema registry, or an error if Start has not
// been called (and no registry was injected for tests). The registry powers
// the dynamic-entity types and schema introspection endpoints.
func (e *Extension) resolveRegistry() (*registry.Registry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.reg == nil {
		return nil, fmt.Errorf("fabriq-admin-api: registry not available (not started)")
	}
	return e.reg, nil
}

var _ forge.Extension = (*Extension)(nil)

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
	"log"
	"sync"

	"github.com/xraph/forge"
	"github.com/xraph/grove"
	"golang.org/x/crypto/bcrypt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/blob"
	corecache "github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/projection"
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
	// WritePolicy is the agent-toolkit write allowlist for the guarded-write
	// endpoint (POST {BasePath}/agent/remember). It is deny-by-default: an empty
	// policy permits no writes, so Remember stays safe unless the host opts
	// specific entity/op pairs in via WithWritePolicy.
	WritePolicy agent.WritePolicy
	// SchemaAdmin enables the privileged schema-ops endpoints (static migration
	// execution and ad-hoc DDL). Default false: those endpoints 403 until the
	// host opts in via WithSchemaAdmin. Read-only status/drift stay available.
	SchemaAdmin bool
	// TenantsAdmin enables the tenant-management endpoints (list/get plus the
	// suspend/resume lifecycle) — the HTTP twin of the `fabriq tenant` CLI.
	// Default false: those endpoints 403 until the host opts in via
	// WithTenantsAdmin. Even when enabled, the endpoints only function in
	// catalog mode (db-per-tenant) — see requireTenantsAdmin.
	TenantsAdmin bool
	// ConnectionsRead enables the connection/topology info endpoints (GET
	// /connections and GET /tenants/:id/connection). Default false: those
	// endpoints 403 until the host opts in via WithConnectionsRead. It is a
	// read-only, least-privilege gate distinct from the mutating TenantsAdmin
	// — a topology viewer needs it without gaining provision/suspend rights.
	// Even redacted, connection info reveals hosts/ports/usernames, so it
	// stays behind an explicit opt-in.
	ConnectionsRead bool
	// KeyStore is the instance-global API key store. When non-nil, per-tenant
	// API-key auth is enabled: the /admin/keys issue/list/revoke routes are
	// registered (see registerKeyRoutes). Set it via WithAuth. Note: this option
	// alone does NOT install the verifying middleware — the host must still
	// attach authMiddleware(store, basePath) via WithRouteOptions to actually
	// gate requests; WithAuth only wires the key-management surface.
	KeyStore KeyStore
	// AdminLoginUser and AdminLoginHash back the dashboard-login surface
	// (POST {BasePath}/login + /logout), set together via WithAdminLogin.
	// AdminLoginUser == "" (the default) means login is disabled and
	// registerLoginRoutes is never called. AdminLoginHash is the bcrypt hash
	// of the configured password — the plaintext is never retained.
	AdminLoginUser string
	AdminLoginHash string
	// authDisabled turns OFF secure-by-default auth provisioning. Set via
	// WithAuthDisabled. See resolveAuthDefaults for the full decision table.
	authDisabled bool
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

// WithWritePolicy sets the agent-toolkit write allowlist backing the guarded
// write endpoint (POST {BasePath}/agent/remember). Deny-by-default: without this
// option Remember permits no writes. Example: allow product creates/updates via
// agent.WritePolicy{Allow: map[string][]command.Op{"product": {command.OpCreate,
// command.OpUpdate}}}.
func WithWritePolicy(p agent.WritePolicy) Option {
	return func(c *config) { c.WritePolicy = p }
}

// WithSchemaAdmin enables the privileged schema-ops endpoints — running/rolling
// back static migrations and executing ad-hoc DDL. These are instance-global,
// schema-owner operations; leave OFF unless the host guards the admin API.
func WithSchemaAdmin() Option { return func(c *config) { c.SchemaAdmin = true } }

// WithTenantsAdmin enables the tenant-management endpoints — list/get plus the
// suspend/resume lifecycle (the HTTP twin of the `fabriq tenant` CLI). These
// are instance-global, catalog-mode-only operations; leave OFF unless the host
// guards the admin API and runs db-per-tenant.
func WithTenantsAdmin() Option { return func(c *config) { c.TenantsAdmin = true } }

// WithConnectionsRead enables the connection/topology info endpoints — GET
// /connections (the tier's stores + cluster topology) and GET
// /tenants/:id/connection (a tenant's dedicated database). All connection info
// is REDACTED server-side: passwords and other secrets are never serialized.
// It is a read-only gate, deliberately separate from the mutating
// WithTenantsAdmin so a topology viewer can be granted without provisioning
// rights; leave OFF unless the host guards the admin API.
func WithConnectionsRead() Option { return func(c *config) { c.ConnectionsRead = true } }

// WithAuth sets the instance-global API key store, enabling the /admin/keys
// issue/list/revoke management routes (registered only when the store is
// non-nil). It does NOT install the request-verifying middleware — that is a
// separate, explicit step: attach authMiddleware(store, basePath) via
// WithRouteOptions to actually gate admin requests on a valid key.
func WithAuth(store KeyStore) Option {
	return func(c *config) { c.KeyStore = store }
}

// WithAdminLogin enables the dashboard-login surface (POST {BasePath}/login +
// /logout): username is compared verbatim at login time and password is
// bcrypt-hashed immediately, at option-application time — the plaintext
// password is never retained. WithAdminLogin REQUIRES WithAuth: login mints a
// session token via KeyStore.IssueSession, so Extension.Start fails fast with
// an error if AdminLoginUser is set but no KeyStore was configured.
func WithAdminLogin(username, password string) Option {
	return func(c *config) {
		c.AdminLoginUser = username
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			// bcrypt.GenerateFromPassword only errors on an invalid cost or a
			// password longer than 72 bytes; DefaultCost is always valid, so
			// this is effectively unreachable for a well-formed password. Leave
			// AdminLoginHash empty so a malformed password can never match any
			// bcrypt comparison (fails closed, not open).
			c.AdminLoginHash = ""
			return
		}
		c.AdminLoginHash = string(hash)
	}
}

// WithAuthDisabled turns OFF secure-by-default auth: no KeyStore is
// auto-provisioned and the admin API is served without authentication. Use for
// trusted internal deployments or tests that intentionally run keyless.
func WithAuthDisabled() Option { return func(c *config) { c.authDisabled = true } }

// authDecision is the outcome of secure-by-default resolution. Start applies it.
type authDecision struct {
	ProvisionKeyStore bool   // build NewKeyStore(gdb) — only meaningful when a DB is available
	DefaultLogin      bool   // set the admin/admin login surface
	Warn              string // non-empty → log once at Start
}

// resolveAuthDefaults decides secure-by-default behavior WITHOUT side effects.
// hasDB reports whether a *grove.DB is reachable from the parent.
//   - authDisabled OR an explicit KeyStore → provision nothing.
//   - not disabled, no KeyStore, hasDB → provision; and default login when no
//     AdminLoginUser was configured (explicit creds are preserved).
//   - not disabled, no KeyStore, no DB → provision nothing + warn (auth wanted
//     but cannot be enforced without a DB; keeps nil-parent tests auth-off).
func resolveAuthDefaults(cfg config, hasDB bool) authDecision {
	if cfg.authDisabled || cfg.KeyStore != nil {
		return authDecision{}
	}
	if !hasDB {
		return authDecision{Warn: "auth is on by default but no Postgres DB is reachable — admin API is UNAUTHENTICATED; provide WithAuth or a Postgres-backed fabric, or set WithAuthDisabled to silence this"}
	}
	return authDecision{ProvisionKeyStore: true, DefaultLogin: cfg.AdminLoginUser == ""}
}

// Extension exposes the fabriq data fabric as a read-only admin HTTP surface.
// It depends on the "fabriq" extension and resolves its query facade at Start.
type Extension struct {
	forge.BaseExtension
	parent *forgeext.Extension
	cfg    config

	mu            sync.Mutex
	authInstalled bool                 // guards the one-time middleware prepend in Routes()
	fabric        query.Fabric         // resolved in Start
	fab           *fabriq.Fabriq       // concrete facade, resolved in Start (powers the file-plane endpoints)
	reg           *registry.Registry   // schema registry, resolved in Start (powers types/schema introspection)
	cas           blob.CAS             // content-addressed store, resolved in Start; nil when EnableCas is off (powers digest summaries)
	stateRepo     projection.StateRepo // projection bookkeeping, resolved in Start; nil when no Postgres store (powers the projections status endpoint)
	cache         corecache.Cache      // engine cache, resolved in Start; nil when Redis is not configured (powers the cache admin endpoints)
	stores        *fabriq.Stores       // opened adapters, resolved in Start; nil in fake-backed tests (powers projection reconcile/rebuild)
	dynWriter     dynamicSchemaWriter  // schema-write facade override for tests; nil means use fab (resolved in Start)
}

// dynamicSchemaWriter is the subset of *fabriq.Fabriq the schema-write handlers
// need. Declared as an interface so tests can inject a fake (no Postgres).
type dynamicSchemaWriter interface {
	DefineDynamic(ctx context.Context, spec registry.EntitySpec) error
	AlterDynamic(ctx context.Context, spec registry.EntitySpec) error
	RenameDynamicField(ctx context.Context, typeName, oldCol, newCol string) error
	DropDynamicField(ctx context.Context, typeName, col string) error
	DropDynamic(ctx context.Context, typeName string) error
}

// resolveDynamicWriter returns the schema-write facade — the injected fake in
// tests, otherwise the concrete *fabriq.Fabriq resolved at Start.
func (e *Extension) resolveDynamicWriter() (dynamicSchemaWriter, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.dynWriter != nil {
		return e.dynWriter, nil
	}
	if e.fab == nil {
		return nil, fmt.Errorf("fabriq-admin-api: not started")
	}
	return e.fab, nil
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
	// Secure-by-default auth MUST be resolved here, BEFORE RegisterController:
	// controller.Routes() installs the auth middleware and registers the /keys +
	// /login routes based on cfg (KeyStore/AdminLoginUser) at registration time,
	// and Routes() runs synchronously inside RegisterController. The parent fabriq
	// extension is a declared dependency, so it has Started (stores open) by the
	// time this runs — the DB is reachable via groveFromParent. (Tests build the
	// controller directly via newAdminController and never call Register, so they
	// stay auth-off.)
	if err := e.applyAuthDefaults(); err != nil {
		return err
	}
	return app.RegisterController(newAdminController(e))
}

// applyAuthDefaults resolves and applies secure-by-default auth: auto-provision a
// KeyStore from the fabric DB and default the admin/admin login, unless the host
// opted out (WithAuthDisabled) or already configured auth. Decided purely by
// resolveAuthDefaults; applied here so controller.Routes() (called next, in
// Register) observes the resulting config.
func (e *Extension) applyAuthDefaults() error {
	d := resolveAuthDefaults(e.cfg, groveFromParent(e) != nil)
	if d.Warn != "" {
		log.Printf("adminapi: %s", d.Warn)
	}
	if d.ProvisionKeyStore {
		e.cfg.KeyStore = NewKeyStore(groveFromParent(e))
	}
	if d.DefaultLogin {
		hash, herr := bcrypt.GenerateFromPassword([]byte("admin"), bcrypt.DefaultCost)
		if herr != nil {
			return fmt.Errorf("adminapi: hash default admin password: %w", herr)
		}
		e.cfg.AdminLoginUser = "admin"
		e.cfg.AdminLoginHash = string(hash)
		log.Printf("adminapi: auth on by default — default admin/admin login enabled; override with WithAdminLogin or ADMIN_LOGIN_USER/PASSWORD")
	}
	return nil
}

// groveFromParent returns the primary shard's *grove.DB, or nil when the parent
// / opened stores / Postgres adapter is absent (nil-parent tests, non-Postgres
// deployments). Never panics.
func groveFromParent(e *Extension) *grove.DB {
	if e.parent == nil {
		return nil
	}
	stores := e.parent.Stores()
	if stores == nil || stores.Postgres == nil {
		return nil
	}
	return stores.Postgres.Grove()
}

// Start resolves the fabriq facade from the started fabriq extension.
func (e *Extension) Start(ctx context.Context) error {
	// Auth defaulting (auto-provision KeyStore + default login) happens in
	// Register (before routes are built). This guard also covers the direct-Start
	// path used by tests: explicit WithAdminLogin without a resolvable KeyStore is
	// a hard error (login set but no DB and no WithAuth).
	if e.cfg.AdminLoginUser != "" && e.cfg.KeyStore == nil {
		return fmt.Errorf("adminapi: WithAdminLogin requires WithAuth")
	}

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
	// Resolve the content-addressed store from the parent's opened adapters. It
	// backs the digest-summary text in the distillation read endpoints; nil when
	// the host did not enable the CAS (Storage.EnableCas false), in which case
	// the distill endpoints degrade to empty summaries (hashes only).
	if stores := e.parent.Stores(); stores != nil {
		if stores.CAS != nil {
			e.cas = stores.CAS
		}
		// The primary shard's projection bookkeeping backs the read-only
		// projections status endpoint. nil when Postgres is absent.
		if stores.Postgres != nil {
			e.stateRepo = stores.Postgres.ProjectionState()
		}
		// The engine cache backs the cache admin endpoints. nil when Redis (and
		// therefore the cache) is not configured.
		e.cache = stores.Cache
		// The opened adapters back the projection reconcile/rebuild controls
		// (Graph/Search Reconciler/Rebuilder constructors live on Stores).
		e.stores = stores
	}
	e.mu.Unlock()

	// When auth is enabled, ensure a usable can-manage-keys admin key exists so
	// an operator is never locked out of a fresh install (env-supplied via
	// FABRIQ_ADMIN_KEY, otherwise one generated + logged once). Idempotent.
	if e.cfg.KeyStore != nil {
		if err := bootstrapAdminKey(ctx, e.cfg.KeyStore); err != nil {
			return fmt.Errorf("fabriq-admin-api: bootstrap admin key: %w", err)
		}
	}

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

// resolveCAS returns the content-addressed store, or nil when the host did not
// enable the CAS. It is never an error: the distillation read endpoints treat a
// nil CAS as graceful degradation (digest summaries come back empty, hashes are
// still served), matching agent.Toolkit's nil-CAS behaviour.
func (e *Extension) resolveCAS() blob.CAS {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cas
}

// resolveStateRepo returns the projection bookkeeping repo, or nil when the host
// has no Postgres store (e.g. a fake-backed test). The projections endpoint
// treats nil as 501 (not available).
func (e *Extension) resolveStateRepo() projection.StateRepo {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stateRepo
}

// resolveCache returns the engine cache, or nil when Redis (and therefore the
// cache) is not configured. The cache admin endpoints treat nil as 501.
func (e *Extension) resolveCache() corecache.Cache {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cache
}

// resolveStores returns the opened adapters, or nil when unavailable (fake test
// harness). The projection reconcile/rebuild endpoints treat nil as 501.
func (e *Extension) resolveStores() *fabriq.Stores {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stores
}

var _ forge.Extension = (*Extension)(nil)

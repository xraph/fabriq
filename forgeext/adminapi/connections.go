package adminapi

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

// maskedSecret is the ONLY value ever placed in a connection view's password
// field. Real passwords are dropped server-side (see the product-owner
// redaction requirement); this constant merely signals that a secret is
// configured without disclosing it.
const maskedSecret = "••••" //nolint:gosec // the OPPOSITE of a credential: a fixed placeholder that REPLACES redacted secrets in responses

// connHealthTimeout bounds every reachability probe so a dead store cannot
// wedge a connection-info request.
const connHealthTimeout = 3 * time.Second

// defaultMaxActiveShards mirrors CatalogConfig's zero-value fallback (open.go)
// so the reported pool cap matches the pool's real ceiling.
const defaultMaxActiveShards = 128

// healthView is a bounded reachability probe result for one connection.
type healthView struct {
	Reachable bool   `json:"reachable"`
	LatencyMs int64  `json:"latencyMs"`
	Error     string `json:"error,omitempty"`
}

// connectionView is one REDACTED store/cluster connection. It carries only
// non-secret fields; a real password is never serialized — Password, when
// present, is the masked constant only.
type connectionView struct {
	Name      string      `json:"name"`
	Kind      string      `json:"kind"` // postgres | redis | falkordb | elasticsearch | blob
	ClusterID string      `json:"clusterId,omitempty"`
	Host      string      `json:"host,omitempty"`
	Port      string      `json:"port,omitempty"`
	Addrs     []string    `json:"addrs,omitempty"`
	Database  string      `json:"database,omitempty"`
	Username  string      `json:"username,omitempty"`
	Password  string      `json:"password,omitempty"` // masked constant ONLY, never a real secret
	SSLMode   string      `json:"sslMode,omitempty"`
	Driver    string      `json:"driver,omitempty"` // blob storage driver
	Bucket    string      `json:"bucket,omitempty"` // blob default bucket
	Health    *healthView `json:"health,omitempty"`
}

// poolView reports the catalog-mode shard pool occupancy against its cap.
type poolView struct {
	Open int `json:"open"`
	Held int `json:"held"`
	Cap  int `json:"cap"`
}

// connectionsResponse is the payload for GET {BasePath}/connections.
type connectionsResponse struct {
	Mode        string           `json:"mode"` // catalog | shards | single
	Pool        *poolView        `json:"pool,omitempty"`
	Connections []connectionView `json:"connections"`
}

// tenantConnectionView is the payload for GET {BasePath}/tenants/:id/connection
// — the catalog entry plus its dedicated database's redacted connection.
type tenantConnectionView struct {
	TenantID   string         `json:"tenantId"`
	ClusterID  string         `json:"clusterId"`
	Database   string         `json:"database"`
	State      string         `json:"state"`
	Version    string         `json:"version"`
	Connection connectionView `json:"connection"`
}

// pgConnParts is the redacted decomposition of a postgres:// DSN. It NEVER
// carries the password — only HasPassword records that one was present.
type pgConnParts struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	Database    string `json:"database"`
	Username    string `json:"username"`
	SSLMode     string `json:"sslMode"`
	HasPassword bool   `json:"hasPassword"`
}

// parsePgDSN decomposes a postgres:// DSN into its non-secret fields, dropping
// the password. It is the single redaction chokepoint for Postgres connection
// info: callers build views from the returned parts, never from the raw DSN.
func parsePgDSN(dsn string) (pgConnParts, error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		return pgConnParts{}, fabriqerr.New(fabriqerr.CodeInvalidInput, "invalid postgres DSN.")
	}
	p := pgConnParts{
		Host:     u.Hostname(),
		Port:     u.Port(),
		Database: pathDatabase(u.Path),
		SSLMode:  u.Query().Get("sslmode"),
	}
	if u.User != nil {
		p.Username = u.User.Username()
		_, p.HasPassword = u.User.Password()
	}
	return p, nil
}

// pathDatabase strips the leading slash of a DSN path to yield the database name.
func pathDatabase(path string) string {
	if path != "" && path[0] == '/' {
		return path[1:]
	}
	return path
}

// pgConnectionView builds a redacted connection view from a Postgres DSN.
func pgConnectionView(name, clusterID, dsn string) connectionView {
	v := connectionView{Name: name, Kind: "postgres", ClusterID: clusterID}
	parts, err := parsePgDSN(dsn)
	if err != nil {
		return v // an unparseable DSN yields host-less info rather than leaking it
	}
	v.Host, v.Port, v.Database, v.Username, v.SSLMode = parts.Host, parts.Port, parts.Database, parts.Username, parts.SSLMode
	if parts.HasPassword {
		v.Password = maskedSecret
	}
	return v
}

// hostPort splits an "host:port" address, tolerating a bare host.
func hostPort(addr string) (host, port string) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	return h, p
}

// redactURL strips any embedded userinfo (credentials) from a URL-shaped
// address, leaving scheme://host:port/path. A non-URL string is returned
// unchanged (it cannot carry userinfo).
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return raw
	}
	u.User = nil
	return u.String()
}

// buildConnections projects a fabriq config into REDACTED connection views. It
// is pure and performs no I/O: health is attached separately by the handler.
// Every secret-bearing field is dropped here — this function is the serialized
// response's redaction boundary.
func buildConnections(cfg fabriq.Config) []connectionView {
	out := []connectionView{}

	// Postgres topology.
	if cfg.Catalog.Enabled() {
		out = append(out, pgConnectionView("catalog-control", "", cfg.Catalog.DSN))
		ids := make([]string, 0, len(cfg.Catalog.ClusterDSNs))
		for id := range cfg.Catalog.ClusterDSNs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			out = append(out, pgConnectionView("cluster:"+id, id, cfg.Catalog.ClusterDSNs[id]))
		}
	} else {
		if cfg.Postgres.DSN != "" {
			out = append(out, pgConnectionView("postgres", "", cfg.Postgres.DSN))
		}
		for _, s := range cfg.Shards {
			out = append(out, pgConnectionView("shard:"+s.ID, s.ID, s.DSN))
		}
	}

	// Redis.
	if cfg.Redis.Addr != "" {
		host, port := hostPort(cfg.Redis.Addr)
		v := connectionView{
			Name: "redis", Kind: "redis", Host: host, Port: port,
			Database: strconv.Itoa(cfg.Redis.DB), Username: cfg.Redis.Username,
		}
		if cfg.Redis.Password != "" {
			v.Password = maskedSecret
		}
		out = append(out, v)
	}

	// FalkorDB (graph projection engine).
	if cfg.FalkorDB.Addr != "" {
		host, port := hostPort(cfg.FalkorDB.Addr)
		v := connectionView{
			Name: "falkordb", Kind: "falkordb", Host: host, Port: port,
			Username: cfg.FalkorDB.Username,
		}
		if cfg.FalkorDB.Password != "" {
			v.Password = maskedSecret
		}
		out = append(out, v)
	}

	// Elasticsearch (search projection engine).
	if len(cfg.Elasticsearch.Addrs) > 0 {
		addrs := make([]string, 0, len(cfg.Elasticsearch.Addrs))
		for _, a := range cfg.Elasticsearch.Addrs {
			addrs = append(addrs, redactURL(a))
		}
		v := connectionView{
			Name: "elasticsearch", Kind: "elasticsearch", Addrs: addrs,
			Username: cfg.Elasticsearch.Username,
		}
		if cfg.Elasticsearch.Password != "" {
			v.Password = maskedSecret
		}
		out = append(out, v)
	}

	// Blob (object store), configured only when a driver is set.
	if cfg.Storage.StorageDriver != "" {
		out = append(out, connectionView{
			Name: "blob", Kind: "blob",
			Driver: cfg.Storage.StorageDriver, Bucket: cfg.Storage.DefaultBucket,
		})
	}

	return out
}

// modeOf reports the routing mode label for the connections response.
func modeOf(cfg fabriq.Config) string {
	switch {
	case cfg.Catalog.Enabled():
		return "catalog"
	case len(cfg.Shards) > 0:
		return "shards"
	default:
		return "single"
	}
}

// requireConnectionsRead gates the connection-info endpoints. It checks the
// connections.read capability opt-in first (403 when off), then resolves the
// started parent's data-fabric config (400 when parent is absent — e.g. the
// fake-backed unit harness, which drives a nil-parent Extension).
//
// Like requireTenantsAdmin it returns a real forge.IHTTPError so callers'
// early return actually short-circuits before dereferencing the config.
func (c *adminController) requireConnectionsRead(ctx forge.Context) (fabriq.Config, error) {
	if err := c.requireCap(ctx, "connections.read"); err != nil {
		return fabriq.Config{}, err
	}
	if c.ext.parent == nil {
		return fabriq.Config{}, forge.BadRequest("connection info requires a started fabriq extension")
	}
	return c.ext.parent.Config(), nil
}

// registerConnectionRoutes wires the connection/topology info surface, gated
// on the connections.read capability. The tenant-scoped route is registered
// after registerTenantRoutes so the static /tenants/... routes are already in
// place and cannot be shadowed.
func (c *adminController) registerConnectionRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	opts := c.ext.cfg.RouteOptions

	connOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.connections.list"),
		forge.WithSummary("List connected stores and cluster topology (redacted)"),
		forge.WithTags("Fabriq", "Admin", "Connections"),
	}, opts...)
	if err := r.GET(base+"/connections", c.handleConnections, connOpts...); err != nil {
		return err
	}

	tenantConnOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.tenants.connection"),
		forge.WithSummary("Get a tenant's dedicated database connection (redacted; catalog mode)"),
		forge.WithTags("Fabriq", "Admin", "Connections"),
	}, opts...)
	return r.GET(base+"/tenants/:id/connection", c.handleTenantConnection, tenantConnOpts...)
}

// handleConnections serves GET {BasePath}/connections — the redacted tier
// topology (Postgres clusters/shards + Redis/FalkorDB/Elasticsearch/blob),
// pool occupancy, and a bounded health probe per connection.
func (c *adminController) handleConnections(ctx forge.Context) error {
	cfg, err := c.requireConnectionsRead(ctx)
	if err != nil {
		return err
	}

	views := buildConnections(cfg)
	stores := c.ext.resolveStores()

	// Pool occupancy is tier-wide (catalog mode only).
	var pool *poolView
	if stores != nil {
		if open, held, ok := stores.PoolStats(); ok {
			poolCap := cfg.Catalog.MaxActiveShards
			if poolCap <= 0 {
				poolCap = defaultMaxActiveShards
			}
			pool = &poolView{Open: open, Held: held, Cap: poolCap}
		}
	}

	c.probeConnections(ctx.Request().Context(), views, stores, cfg)

	return ctx.JSON(http.StatusOK, connectionsResponse{
		Mode:        modeOf(cfg),
		Pool:        pool,
		Connections: views,
	})
}

// handleTenantConnection serves GET {BasePath}/tenants/:id/connection — the
// dedicated database for one tenant, derived from the catalog entry and the
// cluster ops (the same TenantDSN the router dials), redacted + health-probed.
func (c *adminController) handleTenantConnection(ctx forge.Context) error {
	cfg, err := c.requireConnectionsRead(ctx)
	if err != nil {
		return err
	}
	id := ctx.Param("id")
	if !tenant.Valid(id) {
		return forge.BadRequest("invalid tenant id")
	}
	stores := c.ext.resolveStores()
	if stores == nil || stores.Catalog == nil {
		return forge.BadRequest("tenant connection info requires catalog mode (db-per-tenant)")
	}

	reqCtx := ctx.Request().Context()
	e, gerr := stores.Catalog.Get(reqCtx, id)
	if fabriqerr.CodeOf(gerr) == fabriqerr.CodeNotFound {
		return forge.NotFound("no such tenant")
	}
	if gerr != nil {
		// renderError is the leak boundary: any error that is not a recognized
		// *fabriqerr.Error collapses to a generic 500 rather than echoing a raw
		// (possibly DSN-bearing) driver string, unlike forge.InternalError.
		return renderError(ctx, gerr)
	}

	ops := postgres.NewClusterOps(cfg.Catalog.ClusterDSNs)
	dsn, derr := ops.TenantDSN(e.ClusterID, e.Database)
	if derr != nil {
		return renderError(ctx, derr)
	}
	conn := pgConnectionView("tenant:"+id, e.ClusterID, dsn)
	conn.Health = probePostgresDSN(reqCtx, dsn)

	return ctx.JSON(http.StatusOK, tenantConnectionView{
		TenantID:   e.TenantID,
		ClusterID:  e.ClusterID,
		Database:   e.Database,
		State:      string(e.State),
		Version:    e.Version,
		Connection: conn,
	})
}

// probeConnections attaches a bounded health probe to each view in place. It
// uses the already-open adapters where possible (cheap ping) and a bounded
// dial for per-cluster/topology entries that have no persistent adapter.
func (c *adminController) probeConnections(ctx context.Context, views []connectionView, stores *fabriq.Stores, cfg fabriq.Config) {
	for i := range views {
		v := &views[i]
		switch v.Kind {
		case "postgres":
			// The primary adapter answers for "postgres"/"catalog-control"; every
			// cluster/shard is a bounded dial of its own DSN.
			if dsn := pgDSNFor(v, cfg); dsn != "" {
				v.Health = probePostgresDSN(ctx, dsn)
			} else if stores != nil && stores.Postgres != nil {
				v.Health = timedPing(ctx, stores.Postgres.Ping)
			}
		case "redis":
			if stores != nil && stores.Redis != nil {
				v.Health = timedPing(ctx, stores.Redis.Ping)
			}
		case "falkordb":
			if stores != nil && stores.Falkor != nil {
				v.Health = timedPing(ctx, stores.Falkor.Ping)
			}
		case "elasticsearch":
			if stores != nil && stores.Elastic != nil {
				v.Health = timedPing(ctx, stores.Elastic.Ping)
			}
		case "blob":
			if stores != nil && stores.Blob != nil {
				v.Health = probeBlob(ctx, stores.Blob)
			}
		}
	}
}

// pgDSNFor returns the raw DSN backing a Postgres connection view so it can be
// bounded-dialed, or "" for the control/primary entry (which has a live
// adapter to ping instead).
func pgDSNFor(v *connectionView, cfg fabriq.Config) string {
	switch {
	case v.Name == "catalog-control" && cfg.Catalog.Enabled():
		return cfg.Catalog.DSN
	case v.ClusterID != "" && cfg.Catalog.Enabled():
		return cfg.Catalog.ClusterDSNs[v.ClusterID]
	case v.ClusterID != "":
		for _, s := range cfg.Shards {
			if s.ID == v.ClusterID {
				return s.DSN
			}
		}
	}
	return ""
}

// dsnUserinfo matches the "scheme://user:password@" prefix of any URL-shaped
// string so the password can be masked out of an error message.
var dsnUserinfo = regexp.MustCompile(`(://[^:/@\s]+):[^@/\s]+@`)

// scrubSecrets masks the password inside any DSN/URL userinfo embedded in a
// message. Store dial errors can carry the raw connection string (with the
// password); this is the defense-in-depth redaction applied before any error
// text reaches a response body.
func scrubSecrets(msg string) string {
	return dsnUserinfo.ReplaceAllString(msg, "$1:"+maskedSecret+"@")
}

// timedPing runs a ping under connHealthTimeout, recording latency + result.
// Any error text is scrubbed of embedded credentials before it is stored.
func timedPing(ctx context.Context, ping func(context.Context) error) *healthView {
	cctx, cancel := context.WithTimeout(ctx, connHealthTimeout)
	defer cancel()
	start := time.Now()
	err := ping(cctx)
	h := &healthView{LatencyMs: time.Since(start).Milliseconds()}
	if err != nil {
		h.Error = scrubSecrets(err.Error())
		return h
	}
	h.Reachable = true
	return h
}

// probePostgresDSN bounded-dials a Postgres DSN and runs SELECT 1.
func probePostgresDSN(ctx context.Context, dsn string) *healthView {
	return timedPing(ctx, func(cctx context.Context) error {
		return postgres.PingDSN(cctx, dsn)
	})
}

// probeBlob probes blob reachability via Head on a sentinel key: a not-found
// answer still proves the backend is reachable (it responded), so only a
// transport error counts as unreachable.
func probeBlob(ctx context.Context, store blobHeader) *healthView {
	return timedPing(ctx, func(cctx context.Context) error {
		_, err := store.Head(cctx, "__fabriq_health_probe__")
		if err == nil || fabriqerr.CodeOf(err) == fabriqerr.CodeNotFound {
			return nil
		}
		return err
	})
}

// blobHeader is the minimal Head-only view of the blob store the probe needs.
type blobHeader interface {
	Head(ctx context.Context, key string) (blob.ObjectInfo, error)
}

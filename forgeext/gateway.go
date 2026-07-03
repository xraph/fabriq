package forgeext

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/livequery/cluster"
	"github.com/xraph/fabriq/gateway"
)

// GatewayConfig configures the standalone live-query gateway tier: the edge that
// terminates client SSE/WebSocket connections and routes them over the sharded
// control/delta protocol to the matcher shards.
type GatewayConfig struct {
	// GatewayID is this gateway's cluster identity (deltas are routed back to it
	// over lq:delta:{id}). Defaults to "gw-<host>-<pid>".
	GatewayID string
	// BasePath is the SSE endpoint path; the WebSocket endpoint is BasePath+"/ws".
	// Defaults to "/api/v1/live".
	BasePath string
	// HeartbeatTTL is the membership liveness window of the Redis cluster
	// transport (default 6s). Shards must refresh within it.
	HeartbeatTTL time.Duration
	// SSE and WS tune the per-connection delivery loops (heartbeat, write
	// deadlines/watchdog).
	SSE gateway.SSEOptions
	WS  gateway.WSOptions
	// RouteOptions are forwarded verbatim to router.SSE/router.WebSocket, so the
	// host app attaches its own auth middleware and AsyncAPI/OpenAPI schemas
	// (e.g. pkgAuth.RequirePermission(...), forge.WithSSEMessage(...)). fabriq
	// stays auth-scheme-agnostic.
	RouteOptions []forge.RouteOption
	// EnableDocEndpoints mounts the CRDT document endpoints (docs/update,
	// docs/sync, docs/subscribe, docs/presence) under BasePath. OPT-IN:
	// update is a write surface, so a live-query-only gateway must not
	// grow it silently on upgrade.
	EnableDocEndpoints bool
}

// GatewayOption is a functional option for GatewayConfig.
type GatewayOption func(*GatewayConfig)

// WithGatewayID sets the gateway's cluster identity.
func WithGatewayID(id string) GatewayOption { return func(c *GatewayConfig) { c.GatewayID = id } }

// WithGatewayBasePath sets the SSE endpoint path (WS is BasePath+"/ws").
func WithGatewayBasePath(p string) GatewayOption { return func(c *GatewayConfig) { c.BasePath = p } }

// WithGatewayHeartbeatTTL sets the cluster membership liveness window.
func WithGatewayHeartbeatTTL(d time.Duration) GatewayOption {
	return func(c *GatewayConfig) { c.HeartbeatTTL = d }
}

// WithGatewayWriteTimeout bounds a single SSE/WS write before the connection is
// torn down (the client reconnects to a fresh snapshot).
func WithGatewayWriteTimeout(d time.Duration) GatewayOption {
	return func(c *GatewayConfig) { c.SSE.WriteTimeout = d; c.WS.WriteTimeout = d }
}

// WithGatewaySSEHeartbeat sets the SSE keep-alive interval.
func WithGatewaySSEHeartbeat(d time.Duration) GatewayOption {
	return func(c *GatewayConfig) { c.SSE.HeartbeatInterval = d }
}

// WithGatewayRouteOptions appends route options forwarded to the SSE/WS routes
// (auth, AsyncAPI/OpenAPI documentation).
func WithGatewayRouteOptions(opts ...forge.RouteOption) GatewayOption {
	return func(c *GatewayConfig) { c.RouteOptions = append(c.RouteOptions, opts...) }
}

// WithGatewayDocEndpoints mounts the CRDT document sync + presence
// endpoints (opt-in: docs/update is a write surface — pair it with
// fabriq.WithDocumentAuthz and auth middleware via route options).
func WithGatewayDocEndpoints() GatewayOption {
	return func(c *GatewayConfig) { c.EnableDocEndpoints = true }
}

// clusterBackend adapts the in-process cluster.Gateway (which returns a loose
// (subID, channel, cancel, err) tuple) into the gateway.Backend seam, capturing
// subID and the query in the reanchor closure.
type clusterBackend struct{ gw *cluster.Gateway }

func (b clusterBackend) Subscribe(ctx context.Context, q livequery.LiveQuery) (*gateway.Sub, error) {
	id, deltas, cancel, err := b.gw.Subscribe(ctx, q)
	if err != nil {
		return nil, err
	}
	reanchor := func(rctx context.Context, cur *livequery.Cursor, limit int) error {
		return b.gw.Reanchor(rctx, id, q, cur, limit)
	}
	return gateway.NewSub(id, deltas, reanchor, cancel), nil
}

// GatewayExtension exposes the fabriq live-query gateway as a Forge extension:
// it builds a cluster.Gateway over the fabriq facade's Redis transport, runs its
// demux pump, and registers the SSE + WebSocket controllers. It depends on the
// "fabriq" extension (it reads its Stores().Redis), so it starts after it.
type GatewayExtension struct {
	forge.BaseExtension
	fab *Extension
	cfg GatewayConfig

	mu      sync.Mutex
	gw      *cluster.Gateway
	backend gateway.Backend
	cancel  context.CancelFunc
	done    chan struct{}
}

// GatewayExtension runs its demux pump from Start to Stop, so it satisfies the
// base forge.Extension lifecycle (it does not need RunnableExtension's
// PhaseAfterRun Run/Shutdown hooks).
var _ forge.Extension = (*GatewayExtension)(nil)

// NewGateway builds the gateway extension wired to a started fabriq Extension.
func NewGateway(fab *Extension, opts ...GatewayOption) *GatewayExtension {
	cfg := GatewayConfig{BasePath: "/api/v1/live", HeartbeatTTL: 6 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.BasePath == "" {
		cfg.BasePath = "/api/v1/live"
	}
	if cfg.GatewayID == "" {
		host, _ := os.Hostname()
		cfg.GatewayID = fmt.Sprintf("gw-%s-%d", host, os.Getpid())
	}
	return &GatewayExtension{fab: fab, cfg: cfg}
}

func (g *GatewayExtension) Name() string    { return "fabriq-gateway" }
func (g *GatewayExtension) Version() string { return Version }
func (g *GatewayExtension) Description() string {
	return "fabriq live-query edge tier: SSE + WebSocket termination over the sharded delta protocol"
}
func (g *GatewayExtension) Dependencies() []string { return []string{"fabriq"} }

// Register registers the SSE and WebSocket controllers. Their handlers resolve
// the backend lazily (it is built in Start), the same lazy-DI pattern the fabriq
// facade uses — safe because requests only arrive after Start.
func (g *GatewayExtension) Register(app forge.App) error {
	if err := g.BaseExtension.Register(app); err != nil {
		return err
	}
	if err := app.RegisterController(newLiveSSEController(g)); err != nil {
		return err
	}
	if err := app.RegisterController(newLiveWSController(g)); err != nil {
		return err
	}
	if g.cfg.EnableDocEndpoints {
		return app.RegisterController(newDocsController(g))
	}
	return nil
}

// Start builds the cluster.Gateway over the facade's Redis transport and runs
// its demux pump.
func (g *GatewayExtension) Start(_ context.Context) error {
	st := g.fab.Stores()
	if st == nil || st.Redis == nil {
		return fmt.Errorf("fabriq-gateway: requires the fabriq facade with Redis configured (set redis.addr / FABRIQ_REDIS_ADDR)")
	}
	tr := st.Redis.Cluster(g.cfg.HeartbeatTTL)
	gw := cluster.NewGateway(g.cfg.GatewayID, cluster.GatewayDeps{Members: tr, Control: tr, Delta: tr})

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	g.mu.Lock()
	g.gw, g.backend, g.cancel, g.done = gw, clusterBackend{gw: gw}, cancel, done
	g.mu.Unlock()

	go func() {
		defer close(done)
		_ = gw.Run(runCtx)
	}()
	g.MarkStarted()
	return nil
}

// Stop stops the gateway demux pump.
func (g *GatewayExtension) Stop(ctx context.Context) error {
	g.mu.Lock()
	cancel, done := g.cancel, g.done
	g.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	g.MarkStopped()
	return nil
}

// resolveBackend returns the live backend, or an error before Start.
func (g *GatewayExtension) resolveBackend() (gateway.Backend, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.backend == nil {
		return nil, fmt.Errorf("fabriq-gateway: not started")
	}
	return g.backend, nil
}

package forgeext

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/telemetry"
	"github.com/xraph/fabriq/internal/metrics"
)

// initMetrics builds fabriq's private Prometheus registry and instruments,
// once. Everything on it keeps its fabriq_ name (forge's /_/metrics prefixes
// the series it owns with forge_). The scrape-time series read the facade
// and stores through the extension, so they report zero until Start has run.
func (e *Extension) initMetrics() *metrics.Metrics {
	e.metricsOnce.Do(func() {
		reg := prometheus.NewRegistry()
		m, err := metrics.New(reg,
			metrics.WithCatalogReads(func() (primary, replica, failover int64, ok bool) {
				st := e.Stores()
				if st == nil {
					return 0, 0, 0, false
				}
				return st.CatalogReadStats()
			}),
			metrics.WithConflationDepth(func() int {
				f := e.Fabriq()
				if f == nil || f.Hub() == nil {
					return 0
				}
				return f.Hub().ConflationDepth()
			}),
		)
		if err != nil {
			return // a fresh registry cannot collide; metrics stay off
		}
		e.mu.Lock()
		e.promReg, e.metrics = reg, m
		e.mu.Unlock()
	})
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.metrics
}

// mountMetrics serves fabriq's registry at GET /metrics on the app's router,
// once. It runs from Start, so every node that opens the facade serves it
// (command latency and conflation depth are request-path series), not only
// the worker. A route conflict is logged and leaves the series unserved
// rather than failing Start.
func (e *Extension) mountMetrics() {
	app := e.App()
	e.mu.Lock()
	reg, mounted := e.promReg, e.metricsMounted
	if app == nil || reg == nil || mounted {
		e.mu.Unlock()
		return
	}
	e.metricsMounted = true
	e.mu.Unlock()

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	if err := app.Router().GET("/metrics", func(ctx forge.Context) error {
		handler.ServeHTTP(ctx.Response(), ctx.Request())
		return nil
	}); err != nil {
		if log := e.Logger(); log != nil {
			log.Warn("fabriq: could not mount GET /metrics; fabriq's series are recorded but not served",
				forge.String("reason", "route registration failed"))
		}
	}
}

// pollGauges keeps the slow-moving gauges fresh. It polls on a fixed cadence
// and records no spans: its ctx is detached from any trace, so its queries
// stay out of every trace too.
func pollGauges(ctx context.Context, stores *fabriq.Stores, m *metrics.Metrics, every time.Duration) {
	ctx = telemetry.Detach(ctx)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	var lastTrips int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Outbox backlog and backstop trips sum across every shard.
			var backlog, trips int64
			for _, sp := range stores.ShardPGs() {
				row := sp.PG.Driver().QueryRow(ctx,
					`SELECT count(*) FROM fabriq_outbox WHERE published_at IS NULL`)
				var n int64
				if err := row.Scan(&n); err == nil {
					backlog += n
				}
				trips += sp.PG.BackstopTrips()
			}
			m.OutboxBacklog.Set(float64(backlog))
			if trips > lastTrips {
				m.TenantHookTrips.Add(float64(trips - lastTrips))
				lastTrips = trips
			}
			if stores.Redis != nil {
				for _, proj := range []string{"graph", "search"} {
					if lag, err := stores.Redis.GroupLag(ctx, "proj:"+proj); err == nil {
						m.ProjectionLag.WithLabelValues(proj).Set(float64(lag))
					}
				}
			}
		}
	}
}

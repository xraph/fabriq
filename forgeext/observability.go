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
			metrics.WithBackstopTrips(func() int64 {
				st := e.Stores()
				if st == nil {
					return 0
				}
				return st.BackstopTrips()
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
// once. Run calls it first thing, before its RunWorker check: every node
// serves it (command latency, conflation depth and backstop trips are
// request-path series), and it is mounted after every extension's Start, the
// same point in the lifecycle the worker always mounted it. A route conflict
// is logged and leaves the series unserved rather than failing Run.
func (e *Extension) mountMetrics() {
	e.initMetrics()
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

// gaugeReads are the store reads pollGauges makes on each tick.
type gaugeReads struct {
	// backlog sums unpublished outbox rows across shards; ok is false when
	// no shard could be read.
	backlog func(ctx context.Context) (n int64, ok bool)
	// groupLag reads a consumer group's lag plus pending; nil without Redis.
	groupLag func(ctx context.Context, group string) (int64, error)
}

// storeGaugeReads reads the gauges from the opened stores.
func storeGaugeReads(stores *fabriq.Stores) gaugeReads {
	r := gaugeReads{backlog: func(ctx context.Context) (int64, bool) {
		var backlog int64
		ok := false
		for _, sp := range stores.ShardPGs() {
			row := sp.PG.Driver().QueryRow(ctx,
				`SELECT count(*) FROM fabriq_outbox WHERE published_at IS NULL`)
			var n int64
			if err := row.Scan(&n); err == nil {
				backlog += n
				ok = true
			}
		}
		return backlog, ok
	}}
	if stores.Redis != nil {
		r.groupLag = stores.Redis.GroupLag
	}
	return r
}

// pollGauges keeps the worker's slow-moving gauges fresh: the outbox backlog
// and the graph and search consumer lag. Only the worker runs it, so on a
// node without the worker those two series read zero. It polls on a fixed
// cadence and records no spans: its ctx is detached from any trace, so its
// queries stay out of every trace too.
func pollGauges(ctx context.Context, reads gaugeReads, m *metrics.Metrics, every time.Duration) {
	ctx = telemetry.Detach(ctx)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, ok := reads.backlog(ctx); ok {
				m.OutboxBacklog.Set(float64(n))
			}
			if reads.groupLag != nil {
				for _, proj := range []string{"graph", "search"} {
					if lag, err := reads.groupLag(ctx, "proj:"+proj); err == nil {
						m.ProjectionLag.WithLabelValues(proj).Set(float64(lag))
					}
				}
			}
		}
	}
}

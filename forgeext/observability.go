package forgeext

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/internal/metrics"
)

// wireObservability registers fabriq's Prometheus instruments, mounts
// GET /metrics on the forge router, and returns the instruments for use by
// the relay callback and gauge pollers.
func wireObservability(app forge.App, _ *fabriq.Stores) (*metrics.Metrics, error) {
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg)
	if err != nil {
		return nil, err
	}
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	if err := app.Router().GET("/metrics", func(ctx forge.Context) error {
		handler.ServeHTTP(ctx.Response(), ctx.Request())
		return nil
	}); err != nil {
		return nil, err
	}
	return m, nil
}

// pollGauges keeps the slow-moving gauges fresh.
func pollGauges(ctx context.Context, stores *fabriq.Stores, m *metrics.Metrics, every time.Duration) {
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
						m.ProjectionLag.WithLabelValues(proj, "_all").Set(float64(lag))
					}
				}
			}
		}
	}
}

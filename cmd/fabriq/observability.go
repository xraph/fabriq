package main

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
// GET /metrics on the forge router (forge's own /_/livez, /_/readyz and
// /_/health cover liveness), and starts the gauge pollers:
//
//   - fabriq_outbox_backlog: unpublished outbox rows
//   - fabriq_projection_lag_events{projection}: consumer-group lag on the
//     event stream (tenant label "_all": lag is a group property)
//   - fabriq_tenant_hook_trips_total: backstop trips
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
			row := stores.Postgres.Driver().QueryRow(ctx,
				`SELECT count(*) FROM fabriq_outbox WHERE published_at IS NULL`)
			var backlog int64
			if err := row.Scan(&backlog); err == nil {
				m.OutboxBacklog.Set(float64(backlog))
			}
			if trips := stores.Postgres.BackstopTrips(); trips > lastTrips {
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

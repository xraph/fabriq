// Package metrics defines fabriq's Prometheus instruments. Binaries
// register them on their registry (forge exposes /_/metrics); library
// code receives the instruments it needs as plain callbacks/gauges so
// core stays dependency-free.
//
// Operational meaning (see docs/OPERATIONS.md for runbooks):
//
//   - fabriq_outbox_backlog: unpublished outbox rows. Sustained growth
//     means the relay is down or Redis is unreachable.
//   - fabriq_tenant_hook_trips_total: the tenant backstop fired. Any
//     non-zero value in production is a fabriq bug — page.
//   - fabriq_conflation_depth: deltas buffered in the hub. Sustained
//     growth means subscribers cannot keep up.
//   - fabriq_projection_lag_events{projection,tenant}: events behind the
//     stream head (phase 4 wires per-consumer measurement).
//   - fabriq_relay_published_total: relay publish throughput.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics bundles fabriq's instruments.
type Metrics struct {
	OutboxBacklog   prometheus.Gauge
	TenantHookTrips prometheus.Counter
	ConflationDepth prometheus.Gauge
	ProjectionLag   *prometheus.GaugeVec
	RelayPublished  prometheus.Counter
}

// New creates and registers the instruments on reg.
func New(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		OutboxBacklog: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_outbox_backlog",
			Help: "Unpublished transactional-outbox rows.",
		}),
		TenantHookTrips: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_tenant_hook_trips_total",
			Help: "Tenant-guard backstop trips; non-zero means a fabriq bug.",
		}),
		ConflationDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_conflation_depth",
			Help: "Deltas buffered in subscription-hub conflation windows.",
		}),
		ProjectionLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fabriq_projection_lag_events",
			Help: "Events between a projection's position and the stream head.",
		}, []string{"projection", "tenant"}),
		RelayPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_relay_published_total",
			Help: "Events published by the outbox relay.",
		}),
	}
	for _, c := range []prometheus.Collector{
		m.OutboxBacklog, m.TenantHookTrips, m.ConflationDepth, m.ProjectionLag, m.RelayPublished,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

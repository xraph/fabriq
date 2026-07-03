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

	// Blob CAS garbage-collection instruments.
	BlobGCBytesFreed        prometheus.Counter
	BlobGCCollected         prometheus.Counter
	BlobGCRefDriftCorrected prometheus.Counter
	BlobGCBroken            prometheus.Gauge
	BlobGCOrphans           prometheus.Counter

	// Embedding worker (proj:embed) instruments.
	EmbedEventsTotal   prometheus.Counter
	EmbedFailuresTotal prometheus.Counter

	// Distillation worker (proj:distill) instruments.
	DistillNodesTotal          prometheus.Counter
	DistillSummariesTotal      prometheus.Counter
	DistillShortCircuitTotal   prometheus.Counter
	DistillGuardBlockedTotal   prometheus.Counter
	DistillFailuresTotal       prometheus.Counter
	DistillSplitsTotal         prometheus.Counter
	DistillDedupTotal          prometheus.Counter
	DistillIntermediateGCTotal prometheus.Counter
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
		BlobGCBytesFreed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_blob_gc_bytes_freed_total",
			Help: "Bytes reclaimed by blob CAS garbage collection.",
		}),
		BlobGCCollected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_blob_gc_collected_total",
			Help: "Unreferenced CAS entries garbage-collected.",
		}),
		BlobGCRefDriftCorrected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_blob_gc_ref_drift_corrected_total",
			Help: "fabriq_blob_cas ref_count values corrected from the catalog truth.",
		}),
		BlobGCBroken: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_blob_gc_broken",
			Help: "Referenced hashes whose bytes are missing (last reconcile cycle).",
		}),
		BlobGCOrphans: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_blob_gc_orphans_total",
			Help: "Orphan byte objects (no ledger row) deleted.",
		}),
		EmbedEventsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_embed_events_total",
			Help: "Events handled by the embed worker (indexed or ack-skipped).",
		}),
		EmbedFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_embed_failures_total",
			Help: "Events the embed worker failed to process (transient; left pending for retry).",
		}),
		DistillNodesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_distill_nodes_total", Help: "Digest nodes (re)built by the distill worker."}),
		DistillSummariesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_distill_summaries_total", Help: "Summarizer calls made by the distill worker."}),
		DistillShortCircuitTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_distill_shortcircuit_total", Help: "Nodes skipped via the Merkle short-circuit."}),
		DistillGuardBlockedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_distill_guard_blocked_total", Help: "Contents dropped by the guard (fail-closed or block)."}),
		DistillFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_distill_failures_total", Help: "Events the distill worker failed to process (transient)."}),
		DistillSplitsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_distill_splits_total", Help: "Adaptive-depth node splits performed by the distill worker."}),
		DistillDedupTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_distill_dedup_total", Help: "L0 summaries reused via exact source-hash dedup."}),
		DistillIntermediateGCTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_distill_intermediate_gc_total", Help: "Orphaned adaptive-depth intermediate nodes garbage-collected."}),
	}
	for _, c := range []prometheus.Collector{
		m.OutboxBacklog, m.TenantHookTrips, m.ConflationDepth, m.ProjectionLag, m.RelayPublished,
		m.BlobGCBytesFreed, m.BlobGCCollected, m.BlobGCRefDriftCorrected, m.BlobGCBroken, m.BlobGCOrphans,
		m.EmbedEventsTotal, m.EmbedFailuresTotal,
		m.DistillNodesTotal, m.DistillSummariesTotal, m.DistillShortCircuitTotal, m.DistillGuardBlockedTotal, m.DistillFailuresTotal,
		m.DistillSplitsTotal, m.DistillDedupTotal, m.DistillIntermediateGCTotal,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

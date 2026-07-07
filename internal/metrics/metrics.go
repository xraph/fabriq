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

	// Analytics consumer (proj:analytics) instruments.
	AnalyticsAppliedTotal      prometheus.Counter
	AnalyticsFailuresTotal     prometheus.Counter
	AnalyticsLagSeconds        prometheus.Gauge
	AnalyticsTenantsBehind     prometheus.Gauge
	AnalyticsEventsPrunedTotal prometheus.Counter

	// Catalog-mode sweeper instruments.
	SweepPassDuration   prometheus.Histogram
	SweepTenantsTracked prometheus.Gauge
	SweepEligible       prometheus.Gauge
	SweepSweptTotal     prometheus.Counter
	SweepBusyTotal      prometheus.Counter
	SweepErrorsTotal    prometheus.Counter
	PoolShardsOpen      prometheus.Gauge
	PoolShardsHeld      prometheus.Gauge
	PoolCap             prometheus.Gauge
	PoolScaleEvents     *prometheus.CounterVec

	// Catalog HA routing-read instruments (Failover primary/replica/failover
	// counters, polled as gauges — see catalog.Failover.ReadStats).
	CatalogReadPrimary  prometheus.Gauge
	CatalogReadReplica  prometheus.Gauge
	CatalogReadFailover prometheus.Gauge

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
		AnalyticsAppliedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_analytics_applied_total",
			Help: "Envelopes successfully applied by the analytics consumer (proj:analytics).",
		}),
		AnalyticsFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_analytics_failures_total",
			Help: "Envelopes the analytics consumer failed to apply (transient; left pending for retry).",
		}),
		AnalyticsLagSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_analytics_lag_seconds",
			Help: "Worst-case analytics freshness: the stalest tenant's lag (now() minus that tenant's newest fact). A single stalled tenant moves it, unmasked by others.",
		}),
		AnalyticsTenantsBehind: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_analytics_tenants_behind",
			Help: "Number of tenants whose analytics lag exceeds the alarm threshold (60s).",
		}),
		AnalyticsEventsPrunedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_analytics_events_pruned_total",
			Help: "Analytics history events deleted by the retention pruner.",
		}),
		SweepPassDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fabriq_sweep_pass_duration_seconds",
			Help:    "Wall-clock duration of one catalog sweep pass.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms .. ~10s
		}),
		SweepTenantsTracked: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_sweep_tenants_tracked",
			Help: "Tenants in the sweeper's idle-backoff table (last pass).",
		}),
		SweepEligible: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_sweep_tenants_eligible",
			Help: "Active, version-current tenants seen by the last sweep pass.",
		}),
		SweepSweptTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_sweep_swept_total",
			Help: "Tenant maintenance passes dispatched by the sweeper.",
		}),
		SweepBusyTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_sweep_busy_total",
			Help: "Tenant maintenance passes that found work.",
		}),
		SweepErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_sweep_errors_total",
			Help: "Tenant maintenance passes that failed (tenant backs off).",
		}),
		PoolShardsOpen: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_pool_shards_open",
			Help: "Tenant database pools currently open (catalog mode).",
		}),
		PoolShardsHeld: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_pool_shards_held",
			Help: "Open tenant database pools with in-flight acquisitions.",
		}),
		PoolCap: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_pool_cap",
			Help: "Current effective MaxActive shard-pool cap (catalog mode).",
		}),
		PoolScaleEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fabriq_pool_scale_events_total",
			Help: "Adaptive pool cap scaling decisions by direction.",
		}, []string{"direction"}),
		CatalogReadPrimary: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_catalog_read_primary_total",
			Help: "Routing catalog reads served by the primary.",
		}),
		CatalogReadReplica: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_catalog_read_replica_total",
			Help: "Routing catalog reads served by a replica (failover).",
		}),
		CatalogReadFailover: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_catalog_read_failover_total",
			Help: "Routing catalog read failover events (primary unreachable).",
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
		m.AnalyticsAppliedTotal, m.AnalyticsFailuresTotal, m.AnalyticsLagSeconds, m.AnalyticsTenantsBehind, m.AnalyticsEventsPrunedTotal,
		m.SweepPassDuration, m.SweepTenantsTracked, m.SweepEligible,
		m.SweepSweptTotal, m.SweepBusyTotal, m.SweepErrorsTotal,
		m.PoolShardsOpen, m.PoolShardsHeld, m.PoolCap, m.PoolScaleEvents,
		m.CatalogReadPrimary, m.CatalogReadReplica, m.CatalogReadFailover,
		m.DistillNodesTotal, m.DistillSummariesTotal, m.DistillShortCircuitTotal, m.DistillGuardBlockedTotal, m.DistillFailuresTotal,
		m.DistillSplitsTotal, m.DistillDedupTotal, m.DistillIntermediateGCTotal,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

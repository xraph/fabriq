// Package metrics defines fabriq's Prometheus instruments. They live on a
// PRIVATE registry that fabriq's forge extension serves at GET /metrics, so
// every series keeps its fabriq_ name with no forge_ prefix. forge's own
// series (HTTP, and grove's db_client_operation_duration_seconds for the
// databases fabriq opens) are on forge's /_/metrics instead; scrape both.
// Library code receives the instruments it needs as plain callbacks or
// observer interfaces so core stays dependency-free.
//
// Operational meaning (see docs/OPERATIONS.md for runbooks):
//
//   - fabriq_outbox_backlog: unpublished outbox rows. Sustained growth
//     means the relay is down or Redis is unreachable. Worker only.
//   - fabriq_tenant_hook_trips_total: the tenant backstop fired in this
//     process, read at scrape time on every node. Any non-zero value in
//     production is a fabriq bug. Page.
//   - fabriq_conflation_depth: deltas buffered in this node's subscription
//     hub, read at scrape time. Sustained growth means subscribers cannot
//     keep up.
//   - fabriq_projection_lag_events{projection}: entries the graph or search
//     consumer group has not yet acknowledged (lag plus pending). Worker
//     only.
//   - fabriq_relay_published_total: relay publish throughput.
//   - fabriq_command_duration_seconds{op,result},
//     fabriq_relay_batch_duration_seconds{result},
//     fabriq_projection_apply_duration_seconds{projection,result} and
//     fabriq_model_call_duration_seconds{kind,result}: latency of the work
//     the matching spans cover.
//   - fabriq_worker_restarts_total{runner}: a background runner exited and
//     was restarted. Steady growth means it keeps crashing.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles fabriq's instruments.
type Metrics struct {
	OutboxBacklog  prometheus.Gauge
	ProjectionLag  *prometheus.GaugeVec
	RelayPublished prometheus.Counter

	// Durations of the work fabriq traces, and runner restarts.
	CommandDuration         *prometheus.HistogramVec
	RelayBatchDuration      *prometheus.HistogramVec
	ProjectionApplyDuration *prometheus.HistogramVec
	ModelCallDuration       *prometheus.HistogramVec
	WorkerRestarts          *prometheus.CounterVec

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

// Option supplies the live readings behind fabriq's scrape-time series.
type Option func(*sources)

type sources struct {
	catalogReads    func() (primary, replica, failover int64, ok bool)
	conflationDepth func() int
	backstopTrips   func() int64
}

// WithCatalogReads supplies the routing catalog's cumulative read counters
// (catalog.Failover.ReadStats). They are read at scrape time as the
// fabriq_catalog_read_*_total counters; ok=false reads as zero.
func WithCatalogReads(fn func() (primary, replica, failover int64, ok bool)) Option {
	return func(s *sources) { s.catalogReads = fn }
}

// WithBackstopTrips supplies this process's cumulative tenant-guard
// backstop trips, read at scrape time as fabriq_tenant_hook_trips_total. The
// trips happen wherever a query runs, so every node reports its own.
func WithBackstopTrips(fn func() int64) Option {
	return func(s *sources) { s.backstopTrips = fn }
}

// WithConflationDepth supplies the subscription hub's buffered delta count,
// read at scrape time as fabriq_conflation_depth.
func WithConflationDepth(fn func() int) Option {
	return func(s *sources) { s.conflationDepth = fn }
}

// durationBuckets spans 1ms to about 8s: commands, relay batches and
// projection applies.
var durationBuckets = prometheus.ExponentialBuckets(0.001, 2, 14)

// modelBuckets spans 50ms to about 100s: embedding and summarizer calls to a
// model provider.
var modelBuckets = prometheus.ExponentialBuckets(0.05, 2, 12)

// New creates and registers the instruments on reg.
func New(reg prometheus.Registerer, opts ...Option) (*Metrics, error) {
	var src sources
	for _, o := range opts {
		o(&src)
	}
	catalogRead := func(pick func(p, r, f int64) int64) func() float64 {
		return func() float64 {
			if src.catalogReads == nil {
				return 0
			}
			p, r, f, ok := src.catalogReads()
			if !ok {
				return 0
			}
			return float64(pick(p, r, f))
		}
	}
	funcs := []prometheus.Collector{
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "fabriq_conflation_depth",
			Help: "Deltas buffered in this node's subscription-hub conflation windows.",
		}, func() float64 {
			if src.conflationDepth == nil {
				return 0
			}
			return float64(src.conflationDepth())
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "fabriq_tenant_hook_trips_total",
			Help: "Tenant-guard backstop trips in this process; non-zero means a fabriq bug.",
		}, func() float64 {
			if src.backstopTrips == nil {
				return 0
			}
			return float64(src.backstopTrips())
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "fabriq_catalog_read_primary_total",
			Help: "Routing catalog reads served by the primary.",
		}, catalogRead(func(p, _, _ int64) int64 { return p })),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "fabriq_catalog_read_replica_total",
			Help: "Routing catalog reads served by a replica (failover).",
		}, catalogRead(func(_, r, _ int64) int64 { return r })),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "fabriq_catalog_read_failover_total",
			Help: "Routing catalog read failover events (primary unreachable).",
		}, catalogRead(func(_, _, f int64) int64 { return f })),
	}

	m := &Metrics{
		OutboxBacklog: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fabriq_outbox_backlog",
			Help: "Unpublished transactional-outbox rows.",
		}),
		ProjectionLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "fabriq_projection_lag_events",
			Help: "Stream entries a projection's consumer group has not acknowledged (lag plus pending).",
		}, []string{"projection"}),
		RelayPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fabriq_relay_published_total",
			Help: "Events published by the outbox relay.",
		}),
		CommandDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fabriq_command_duration_seconds",
			Help:    "Duration of one command batch (Exec or ExecBatch), validation through commit.",
			Buckets: durationBuckets,
		}, []string{"op", "result"}),
		RelayBatchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fabriq_relay_batch_duration_seconds",
			Help:    "Duration of one outbox batch the relay published (empty polls are not observed).",
			Buckets: durationBuckets,
		}, []string{"result"}),
		ProjectionApplyDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fabriq_projection_apply_duration_seconds",
			Help:    "Duration of applying one envelope to a projection (skipped envelopes are not observed).",
			Buckets: durationBuckets,
		}, []string{"projection", "result"}),
		ModelCallDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fabriq_model_call_duration_seconds",
			Help:    "Duration of one Embedder or Summarizer call made by the embed and distill workers.",
			Buckets: modelBuckets,
		}, []string{"kind", "result"}),
		WorkerRestarts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fabriq_worker_restarts_total",
			Help: "Background runner exits that the supervisor restarted.",
		}, []string{"runner"}),
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
	for _, c := range append(funcs,
		m.OutboxBacklog, m.ProjectionLag, m.RelayPublished,
		m.CommandDuration, m.RelayBatchDuration, m.ProjectionApplyDuration, m.ModelCallDuration, m.WorkerRestarts,
		m.BlobGCBytesFreed, m.BlobGCCollected, m.BlobGCRefDriftCorrected, m.BlobGCBroken, m.BlobGCOrphans,
		m.EmbedEventsTotal, m.EmbedFailuresTotal,
		m.AnalyticsAppliedTotal, m.AnalyticsFailuresTotal, m.AnalyticsLagSeconds, m.AnalyticsTenantsBehind, m.AnalyticsEventsPrunedTotal,
		m.SweepPassDuration, m.SweepTenantsTracked, m.SweepEligible,
		m.SweepSweptTotal, m.SweepBusyTotal, m.SweepErrorsTotal,
		m.PoolShardsOpen, m.PoolShardsHeld, m.PoolCap, m.PoolScaleEvents,
		m.DistillNodesTotal, m.DistillSummariesTotal, m.DistillShortCircuitTotal, m.DistillGuardBlockedTotal, m.DistillFailuresTotal,
		m.DistillSplitsTotal, m.DistillDedupTotal, m.DistillIntermediateGCTotal,
	) {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// ObserveCommand implements telemetry.CommandObserver.
func (m *Metrics) ObserveCommand(op, result string, elapsed time.Duration) {
	m.CommandDuration.WithLabelValues(op, result).Observe(elapsed.Seconds())
}

// ObserveApply implements telemetry.ApplyObserver.
func (m *Metrics) ObserveApply(projection, result string, elapsed time.Duration) {
	m.ProjectionApplyDuration.WithLabelValues(projection, result).Observe(elapsed.Seconds())
}

// ObserveRelayBatch records one published outbox batch.
func (m *Metrics) ObserveRelayBatch(result string, elapsed time.Duration) {
	m.RelayBatchDuration.WithLabelValues(result).Observe(elapsed.Seconds())
}

// ObserveModelCall records one Embedder (kind "embed") or Summarizer (kind
// "summarize") call.
func (m *Metrics) ObserveModelCall(kind, result string, elapsed time.Duration) {
	m.ModelCallDuration.WithLabelValues(kind, result).Observe(elapsed.Seconds())
}

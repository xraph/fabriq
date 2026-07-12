package forgeext

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/internal/metrics"
)

// Advisory lock keys live with the adapter (postgres.LockKey*) so the
// static worker's leader election and the catalog-mode sweeper's per-pass
// claims can never diverge — same keys, same databases, one worker wins.
const (
	lockKeyRelay         = postgres.LockKeyRelay
	lockKeyReconciler    = postgres.LockKeyReconciler
	lockKeyDocumentPlane = postgres.LockKeyDocumentPlane
	lockKeyBlobGC        = postgres.LockKeyBlobGC
)

// Run implements forge.RunnableExtension: supervise the leader-elected relay
// until shutdown. If RunWorker is false this is a no-op.
func (e *Extension) Run(ctx context.Context) error {
	if !e.cfg.RunWorker {
		return nil
	}

	e.mu.Lock()
	stores := e.stores
	e.mu.Unlock()
	if stores == nil {
		return fmt.Errorf("fabriq: Run called before Start")
	}
	if stores.Postgres == nil {
		// Catalog mode: the worker plane is the sweeper (spec 2026-07-03 D5),
		// not boot-time per-shard loops.
		if stores.Catalog == nil || stores.TenantSweeper() == nil {
			return fmt.Errorf("fabriq: Run needs either a primary shard or a tenant catalog")
		}
		return e.runCatalogSweeper()
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	e.mu.Lock()
	e.cancel, e.done = cancel, done
	e.mu.Unlock()

	// Observability: /metrics + gauge pollers.
	app := e.App()
	var relayOpts []postgres.RelayOption
	if app != nil {
		if m, err := wireObservability(app, stores); err == nil {
			e.mu.Lock()
			e.metrics = m
			e.mu.Unlock()
			relayOpts = append(relayOpts, postgres.WithRelayOnPublish(func(n int) {
				m.RelayPublished.Add(float64(n))
			}))
			go pollGauges(runCtx, stores, m, 15*time.Second)
		}
	}

	var logger forge.Logger
	if app != nil {
		logger = app.Logger()
	}

	// Outbox relay: one per shard. The outbox is shard-local, and advisory
	// locks are per-database, so each shard elects its own relay leader
	// independently — relay throughput scales with shard count (ADR 0007).
	var wg sync.WaitGroup
	shardPGs := stores.ShardPGs()
	for _, sp := range shardPGs {
		sp := sp
		relay := postgres.NewRelay(sp.PG, e.reg, stores.Redis, relayOpts...)
		elector := postgres.NewElector(sp.PG, lockKeyRelay)
		label := "relay"
		if len(shardPGs) > 1 {
			label = "relay:" + sp.ID
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, label, func(c context.Context) error { return elector.Run(c, relay.Run) })
		}()
	}

	// Document plane: quiet-window materializer + compactor (leader 1003).
	docElector := postgres.NewElector(stores.Postgres, lockKeyDocumentPlane)
	wg.Add(1)
	go func() {
		defer wg.Done()
		supervise(runCtx, logger, "document-plane", func(c context.Context) error {
			return docElector.Run(c, func(leadCtx context.Context) error {
				e.runDocumentPlane(leadCtx, time.Second)
				return leadCtx.Err()
			})
		})
	}()

	// Scheduled reconciler: leader-elected, one scanner across replicas.
	reconcileInterval := e.cfg.ReconcileInterval
	if reconcileInterval > 0 && (stores.Falkor != nil || stores.Elastic != nil) {
		reconElector := postgres.NewElector(stores.Postgres, lockKeyReconciler)
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "reconciler", func(c context.Context) error {
				return reconElector.Run(c, func(leadCtx context.Context) error {
					e.runReconciler(leadCtx, reconcileInterval)
					return leadCtx.Err()
				})
			})
		}()
	}

	// Blob CAS GC: leader-elected (lock 1004), one reconciler across replicas.
	// Reuses the reconcile interval; disabled when CAS is not configured.
	if reconcileInterval > 0 && stores.CAS != nil {
		gcElector := postgres.NewElector(stores.Postgres, lockKeyBlobGC)
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "blob-gc", func(c context.Context) error {
				return gcElector.Run(c, func(leadCtx context.Context) error {
					e.runBlobGC(leadCtx, reconcileInterval)
					return leadCtx.Err()
				})
			})
		}()
	}

	// Projection consumers scale by replica count — no election needed.
	consumer := consumerName()
	e.mu.Lock()
	fab := e.fab
	e.mu.Unlock()
	if stores.Falkor != nil {
		engine, err := stores.GraphEngine(e.reg, fab.Upcasters())
		if err != nil {
			cancel()
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:graph", func(c context.Context) error { return engine.Run(c, consumer) })
		}()
	}
	if stores.Elastic != nil {
		engine, err := stores.SearchEngine(e.reg, fab.Upcasters())
		if err != nil {
			cancel()
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:search", func(c context.Context) error { return engine.Run(c, consumer) })
		}()
	}
	if stores.Analytics != nil && stores.Redis != nil && hasAnalyticsEntity(e.reg) {
		cons, err := stores.AnalyticsConsumer(e.reg, fab.Upcasters())
		if err != nil {
			cancel()
			return err
		}
		if e.metrics != nil {
			cons.OnApplied = e.metrics.AnalyticsAppliedTotal.Inc
			cons.OnFailure = e.metrics.AnalyticsFailuresTotal.Inc
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:analytics", func(c context.Context) error { return cons.Run(c, consumer) })
		}()
		if e.metrics != nil {
			m, sink := e.metrics, stores.Analytics
			wg.Add(1)
			go func() {
				defer wg.Done()
				pollAnalyticsLag(runCtx, m, sink)
			}()
		}
	} else if stores.Analytics != nil && stores.Redis != nil {
		if logger != nil {
			logger.Warn("fabriq: analytics is configured but no entity is marked for it; nothing will flow to the analytics sink")
		}
	}
	if stores.Analytics != nil && (e.cfg.Fabriq.Analytics.EventRetention > 0 || e.cfg.Fabriq.Analytics.PartitionEvents) {
		m, sink, retention := e.metrics, stores.Analytics, e.cfg.Fabriq.Analytics.EventRetention
		wg.Add(1)
		go func() {
			defer wg.Done()
			analyticsRetentionLoop(runCtx, m, sink, retention)
		}()
	}
	if e.cfg.Fabriq.Insights.Enabled && stores.Redis != nil && hasInsightsEntity(e.reg) {
		cons, err := stores.InsightsConsumer(e.reg)
		if err != nil {
			cancel()
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:insights", func(c context.Context) error { return cons.Run(c, consumer) })
		}()
	} else if e.cfg.Fabriq.Insights.Enabled && stores.Redis != nil {
		if logger != nil {
			logger.Warn("fabriq: insights is enabled but no entity is marked for it; nothing will flow to any tenant's insights store")
		}
	}

	// Embedding worker: one consumer per replica, no election needed.
	if e.cfg.Embedder != nil && stores.Redis != nil && hasEmbeddableEntity(e.reg) {
		ix, ierr := agent.NewIndexer(fab, e.reg, e.cfg.Embedder)
		if ierr != nil {
			cancel()
			return ierr
		}
		handle := embedHandler(runCtx, ix, e.metrics)
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:embed", func(c context.Context) error {
				if err := stores.Redis.EnsureGroup(c, "proj:embed"); err != nil {
					return err
				}
				return stores.Redis.Consume(c, "proj:embed", consumer, handle)
			})
		}()
	}

	// Distillation worker: debounced, per-tenant single-flight; one per replica.
	// Requires an Embedder (digests share the entity vector space) + a Summarizer
	// + CAS + Redis + at least one distillable entity.
	if e.cfg.Summarizer != nil && e.cfg.Embedder != nil && stores.Redis != nil && stores.CAS != nil && hasDistillableEntity(e.reg) {
		dcfg := agent.DistillConfig{
			VectorDims:    e.cfg.Embedder.Dims(), // dims come straight from the embedder
			RecipeVersion: e.cfg.DistillRecipeVersion,
			FailOpenGuard: e.cfg.DistillFailOpenGuard,
			Clusterer:     e.cfg.Clusterer,
		}
		dist, derr := agent.NewDistiller(fab, e.reg, e.cfg.Embedder, e.cfg.Summarizer, e.cfg.Guard, stores.CAS, dcfg)
		if derr != nil {
			cancel()
			return derr
		}
		sw := newDistillSweeper(dist, e.cfg.DistillDebounce, e.cfg.DistillMaxWait, e.metrics)
		handle := distillHandler(runCtx, sw)
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:distill", func(c context.Context) error {
				if err := stores.Redis.EnsureGroup(c, "proj:distill"); err != nil {
					return err
				}
				return stores.Redis.Consume(c, "proj:distill", consumer, handle)
			})
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()
	// Run returns immediately; the worker's lifetime is bound to runCtx and
	// torn down by Shutdown, not by this call's ctx — so ctx is intentionally
	// not propagated to the background loops.
	_ = ctx
	return nil
}

// Shutdown implements forge.RunnableExtension: SIGTERM drain.
// If RunWorker is false (or Run was never called), this is a no-op.
func (e *Extension) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	cancel, done := e.cancel, e.done
	e.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return fmt.Errorf("fabriq: worker did not drain in time")
	}
}

// consumerName identifies this replica within the consumer groups.
func consumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "fabriq-worker"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// supervise keeps a runner alive for the worker's lifetime: every exit is
// logged (never swallowed) and restarted with exponential backoff
// (1s -> 30s cap), resetting after a healthy stretch.
func supervise(ctx context.Context, log forge.Logger, name string, run func(ctx context.Context) error) {
	const (
		baseBackoff  = time.Second
		maxBackoff   = 30 * time.Second
		healthyReset = 5 * time.Minute
	)
	backoff := baseBackoff
	for {
		started := time.Now()
		err := run(ctx)
		if ctx.Err() != nil {
			return // orderly shutdown
		}
		if time.Since(started) >= healthyReset {
			backoff = baseBackoff
		}
		if log != nil {
			log.Error("fabriq: runner exited; restarting",
				forge.String("runner", name),
				forge.Duration("backoff", backoff),
				forge.Error(err),
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// runReconciler is the scheduled drift healer: leader-elected (lock 1002)
// so exactly one replica scans, iterating every tenant that ever emitted an event.
// Interval 0 disables it.
func (e *Extension) runReconciler(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.reconcileAll(ctx)
		}
	}
}

// runBlobGC is the scheduled blob garbage collector: leader-elected (lock
// 1004) so exactly one replica reconciles, iterating every tenant. Interval 0
// disables it.
func (e *Extension) runBlobGC(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.gcBlobAll(ctx)
		}
	}
}

// gcBlobAll reconciles the blob CAS for every tenant, repairing drift and
// collecting unreferenced bytes past the grace window.
func (e *Extension) gcBlobAll(ctx context.Context) {
	e.mu.Lock()
	stores := e.stores
	m := e.metrics
	e.mu.Unlock()
	if stores == nil {
		return
	}
	grace := e.cfg.BlobGCGrace
	if grace <= 0 {
		grace = time.Hour
	}
	rec, err := stores.BlobReconciler(grace)
	if err != nil {
		return
	}
	tenants, err := stores.AllTenants(ctx)
	if err != nil {
		return
	}
	var broken int
	for _, tenantID := range tenants {
		tctx, err := tenant.WithTenant(ctx, tenantID)
		if err != nil {
			continue
		}
		rep, err := rec.Reconcile(tctx, true)
		if err != nil {
			continue
		}
		broken += len(rep.Broken)
		if m != nil {
			m.BlobGCBytesFreed.Add(float64(rep.BytesFreed))
			m.BlobGCCollected.Add(float64(rep.GCCount))
			m.BlobGCRefDriftCorrected.Add(float64(rep.RefsCorrected))
			m.BlobGCOrphans.Add(float64(rep.OrphansDeleted))
		}
	}
	if m != nil {
		m.BlobGCBroken.Set(float64(broken))
	}
}

// runDocumentPlane is the materializer + compactor: every interval it
// materializes quiet documents (one ordinary versioned event each) and
// compacts logs at their SnapshotEvery budget. Leader-elected (1003).
// Both sweeps go through stores.Docs — the archive-wired store — so
// compaction seals trimmed history to blob segments when configured
// (Postgres.Documents() would mint a store without the blob handle).
func (e *Extension) runDocumentPlane(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Materialization is latency-sensitive (quiet-window detection) and
	// runs every tick; the compaction sweep scans and aggregates the whole
	// log table, so it runs on its own, much slower cadence
	// (Config.DocCompactInterval; default 30s).
	compactInterval := e.cfg.DocCompactInterval
	if compactInterval <= 0 {
		compactInterval = 30 * time.Second
	}
	compactTicker := time.NewTicker(compactInterval)
	defer compactTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.mu.Lock()
			stores := e.stores
			e.mu.Unlock()
			if stores == nil {
				continue
			}
			_, _ = stores.Docs.MaterializeQuiet(ctx, nil)
		case <-compactTicker.C:
			e.mu.Lock()
			stores := e.stores
			e.mu.Unlock()
			if stores == nil {
				continue
			}
			_, _ = stores.Docs.CompactDue(ctx)
		}
	}
}

func (e *Extension) reconcileAll(ctx context.Context) {
	e.mu.Lock()
	stores := e.stores
	e.mu.Unlock()
	if stores == nil {
		return
	}
	tenants, err := stores.AllTenants(ctx)
	if err != nil {
		return
	}
	for _, tenantID := range tenants {
		if stores.Falkor != nil {
			if rec, err := stores.GraphReconciler(e.reg); err == nil {
				_, _ = rec.Reconcile(ctx, tenantID, true)
			}
		}
		if stores.Elastic != nil {
			if rec, err := stores.SearchReconciler(e.reg); err == nil {
				_, _ = rec.Reconcile(ctx, tenantID, true)
			}
		}
	}
}

// analyticsRetentionLoop keeps the append-only event log bounded on a slow
// (hourly) cadence: it maintains monthly partitions (creating upcoming ones and
// dropping aged ones — a no-op for a non-partitioned sink) and, when retention
// is set, deletes any straggler events older than the window. Facts are never
// pruned. Runs on every replica; both operations are idempotent, so redundant
// runs just find less to do.
func analyticsRetentionLoop(ctx context.Context, m *metrics.Metrics, sink analytics.Sink, retention time.Duration) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	tick := func() {
		// Partition maintenance first (create ahead / drop aged), then a
		// delete-scan for anything retention-expired that a dropped partition
		// did not cover (the default partition, or a non-partitioned table).
		_, _, _ = sink.MaintainPartitions(ctx, retention)
		if retention > 0 {
			if n, err := sink.PruneEvents(ctx, time.Now().Add(-retention)); err == nil && n > 0 && m != nil {
				m.AnalyticsEventsPrunedTotal.Add(float64(n))
			}
		}
	}
	tick() // once at startup so upcoming partitions exist and an idle log is trimmed
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}

// analyticsLagBehindThreshold is the per-tenant lag past which a tenant counts
// toward fabriq_analytics_tenants_behind — the "this tenant is stalled" alarm.
const analyticsLagBehindThreshold = 60.0 // seconds

// pollAnalyticsLag samples per-tenant analytics freshness every 15s and
// publishes two low-cardinality gauges derived from it: the worst-case lag
// (stalest tenant) and the count of tenants past the alarm threshold. Reading
// per-tenant (rather than one fleet-wide max(at)) is what keeps a single
// stalled tenant from hiding behind others still flowing. Skips a tick on a
// transient read error or an empty sink. Shared by the static worker and the
// catalog sweeper planes.
func pollAnalyticsLag(ctx context.Context, m *metrics.Metrics, sink analytics.Sink) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lags, err := sink.LagByTenant(ctx)
			if err != nil || len(lags) == 0 {
				continue
			}
			var worst float64
			var behind int
			for _, lag := range lags {
				if lag > worst {
					worst = lag
				}
				if lag > analyticsLagBehindThreshold {
					behind++
				}
			}
			m.AnalyticsLagSeconds.Set(worst)
			m.AnalyticsTenantsBehind.Set(float64(behind))
		}
	}
}

package forgeext

import (
	"context"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/internal/metrics"
	"github.com/xraph/fabriq/migrations"
)

// runCatalogSweeper is the catalog-mode worker plane: one sweep engine per
// replica walks the tenant catalog and runs claim-guarded maintenance
// passes (relay -> materialize -> compact) against each active tenant's
// database. Replicas cooperate through the per-database advisory locks, so
// scaling out never duplicates work. Redis wake nudges (published by the
// facade write path) give busy tenants sub-second relay latency.
//
// Called from Run under its RunWorker gate; mirrors Run's lifecycle
// contract (returns immediately, torn down by Shutdown).
func (e *Extension) runCatalogSweeper() error {
	e.mu.Lock()
	stores := e.stores
	e.mu.Unlock()

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	e.mu.Lock()
	e.cancel, e.done = cancel, done
	e.mu.Unlock()

	var logger forge.Logger
	app := e.App()
	if app != nil {
		logger = app.Logger()
	}

	// Observability: /metrics plus per-pass sweeper counters and pool
	// gauges (spec P6). Best-effort — a metrics failure never blocks the
	// worker plane.
	var m *metrics.Metrics
	if app != nil {
		if wired, err := wireObservability(app, stores); err == nil {
			m = wired
			stores.AttachMetrics(m)
			e.mu.Lock()
			e.metrics = m
			e.mu.Unlock()
		}
	}

	engine := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		CompactEvery: e.cfg.DocCompactInterval,
		MinVersion:   migrations.HeadVersion(),
		OnPass: func(st sweep.Stats) {
			if m == nil {
				return
			}
			m.SweepPassDuration.Observe(st.Duration.Seconds())
			m.SweepEligible.Set(float64(st.Eligible))
			m.SweepSweptTotal.Add(float64(st.Swept))
			m.SweepBusyTotal.Add(float64(st.Busy))
			m.SweepErrorsTotal.Add(float64(st.Errors))
			if open, held, ok := stores.PoolStats(); ok {
				m.PoolShardsOpen.Set(float64(open))
				m.PoolShardsHeld.Set(float64(held))
			}
			if capv, ok := stores.PoolCap(); ok {
				m.PoolCap.Set(float64(capv))
			}
		},
		OnError: func(tenantID string, err error) {
			if logger != nil {
				logger.Warn("fabriq: tenant sweep failed",
					forge.String("tenant", tenantID), forge.Error(err))
			}
		},
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		supervise(runCtx, logger, "sweeper", engine.Run)
	}()

	// Projection consumers are unchanged from the static plane: consumer
	// groups on the shared stream scale by replica count, no election —
	// the sweeper's relays feed them and the bookkeeping routes per tenant.
	consumer := consumerName()
	e.mu.Lock()
	fab := e.fab
	e.mu.Unlock()
	if stores.Falkor != nil {
		gengine, err := stores.GraphEngine(e.reg, fab.Upcasters())
		if err != nil {
			cancel()
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:graph", func(c context.Context) error { return gengine.Run(c, consumer) })
		}()
	}
	if stores.Elastic != nil {
		sengine, err := stores.SearchEngine(e.reg, fab.Upcasters())
		if err != nil {
			cancel()
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "proj:search", func(c context.Context) error { return sengine.Run(c, consumer) })
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
	} else if stores.Analytics != nil && stores.Redis != nil {
		if logger != nil {
			logger.Warn("fabriq: analytics is configured but no entity is marked for it; nothing will flow to the analytics sink")
		}
	}

	// The tracked-tenants gauge moves slowly; poll it off the pass path.
	if m != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case <-ticker.C:
					m.SweepTenantsTracked.Set(float64(engine.TrackedTenants()))
					if p, r, fo, ok := stores.CatalogReadStats(); ok {
						m.CatalogReadPrimary.Set(float64(p))
						m.CatalogReadReplica.Set(float64(r))
						m.CatalogReadFailover.Set(float64(fo))
					}
				}
			}
		}()
	}

	// The wake subscription turns write-path nudges into immediate passes.
	// Losing it is safe — the scan cadence still sweeps everyone.
	if stores.Redis != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			supervise(runCtx, logger, "sweeper:wake", func(c context.Context) error {
				return stores.Redis.SubscribeWakes(c, engine.Wake, nil)
			})
		}()
	}

	// Drift reconciler: leader-elected on the CATALOG control DB (no
	// primary shard exists to elect on). One scanner across replicas
	// iterates AllTenants and reconciles each tenant's projections against
	// the shared sinks. Same lock key as the static worker's reconciler.
	reconcileInterval := e.cfg.ReconcileInterval
	if reconcileInterval > 0 && (stores.Falkor != nil || stores.Elastic != nil) {
		reconElector := stores.Catalog.Elector(postgres.LockKeyReconciler)
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

	go func() {
		wg.Wait()
		close(done)
	}()
	return nil
}

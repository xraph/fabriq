package forgeext

import (
	"context"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// defaultRollupInterval is used when Config.RollupInterval is left at zero:
// unlike ReconcileInterval (opt-in, disabled at zero), the rollup:insights
// job defaults ON once Insights is enabled and at least one metric declares
// a Rollup, so materialized rollups stay current without extra config.
const defaultRollupInterval = time.Minute

// hasMaterializedMetric reports whether the registry declares at least one
// metric opting into materialization (MetricSpec.Rollup != nil) — the gate
// for the rollup:insights maintainer job, mirroring hasAnalyticsEntity/
// hasInsightsEntity's role for their own supervised jobs.
func hasMaterializedMetric(reg *registry.Registry) bool {
	return len(reg.MaterializedMetrics()) > 0
}

// runRollupMaintainer is the rollup:insights job body: once at start it
// ensures every materialized metric's rollup table exists on each
// statically-sharded physical database (idempotent DDL — catalog/db-per-
// tenant mode has no shared database to bootstrap here; EnsureRollupTable
// runs per tenant inside the tick below instead, equally idempotent), then
// on every tick it runs one maintainer pass: for each materialized metric,
// enumerate the tenants relevant to it (tenantsForMetric — deliberately NOT
// Stores.AllTenants; see that method's doc) and, under an UNSCOPED tenant
// ctx routed to that tenant's shard/database (Stores.RollupSurfaceFor — the
// same per-tenant routing proj:insights' FactSink uses), ensure the rollup
// table and run MaintainRollup. Errors are logged and isolated per (shard,
// tenant, metric) — one shard's, tenant's, or metric's failure never blocks
// another's, or the next tick.
//
// interval <= 0 disables the job (mirrors runReconciler/runBlobGC).
func (e *Extension) runRollupMaintainer(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	e.mu.Lock()
	stores := e.stores
	e.mu.Unlock()
	if stores == nil {
		return
	}
	var logger forge.Logger
	if app := e.App(); app != nil {
		logger = app.Logger()
	}

	metrics := e.reg.MaterializedMetrics()
	if len(metrics) == 0 {
		return
	}

	// Boot: bootstrap rollup tables on every statically-sharded physical
	// database up front, so the first tick's MaintainRollup calls never race
	// a missing table. Guarded by Shards != nil: ShardPGs() dereferences it,
	// and it is nil in catalog mode (no shared physical database to bootstrap
	// here — see the per-tenant EnsureRollupTable call in the tick below).
	if stores.Shards != nil {
		for _, sp := range stores.ShardPGs() {
			for _, m := range metrics {
				if err := sp.PG.EnsureRollupTable(ctx, m); err != nil && logger != nil {
					logger.Warn("fabriq: rollup:insights ensure-table failed at boot",
						forge.String("shard", sp.ID), forge.String("metric", m.Name), forge.Error(err))
				}
			}
		}
	}

	tick := func() { e.rollupPass(ctx, stores, logger, metrics) }
	tick()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}

// rollupPass runs one maintainer pass over every materialized metric, for
// every tenant relevant to it. Per-metric and per-(tenant, metric) error
// isolation: log and continue.
func (e *Extension) rollupPass(ctx context.Context, stores *fabriq.Stores, logger forge.Logger, metrics []*registry.MetricSpec) {
	now := time.Now()
	for _, m := range metrics {
		tenants := e.tenantsForMetric(ctx, stores, logger, m)
		for _, tid := range tenants {
			e.rollupOne(ctx, stores, logger, m, tid, now)
		}
	}
}

// tenantsForMetric enumerates the tenants relevant to materialized metric m.
// Deliberately NOT Stores.AllTenants uniformly — that seam has two different
// shapes depending on tenancy mode:
//
//   - catalog / db-per-tenant mode (Stores.Shards nil): AllTenants pages the
//     CATALOG's active entries — every tenant's own database, authoritative
//     regardless of what that tenant has or hasn't written. Safe and correct
//     to reuse as-is (a tenant with no matching events just gets a harmless
//     no-op EnsureRollupTable+MaintainRollup pass).
//   - static sharding: AllTenants unions each shard's fabriq_outbox-derived
//     projection bookkeeping (postgres.StateRepo.Tenants) — which MISSES any
//     tenant that only ever called Track/f.Analytics(), since Insights
//     events bypass the outbox entirely. So this instead queries each
//     shard's fabriq_insights_events directly, as OWNER (bypassing RLS —
//     the maintainer must see every tenant's rows to discover them, not just
//     one), for DISTINCT tenant_id WHERE name = m.Source
//     (postgres.Adapter.TenantsForInsightsEvent).
//
// One shard's enumeration failure is logged and skipped, not fatal to the
// others (mirrors rollupOne's per-tenant isolation one level up).
func (e *Extension) tenantsForMetric(ctx context.Context, stores *fabriq.Stores, logger forge.Logger, m *registry.MetricSpec) []string {
	if stores.Shards == nil {
		tenants, err := stores.AllTenants(ctx)
		if err != nil {
			if logger != nil {
				logger.Warn("fabriq: rollup:insights list tenants failed",
					forge.String("metric", m.Name), forge.Error(err))
			}
			return nil
		}
		return tenants
	}

	seen := map[string]struct{}{}
	var out []string
	for _, sp := range stores.ShardPGs() {
		ts, err := sp.PG.TenantsForInsightsEvent(ctx, m.Source)
		if err != nil {
			if logger != nil {
				logger.Warn("fabriq: rollup:insights list tenants failed on shard",
					forge.String("shard", sp.ID), forge.String("metric", m.Name), forge.Error(err))
			}
			continue
		}
		for _, t := range ts {
			if _, dup := seen[t]; !dup {
				seen[t] = struct{}{}
				out = append(out, t)
			}
		}
	}
	return out
}

// rollupOne runs EnsureRollupTable + MaintainRollup for one (tenant, metric)
// pair, under an unscoped tenant ctx routed to that tenant's shard/database.
// Any failure is logged and swallowed — the caller's loop must not stop for
// one tenant's or one metric's error.
func (e *Extension) rollupOne(ctx context.Context, stores *fabriq.Stores, logger forge.Logger, m *registry.MetricSpec, tenantID string, now time.Time) {
	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		if logger != nil {
			logger.Warn("fabriq: rollup:insights invalid tenant id",
				forge.String("tenant", tenantID), forge.Error(err))
		}
		return
	}
	rs, sctx, release, err := stores.RollupSurfaceFor(tctx, tenantID)
	if err != nil {
		if logger != nil {
			logger.Warn("fabriq: rollup:insights resolve tenant surface failed",
				forge.String("tenant", tenantID), forge.String("metric", m.Name), forge.Error(err))
		}
		return
	}
	defer release()

	// Idempotent — safe every pass; the boot loop above already covers
	// static sharding, this is what bootstraps catalog/db-per-tenant mode
	// (and any tenant provisioned after boot in static mode too).
	if err := rs.EnsureRollupTable(sctx, m); err != nil {
		if logger != nil {
			logger.Warn("fabriq: rollup:insights ensure-table failed",
				forge.String("tenant", tenantID), forge.String("metric", m.Name), forge.Error(err))
		}
		return
	}
	if err := rs.MaintainRollup(sctx, m, now); err != nil {
		if logger != nil {
			logger.Warn("fabriq: rollup:insights maintain failed",
				forge.String("tenant", tenantID), forge.String("metric", m.Name), forge.Error(err))
		}
	}
}

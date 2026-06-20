package forgeext

import (
	"context"
	"sync"
	"time"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/internal/metrics"
)

// The proj:distill worker maintains each tenant's context-distillation Merkle
// tree from write events. It is PAYLOAD-DRIVEN (like proj:embed): the L0 source
// values come straight off the event payload, so no store reload is needed and
// the worker never reads back the row it just observed.
//
// Per tenant it coalesces a burst of writes (latest-envelope-per-ref) behind a
// debounce timer; when the timer fires it runs a single-flight sweep
// (DistillEvent per ref, then one Rollup) serialized by a per-tenant gate. The
// sweep is best-effort and at-most-once: marks recorded but not yet swept are
// lost if the replica crashes between the consumer ack and the timer firing —
// the Phase-3 backfill reconcile heals that drift.

// hasDistillableEntity reports whether any registered entity opts into distillation.
func hasDistillableEntity(reg *registry.Registry) bool {
	for _, e := range reg.All() {
		if e.Spec.Distill != nil {
			return true
		}
	}
	return false
}

// tenantGate serializes work per tenant id: a tenant's sweep never overlaps
// another sweep for the same tenant, but distinct tenants proceed concurrently.
type tenantGate struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}

func newTenantGate() *tenantGate { return &tenantGate{m: map[string]*sync.Mutex{}} }

// Do runs fn while holding the tenant's lock. The map lock is held only long
// enough to look up (or create) the per-tenant lock, not while fn runs.
func (g *tenantGate) Do(tenantID string, fn func()) {
	g.mu.Lock()
	lk := g.m[tenantID]
	if lk == nil {
		lk = &sync.Mutex{}
		g.m[tenantID] = lk
	}
	g.mu.Unlock()
	lk.Lock()
	defer lk.Unlock()
	fn()
}

// ref identifies a source aggregate (entity + id) within a tenant.
type ref struct{ entity, id string }

// distillSweeper coalesces write events per tenant into debounced single-flight
// sweeps.
type distillSweeper struct {
	d        *agent.Distiller
	debounce time.Duration
	gate     *tenantGate
	m        *metrics.Metrics

	mu     sync.Mutex
	dirty  map[string]map[ref]event.Envelope // tenant -> ref -> LATEST envelope
	timers map[string]*time.Timer
}

// newDistillSweeper builds a sweeper over a Distiller. A non-positive debounce
// defaults to one second. When metrics are supplied, an observer adapter is
// attached to the Distiller so per-node counters increment from inside core.
func newDistillSweeper(d *agent.Distiller, debounce time.Duration, m *metrics.Metrics) *distillSweeper {
	if debounce <= 0 {
		debounce = time.Second
	}
	if m != nil {
		d.SetObserver(distillMetricsObserver{m})
	}
	return &distillSweeper{
		d: d, debounce: debounce, gate: newTenantGate(), m: m,
		dirty:  map[string]map[ref]event.Envelope{},
		timers: map[string]*time.Timer{},
	}
}

// MarkAndSchedule records the latest envelope for a ref and (re)arms the
// tenant's debounce timer. Latest-wins coalesces a burst of edits to the final
// payload, so a sweep summarizes each ref at most once per window.
func (s *distillSweeper) MarkAndSchedule(env event.Envelope) {
	r := ref{entity: env.Aggregate, id: env.AggID}
	s.mu.Lock()
	if s.dirty[env.TenantID] == nil {
		s.dirty[env.TenantID] = map[ref]event.Envelope{}
	}
	s.dirty[env.TenantID][r] = env
	if t := s.timers[env.TenantID]; t != nil {
		t.Stop()
	}
	tenantID := env.TenantID
	s.timers[env.TenantID] = time.AfterFunc(s.debounce, func() { s.sweep(tenantID) })
	s.mu.Unlock()
}

// sweep snapshots + clears the tenant's dirty set, then under the tenant gate
// distills each ref's latest envelope and runs one rollup. Each distill or
// rollup error increments DistillFailuresTotal but never aborts the batch.
func (s *distillSweeper) sweep(tenantID string) {
	s.mu.Lock()
	envs := s.dirty[tenantID]
	delete(s.dirty, tenantID)
	delete(s.timers, tenantID)
	s.mu.Unlock()
	if len(envs) == 0 {
		return
	}
	s.gate.Do(tenantID, func() {
		ctx, err := tenant.WithTenant(context.Background(), tenantID)
		if err != nil {
			return
		}
		for _, env := range envs {
			if _, derr := s.d.DistillEvent(ctx, env); derr != nil && s.m != nil {
				s.m.DistillFailuresTotal.Inc()
			}
		}
		if _, rerr := s.d.Rollup(ctx); rerr != nil && s.m != nil {
			s.m.DistillFailuresTotal.Inc()
		}
	})
}

// distillHandler is the per-event consumer callback: it records the latest
// envelope per distillable aggregate and schedules a debounced sweep. Tenant-less
// or non-distillable events are ack-skipped (returns nil). Marks lost to a crash
// before the sweep are healed by the Phase-3 backfill reconcile.
func distillHandler(_ context.Context, sw *distillSweeper) func(string, event.Envelope) error {
	return func(_ string, env event.Envelope) error {
		if env.TenantID == "" || env.Aggregate == "" {
			return nil
		}
		if sw.d.DistillSpecFor(env.Aggregate) == nil {
			return nil
		}
		sw.MarkAndSchedule(env)
		return nil
	}
}

// distillMetricsObserver maps the four DistillObserver callbacks onto the
// proj:distill Prometheus counters. It lives in the worker (not core) so
// core/agent stays free of the Prometheus dependency.
type distillMetricsObserver struct{ m *metrics.Metrics }

func (o distillMetricsObserver) Summarized()     { o.m.DistillSummariesTotal.Inc() }
func (o distillMetricsObserver) ShortCircuited() { o.m.DistillShortCircuitTotal.Inc() }
func (o distillMetricsObserver) NodeBuilt()      { o.m.DistillNodesTotal.Inc() }
func (o distillMetricsObserver) GuardBlocked()   { o.m.DistillGuardBlockedTotal.Inc() }

var _ agent.DistillObserver = distillMetricsObserver{}

package analytics

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/fabriq/core/event"
)

// Report summarizes one tenant's reconciliation: how many source aggregates
// were checked, how many were absent or behind in the analytics store (drift a
// skipped/poison event left), and how many were healed by re-applying.
type Report struct {
	Checked int `json:"checked"`
	Missing int `json:"missing"`
	Stale   int `json:"stale"`
	Healed  int `json:"healed"`
}

// Drifted is Missing + Stale — aggregates whose analytics state diverged from
// the source of truth.
func (r Report) Drifted() int { return r.Missing + r.Stale }

// Reconciler detects and heals divergence between the source databases (the
// truth) and the analytics store. The analytics consumer skips events it cannot
// upcast or apply (poison-avoidance), which can leave a fact permanently
// missing or stale — graph and search heal this from Postgres via their
// reconcilers; this is the analytics equivalent. It re-projects each marked
// aggregate's CURRENT source state and, comparing against the stored watermark,
// re-applies only the aggregates that are missing or behind.
type Reconciler struct {
	Snapshot SnapshotFunc
	Applier  *Applier
	Sink     Sink
}

// Tenant reconciles one tenant, returning what it found and healed.
func (rc *Reconciler) Tenant(ctx context.Context, tenantID string) (Report, error) {
	var rep Report

	wms, err := rc.Sink.AllWatermarks(ctx, tenantID)
	if err != nil {
		return rep, fmt.Errorf("fabriq: analytics reconcile watermarks %s: %w", tenantID, err)
	}
	have := make(map[string]int64, len(wms))
	for _, w := range wms {
		have[w.Aggregate+"\x00"+w.AggID] = w.Version
	}

	err = rc.Snapshot(ctx, tenantID, func(env event.Envelope) error {
		fact, ev, ok, aerr := rc.Applier.Apply(env)
		if aerr != nil || !ok {
			return aerr // skip unmarked (nil); a malformed source row aborts, fail loud
		}
		rep.Checked++
		key := env.Aggregate + "\x00" + env.AggID
		stored, present := have[key]
		switch {
		case !present:
			rep.Missing++
		case stored < env.Version:
			rep.Stale++
		default:
			return nil // analytics is current for this aggregate — nothing to heal
		}
		if err := rc.Sink.UpsertFacts(ctx, []Fact{fact}); err != nil {
			return err
		}
		if err := rc.Sink.AppendEvents(ctx, []Event{ev}); err != nil {
			return err
		}
		if err := rc.Sink.SetWatermark(ctx, []Watermark{{
			TenantID: env.TenantID, Aggregate: env.Aggregate, AggID: env.AggID, Version: env.Version,
		}}); err != nil {
			return err
		}
		rep.Healed++
		return nil
	})
	if err != nil {
		return rep, err
	}
	return rep, nil
}

// AllTenants reconciles each tenant with bounded concurrency, returning a report
// per tenant. One tenant's failure is recorded (first error) but does not abort
// the others. Concurrency <= 0 defaults to 4.
func (rc *Reconciler) AllTenants(ctx context.Context, tenants []string, concurrency int) (map[string]Report, error) {
	if concurrency <= 0 {
		concurrency = 4
	}
	sem := make(chan struct{}, concurrency)
	var mu sync.Mutex
	reports := make(map[string]Report, len(tenants))
	var firstErr error
	var wg sync.WaitGroup

	for _, tn := range tenants {
		wg.Add(1)
		sem <- struct{}{}
		go func(tn string) {
			defer wg.Done()
			defer func() { <-sem }()
			rep, err := rc.Tenant(ctx, tn)
			mu.Lock()
			reports[tn] = rep
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("fabriq: analytics reconcile tenant %s: %w", tn, err)
			}
			mu.Unlock()
		}(tn)
	}
	wg.Wait()
	return reports, firstErr
}

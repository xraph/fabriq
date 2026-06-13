package main

import (
	"context"
	"time"
)

// runReconciler is the scheduled drift healer: leader-elected (lock 1002)
// so exactly one replica scans, iterating every tenant that ever emitted
// an event. Interval 0 disables it (FABRIQ_RECONCILE_INTERVAL).
func (e *workerExtension) runReconciler(ctx context.Context, interval time.Duration) {
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

// runDocumentPlane is the materializer + compactor: every interval it
// materializes quiet documents (one ordinary versioned event each) and
// compacts logs past their SnapshotEvery budget. Leader-elected (1003).
func (e *workerExtension) runDocumentPlane(ctx context.Context, interval time.Duration) {
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
			e.mu.Lock()
			stores := e.stores
			e.mu.Unlock()
			if stores == nil {
				continue
			}
			_, _ = stores.Postgres.Documents().MaterializeQuiet(ctx, nil)
		}
	}
}

func (e *workerExtension) reconcileAll(ctx context.Context) {
	e.mu.Lock()
	stores := e.stores
	e.mu.Unlock()
	if stores == nil {
		return
	}
	tenants, err := stores.AllTenants(ctx) // union across shards; reconcilers route per tenant
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

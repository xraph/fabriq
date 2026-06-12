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

func (e *workerExtension) reconcileAll(ctx context.Context) {
	e.mu.Lock()
	stores := e.stores
	e.mu.Unlock()
	if stores == nil {
		return
	}
	tenants, err := stores.Postgres.ProjectionState().Tenants(ctx)
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

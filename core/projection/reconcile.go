package projection

import (
	"context"
	"fmt"
)

// PHASE 6 SCAFFOLD — drift reconciliation.
//
// Reconciler contract (normative; cron-style, leader-elected in
// fabriq-worker; `fabriq reconcile --tenant T [--repair]`):
//
//  1. For each tenant and projection, compare per-aggregate-type COUNTS
//     and MAX VERSIONS between Postgres (source of truth) and the
//     projection engine (graph node counts per label / index doc counts).
//  2. Narrow mismatches to aggregates by comparing stored versions
//     (projection node/doc version vs Postgres row version).
//  3. With --repair (or in the scheduled job): re-emit SYNTHETIC events
//     for drifted aggregates through the ordinary outbox (one
//     <entity>.updated at the current version), so the normal pipeline
//     heals the projection — reconciliation never writes engines directly.
//  4. Report drift counts through internal/metrics (projection drift
//     gauge) so silent corruption pages someone.
type Reconciler struct {
	State StateRepo
	// TODO(phase 6): count/max-version sources for postgres and each
	// engine, plus the synthetic re-emitter (outbox append outside the
	// command plane, version unchanged).
}

// Reconcile checks one tenant and optionally repairs drift.
func (r *Reconciler) Reconcile(ctx context.Context, tenantID string, repair bool) error {
	_ = ctx
	_ = tenantID
	_ = repair
	return fmt.Errorf("fabriq: reconciler not implemented yet (phase 6)")
}

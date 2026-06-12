package projection

import (
	"context"
	"fmt"
	"sort"

	"github.com/xraph/fabriq/core/registry"
)

// Drift is one aggregate whose projection disagrees with Postgres.
// TruthVersion 0 means the row no longer exists (the projection holds a
// zombie); ProjectedVersion 0 means the projection never saw it.
type Drift struct {
	Entity           string
	AggID            string
	TruthVersion     int64
	ProjectedVersion int64
}

// TruthVersions reads id->version for a tenant's entity from Postgres
// (the source of truth) — implemented by adapters/postgres.
type TruthVersions func(ctx context.Context, tenantID, entity string) (map[string]int64, error)

// ProjectedVersions reads id->version from a projection engine —
// implemented by the graph/search adapters.
type ProjectedVersions func(ctx context.Context, tenantID string, ent *registry.Entity) (map[string]int64, error)

// RepairFunc heals one drifted aggregate THROUGH THE NORMAL PIPELINE:
// republish the aggregate's latest event (missing/stale) or emit a
// synthetic deleted event (zombie) via the outbox — reconciliation never
// writes engines directly.
type RepairFunc func(ctx context.Context, tenantID string, d Drift) error

// Reconciler compares per-aggregate versions between Postgres and one
// projection and optionally repairs the differences. Run it scheduled
// (leader-elected in fabriq-worker) or on demand (`fabriq reconcile`).
//
// A projection AHEAD of the truth scan is not drift — events legitimately
// land between the two reads; the version-gated pipeline converges on its
// own.
type Reconciler struct {
	Projection string
	Registry   *registry.Registry
	Include    func(ent *registry.Entity) bool // which entities this projection carries
	Truth      TruthVersions
	Projected  ProjectedVersions
	Repair     RepairFunc
}

// Reconcile scans one tenant. With repair=false it only reports.
func (r *Reconciler) Reconcile(ctx context.Context, tenantID string, repair bool) ([]Drift, error) {
	if r.Registry == nil || r.Truth == nil || r.Projected == nil {
		return nil, fmt.Errorf("fabriq: reconciler %q not fully wired", r.Projection)
	}
	var drifts []Drift
	for _, ent := range r.Registry.All() {
		if ent.Spec.Kind != registry.KindAggregate {
			continue
		}
		if r.Include != nil && !r.Include(ent) {
			continue
		}
		truth, err := r.Truth(ctx, tenantID, ent.Spec.Name)
		if err != nil {
			return nil, fmt.Errorf("fabriq: reconcile %s truth: %w", ent.Spec.Name, err)
		}
		projected, err := r.Projected(ctx, tenantID, ent)
		if err != nil {
			return nil, fmt.Errorf("fabriq: reconcile %s projection: %w", ent.Spec.Name, err)
		}

		for id, tv := range truth {
			pv := projected[id]
			if pv < tv { // missing (0) or stale
				drifts = append(drifts, Drift{Entity: ent.Spec.Name, AggID: id, TruthVersion: tv, ProjectedVersion: pv})
			}
		}
		for id, pv := range projected {
			if _, exists := truth[id]; !exists { // zombie
				drifts = append(drifts, Drift{Entity: ent.Spec.Name, AggID: id, TruthVersion: 0, ProjectedVersion: pv})
			}
		}
	}

	sort.Slice(drifts, func(i, j int) bool {
		if drifts[i].Entity != drifts[j].Entity {
			return drifts[i].Entity < drifts[j].Entity
		}
		return drifts[i].AggID < drifts[j].AggID
	})

	if repair && r.Repair != nil {
		for _, d := range drifts {
			if err := r.Repair(ctx, tenantID, d); err != nil {
				return drifts, fmt.Errorf("fabriq: repair %s/%s: %w", d.Entity, d.AggID, err)
			}
		}
	}
	return drifts, nil
}

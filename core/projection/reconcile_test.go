package projection_test

import (
	"context"
	"sort"
	"testing"

	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
)

func reconciler(t *testing.T, truth map[string]map[string]int64, proj map[string]map[string]int64) (*projection.Reconciler, *[]projection.Drift) {
	t.Helper()
	reg := testRegistry(t) // site + asset, both graph-mapped
	repaired := &[]projection.Drift{}
	r := &projection.Reconciler{
		Projection: "graph",
		Registry:   reg,
		Include:    func(ent *registry.Entity) bool { return ent.Spec.GraphNode != "" },
		Truth: func(_ context.Context, _, entity string) (map[string]int64, error) {
			return truth[entity], nil
		},
		Projected: func(_ context.Context, _ string, ent *registry.Entity) (map[string]int64, error) {
			return proj[ent.Spec.Name], nil
		},
		Repair: func(_ context.Context, _ string, d projection.Drift) error {
			*repaired = append(*repaired, d)
			return nil
		},
	}
	return r, repaired
}

func TestReconcile_NoDriftIsQuiet(t *testing.T) {
	r, repaired := reconciler(t,
		map[string]map[string]int64{"asset": {"A1": 3}, "site": {"S1": 1}},
		map[string]map[string]int64{"asset": {"A1": 3}, "site": {"S1": 1}},
	)
	drifts, err := r.Reconcile(context.Background(), "acme", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(drifts) != 0 || len(*repaired) != 0 {
		t.Fatalf("clean state reported drift: %v", drifts)
	}
}

func TestReconcile_DetectsMissingStaleAndZombie(t *testing.T) {
	r, repaired := reconciler(t,
		map[string]map[string]int64{
			"asset": {"A1": 3, "A2": 5}, // A1 stale in proj, A2 missing
			"site":  {},
		},
		map[string]map[string]int64{
			"asset": {"A1": 2},
			"site":  {"GHOST": 4}, // zombie: not in Postgres
		},
	)
	drifts, err := r.Reconcile(context.Background(), "acme", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(drifts) != 3 || len(*repaired) != 3 {
		t.Fatalf("drifts = %+v", drifts)
	}
	byKey := map[string]projection.Drift{}
	for _, d := range drifts {
		byKey[d.Entity+"/"+d.AggID] = d
	}
	if d := byKey["asset/A1"]; d.TruthVersion != 3 || d.ProjectedVersion != 2 {
		t.Fatalf("stale drift = %+v", d)
	}
	if d := byKey["asset/A2"]; d.TruthVersion != 5 || d.ProjectedVersion != 0 {
		t.Fatalf("missing drift = %+v", d)
	}
	if d := byKey["site/GHOST"]; d.TruthVersion != 0 || d.ProjectedVersion != 4 {
		t.Fatalf("zombie drift = %+v", d)
	}
}

func TestReconcile_DryRunDetectsWithoutRepair(t *testing.T) {
	r, repaired := reconciler(t,
		map[string]map[string]int64{"asset": {"A1": 3}, "site": {}},
		map[string]map[string]int64{"asset": {}, "site": {}},
	)
	drifts, err := r.Reconcile(context.Background(), "acme", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(drifts) != 1 || len(*repaired) != 0 {
		t.Fatalf("dry run: drifts=%v repaired=%v", drifts, *repaired)
	}
}

func TestReconcile_ProjectionAheadIsNotDrift(t *testing.T) {
	// The projection being AHEAD of a lagging truth read (event applied
	// between the two scans) must not trigger pointless repairs.
	r, repaired := reconciler(t,
		map[string]map[string]int64{"asset": {"A1": 3}, "site": {}},
		map[string]map[string]int64{"asset": {"A1": 4}, "site": {}},
	)
	drifts, err := r.Reconcile(context.Background(), "acme", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(drifts) != 0 || len(*repaired) != 0 {
		t.Fatalf("ahead projection flagged as drift: %v", drifts)
	}
}

func TestReconcile_OrderIsDeterministic(t *testing.T) {
	r, _ := reconciler(t,
		map[string]map[string]int64{"asset": {"B": 1, "A": 1, "C": 1}, "site": {}},
		map[string]map[string]int64{"asset": {}, "site": {}},
	)
	drifts, err := r.Reconcile(context.Background(), "acme", false)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(drifts))
	for i, d := range drifts {
		ids[i] = d.AggID
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatalf("drift order not deterministic: %v", ids)
	}
}

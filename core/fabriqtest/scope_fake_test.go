package fabriqtest

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

func TestFakeVector_ScopeSoftFilter(t *testing.T) {
	f := &FakeVector{data: map[string]map[string]map[string]vecEntry{}}
	base, _ := tenant.WithTenant(context.Background(), "ws_1")
	a, _ := tenant.WithScope(base, "proj_A")
	b, _ := tenant.WithScope(base, "proj_B")
	_ = f.Upsert(a, "node", "ra", []float32{1, 0, 0}, nil)
	_ = f.Upsert(b, "node", "rb", []float32{1, 0, 0}, nil)
	_ = f.Upsert(base, "node", "shared", []float32{1, 0, 0}, nil)
	var got []query.VectorMatch
	_ = f.Similar(a, query.VectorQuery{Entity: "node", Embedding: []float32{1, 0, 0}, K: 10}, &got)
	ids := map[string]bool{}
	for _, m := range got {
		ids[m.ID] = true
	}
	if !ids["ra"] || !ids["shared"] || ids["rb"] {
		t.Fatalf("scoped read want {ra,shared}, got %v", ids)
	}
	var all []query.VectorMatch
	_ = f.Similar(base, query.VectorQuery{Entity: "node", Embedding: []float32{1, 0, 0}, K: 10}, &all)
	if len(all) != 3 {
		t.Fatalf("unscoped read want 3, got %d", len(all))
	}
}

func TestFakeSpatial_ScopeSoftFilter(t *testing.T) {
	f := &FakeSpatial{data: map[string]map[string]map[string]geoEntry{}}
	base, _ := tenant.WithTenant(context.Background(), "ws_1")
	a, _ := tenant.WithScope(base, "proj_A")
	b, _ := tenant.WithScope(base, "proj_B")
	_ = f.Upsert(a, "node", "ra", query.Geometry{WKT: "POINT (0 0)", SRID: 0}, nil)
	_ = f.Upsert(b, "node", "rb", query.Geometry{WKT: "POINT (0 0)", SRID: 0}, nil)
	_ = f.Upsert(base, "node", "shared", query.Geometry{WKT: "POINT (0 0)", SRID: 0}, nil)

	var got []query.SpatialMatch
	_ = f.Within(a, query.SpatialQuery{
		Entity: "node", Center: query.Geometry{WKT: "POINT (0 0)", SRID: 0}, RadiusM: 1, K: 10,
	}, &got)
	ids := map[string]bool{}
	for _, m := range got {
		ids[m.ID] = true
	}
	if !ids["ra"] || !ids["shared"] || ids["rb"] {
		t.Fatalf("scoped spatial read want {ra,shared}, got %v", ids)
	}

	var all []query.SpatialMatch
	_ = f.Within(base, query.SpatialQuery{
		Entity: "node", Center: query.Geometry{WKT: "POINT (0 0)", SRID: 0}, RadiusM: 1, K: 10,
	}, &all)
	if len(all) != 3 {
		t.Fatalf("unscoped spatial read want 3, got %d", len(all))
	}
}

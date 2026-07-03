package fabriqtest

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
)

func TestFakeSpatial_WithinNearestFirst(t *testing.T) {
	f := &FakeSpatial{data: map[string]map[string]map[string]geoEntry{}}
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	mustUp := func(id, wkt string) {
		if err := f.Upsert(ctx, "node", id, query.Geometry{WKT: wkt, SRID: 0}, nil); err != nil {
			t.Fatal(err)
		}
	}
	mustUp("origin", "POINT Z (0 0 0)")
	mustUp("near", "POINT Z (3 4 0)")  // dist 5
	mustUp("far", "POINT Z (30 40 0)") // dist 50

	var got []query.SpatialMatch
	if err := f.Within(ctx, query.SpatialQuery{
		Entity: "node", Center: query.Geometry{WKT: "POINT Z (0 0 0)", SRID: 0}, RadiusM: 10, K: 5,
	}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "origin" || got[1].ID != "near" {
		t.Fatalf("want [origin,near] nearest-first, got %#v", got)
	}
	if got[1].DistanceM < 4.99 || got[1].DistanceM > 5.01 {
		t.Fatalf("near distance want ~5, got %v", got[1].DistanceM)
	}
}

func TestFakeSpatial_Delete(t *testing.T) {
	f := &FakeSpatial{data: map[string]map[string]map[string]geoEntry{}}
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	_ = f.Upsert(ctx, "node", "a", query.Geometry{WKT: "POINT (1 1)", SRID: 0}, nil)
	_ = f.Delete(ctx, "node", "a")
	var got []query.SpatialMatch
	_ = f.Within(ctx, query.SpatialQuery{Entity: "node", Center: query.Geometry{WKT: "POINT (1 1)", SRID: 0}, RadiusM: 100, K: 5}, &got)
	if len(got) != 0 {
		t.Fatalf("deleted geometry must not match, got %#v", got)
	}
}

func TestFakeSpatial_Get(t *testing.T) {
	f := &FakeSpatial{data: map[string]map[string]map[string]geoEntry{}}
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	if err := f.Upsert(ctx, "site", "s1", query.Geometry{WKT: "POINT (-122.42 37.77)", SRID: 4326}, map[string]any{"name": "Plant A"}); err != nil {
		t.Fatal(err)
	}
	geom, meta, ok, err := f.Get(ctx, "site", "s1")
	if err != nil || !ok {
		t.Fatalf("want ok, got ok=%v err=%v", ok, err)
	}
	if geom.SRID != 4326 || meta["name"] != "Plant A" {
		t.Fatalf("unexpected geom/meta: %+v %+v", geom, meta)
	}
	// A point must round-trip through Within as a valid center.
	var got []query.SpatialMatch
	if err := f.Within(ctx, query.SpatialQuery{Entity: "site", Center: geom, RadiusM: 1000, K: 5}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "s1" {
		t.Fatalf("want [s1], got %#v", got)
	}
	// Missing id → ok=false, nil error.
	_, _, ok, err = f.Get(ctx, "site", "nope")
	if err != nil || ok {
		t.Fatalf("missing id want ok=false nil err, got ok=%v err=%v", ok, err)
	}
}

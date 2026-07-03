//go:build integration

package postgres_test

import (
	"context"
	"math"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// newSpatialHarness boots a container, runs all migrations (which install
// PostGIS via 0011 if the extension is available), and returns a
// SpatialAdapter backed by a restricted-role Adapter (so RLS applies).
// If fabriq_geometries does not exist after migrations (PostGIS absent in the
// container), the test is skipped — mirrors the extension-guarded migration.
func newSpatialHarness(t *testing.T) *postgres.SpatialAdapter {
	t.Helper()

	superDSN := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	// Run migrations as superuser (same pattern as newHarness).
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	orch, err := migrations.NewOrchestrator(owner.Driver())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Check whether the PostGIS migration actually created the table.
	// If PostGIS is absent in the container, the migration skips it silently
	// and the table does not exist — skip the test cleanly.
	var tableExists bool
	row := owner.Driver().QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'fabriq_geometries'
		)`)
	if err := row.Scan(&tableExists); err != nil {
		_ = owner.Close()
		t.Fatalf("check fabriq_geometries: %v", err)
	}
	_ = owner.Close()
	if !tableExists {
		t.Skip("fabriq_geometries not present: PostGIS unavailable in this Postgres image")
	}

	// App-role adapter (RLS actually applies — superusers bypass it).
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	a, err := postgres.Open(ctx, appDSN, reg, postgres.WithGuardedTables(domain.ReadingsSeries))
	if err != nil {
		t.Fatalf("postgres.Open (app): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	return postgres.NewSpatialAdapter(a)
}

// spatialCtx returns a tenant-stamped context for the given tenant ID.
func spatialCtx(t testing.TB, tenantID string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("WithTenant(%q): %v", tenantID, err)
	}
	return ctx
}

// TestSpatial_LocalPlanarNearestFirst verifies GiST radius search + nearest-
// first ordering for SRID 0 (planar, units are the same as the coordinate
// system — here metres by convention).
func TestSpatial_LocalPlanarNearestFirst(t *testing.T) {
	s := newSpatialHarness(t)
	ctx := spatialCtx(t, "acme")

	// A at origin (dist 0), B at (3,4,0) (dist 5), C at (30,40,0) (dist 50).
	for _, tc := range []struct {
		id  string
		wkt string
	}{
		{"A", "POINT Z (0 0 0)"},
		{"B", "POINT Z (3 4 0)"},
		{"C", "POINT Z (30 40 0)"},
	} {
		if err := s.Upsert(ctx, "node", tc.id, query.Geometry{WKT: tc.wkt, SRID: 0}, nil); err != nil {
			t.Fatalf("Upsert %s: %v", tc.id, err)
		}
	}

	var got []query.SpatialMatch
	if err := s.Within(ctx, query.SpatialQuery{
		Entity:  "node",
		Center:  query.Geometry{WKT: "POINT Z (0 0 0)", SRID: 0},
		RadiusM: 10,
		K:       5,
	}, &got); err != nil {
		t.Fatalf("Within: %v", err)
	}

	// Exactly A and B; C (dist 50) must be excluded.
	if len(got) != 2 {
		t.Fatalf("Within returned %d results, want 2: %+v", len(got), got)
	}
	if got[0].ID != "A" {
		t.Errorf("got[0].ID = %q, want \"A\"", got[0].ID)
	}
	if got[1].ID != "B" {
		t.Errorf("got[1].ID = %q, want \"B\"", got[1].ID)
	}

	// A is at the center: distance must be 0 (or negligibly small).
	if got[0].DistanceM > 0.01 {
		t.Errorf("A distance = %v, want ~0", got[0].DistanceM)
	}
	// B is at Pythagorean distance 5 in SRID 0 planar units.
	if math.Abs(got[1].DistanceM-5.0) > 0.01 {
		t.Errorf("B distance = %v, want ~5 (±0.01)", got[1].DistanceM)
	}
}

// TestSpatial_GeographicTrueMetres verifies that SRID 4326 queries use the
// geography cast so distances are in true metres on the WGS-84 ellipsoid.
// SF (-122.4194 37.7749) and a point ~1.4 km to the north (-122.4194 37.7876).
// True great-circle distance ≈ 1413 m; we include it with a 2000 m radius and
// exclude a far point 200 km away.
func TestSpatial_GeographicTrueMetres(t *testing.T) {
	s := newSpatialHarness(t)
	ctx := spatialCtx(t, "acme")

	for _, tc := range []struct {
		id  string
		wkt string
	}{
		{"SF", "POINT (-122.4194 37.7749)"},
		{"near", "POINT (-122.4194 37.7876)"},
		{"far", "POINT (-120.6394 37.7749)"},
	} {
		if err := s.Upsert(ctx, "geo", tc.id, query.Geometry{WKT: tc.wkt, SRID: 4326}, nil); err != nil {
			t.Fatalf("Upsert %s: %v", tc.id, err)
		}
	}

	var got []query.SpatialMatch
	if err := s.Within(ctx, query.SpatialQuery{
		Entity:  "geo",
		Center:  query.Geometry{WKT: "POINT (-122.4194 37.7749)", SRID: 4326},
		RadiusM: 2000, // 2 km — includes SF (0 m) and near (~1413 m), excludes far (~200 km).
		K:       5,
	}, &got); err != nil {
		t.Fatalf("Within geo: %v", err)
	}

	// Must return SF (at center, dist 0) and near (~1413 m); far excluded.
	if len(got) != 2 {
		t.Fatalf("geo Within returned %d results, want 2: %+v", len(got), got)
	}
	// Nearest-first: SF is at the center.
	if got[0].ID != "SF" {
		t.Errorf("got[0].ID = %q, want \"SF\"", got[0].ID)
	}
	if got[1].ID != "near" {
		t.Errorf("got[1].ID = %q, want \"near\"", got[1].ID)
	}

	// SF is the query center: distance should be ~0 m.
	if got[0].DistanceM > 1.0 {
		t.Errorf("SF distance = %v m, want ~0", got[0].DistanceM)
	}

	// near is ~1413 m away; accept ±5% tolerance (~71 m).
	const trueMetres = 1413.0
	const tol = trueMetres * 0.05
	if math.Abs(got[1].DistanceM-trueMetres) > tol {
		t.Errorf("near distance = %v m, want %v±%v m (5%% of true great-circle distance)",
			got[1].DistanceM, trueMetres, tol)
	}
}

// TestSpatial_Delete verifies that a deleted geometry no longer appears in
// Within results.
func TestSpatial_Delete(t *testing.T) {
	s := newSpatialHarness(t)
	ctx := spatialCtx(t, "acme")

	for _, tc := range []struct {
		id  string
		wkt string
	}{
		{"A", "POINT Z (0 0 0)"},
		{"B", "POINT Z (3 4 0)"},
	} {
		if err := s.Upsert(ctx, "node", tc.id, query.Geometry{WKT: tc.wkt, SRID: 0}, nil); err != nil {
			t.Fatalf("Upsert %s: %v", tc.id, err)
		}
	}

	if err := s.Delete(ctx, "node", "B"); err != nil {
		t.Fatalf("Delete B: %v", err)
	}

	var got []query.SpatialMatch
	if err := s.Within(ctx, query.SpatialQuery{
		Entity:  "node",
		Center:  query.Geometry{WKT: "POINT Z (0 0 0)", SRID: 0},
		RadiusM: 100,
		K:       5,
	}, &got); err != nil {
		t.Fatalf("Within after delete: %v", err)
	}

	for _, m := range got {
		if m.ID == "B" {
			t.Errorf("deleted geometry B still returned by Within: %+v", got)
		}
	}
	if len(got) != 1 || got[0].ID != "A" {
		t.Errorf("After deleting B, want only [A], got %+v", got)
	}
}

// TestSpatial_RLSTenantIsolation verifies that rows written under tenant "t2"
// are invisible to queries under tenant "acme", and vice-versa.
func TestSpatial_RLSTenantIsolation(t *testing.T) {
	s := newSpatialHarness(t)
	acme := spatialCtx(t, "acme")
	t2 := spatialCtx(t, "t2")

	// Write a geometry under acme.
	if err := s.Upsert(acme, "node", "acme-pt", query.Geometry{WKT: "POINT (1 1)", SRID: 0}, nil); err != nil {
		t.Fatalf("Upsert acme-pt: %v", err)
	}

	// Write a geometry under t2.
	if err := s.Upsert(t2, "node", "t2-pt", query.Geometry{WKT: "POINT (1 1)", SRID: 0}, nil); err != nil {
		t.Fatalf("Upsert t2-pt: %v", err)
	}

	// acme searches — must not see t2's rows.
	var acmeGot []query.SpatialMatch
	if err := s.Within(acme, query.SpatialQuery{
		Entity:  "node",
		Center:  query.Geometry{WKT: "POINT (1 1)", SRID: 0},
		RadiusM: 1000,
		K:       50,
	}, &acmeGot); err != nil {
		t.Fatalf("Within acme: %v", err)
	}
	for _, m := range acmeGot {
		if m.ID == "t2-pt" {
			t.Errorf("t2 row leaked into acme results: %+v", acmeGot)
		}
	}
	foundAcme := false
	for _, m := range acmeGot {
		if m.ID == "acme-pt" {
			foundAcme = true
		}
	}
	if !foundAcme {
		t.Errorf("acme-pt missing from acme results: %+v", acmeGot)
	}

	// t2 searches — must not see acme's rows.
	var t2Got []query.SpatialMatch
	if err := s.Within(t2, query.SpatialQuery{
		Entity:  "node",
		Center:  query.Geometry{WKT: "POINT (1 1)", SRID: 0},
		RadiusM: 1000,
		K:       50,
	}, &t2Got); err != nil {
		t.Fatalf("Within t2: %v", err)
	}
	for _, m := range t2Got {
		if m.ID == "acme-pt" {
			t.Errorf("acme row leaked into t2 results: %+v", t2Got)
		}
	}
	foundT2 := false
	for _, m := range t2Got {
		if m.ID == "t2-pt" {
			foundT2 = true
		}
	}
	if !foundT2 {
		t.Errorf("t2-pt missing from t2 results: %+v", t2Got)
	}
}

// TestSpatialAdapter_GetAndFilter verifies Get round-trips geometry+meta (and
// reports ok=false for a missing id), and that Within's Filter narrows
// results to rows whose meta matches every k=v predicate.
func TestSpatialAdapter_GetAndFilter(t *testing.T) {
	s := newSpatialHarness(t)
	ctx := spatialCtx(t, "acme")
	up := func(id, wkt, tag string) {
		if err := s.Upsert(ctx, "equipment", id, query.Geometry{WKT: wkt, SRID: 4326}, map[string]any{"tag": tag}); err != nil {
			t.Fatal(err)
		}
	}
	up("pump1", "POINT(-122.42 37.77)", "pump")
	up("valve1", "POINT(-122.4201 37.7701)", "valve")

	geom, meta, ok, err := s.Get(ctx, "equipment", "pump1")
	if err != nil || !ok || geom.SRID != 4326 || meta["tag"] != "pump" {
		t.Fatalf("Get pump1: geom=%+v meta=%+v ok=%v err=%v", geom, meta, ok, err)
	}
	if _, _, ok, _ := s.Get(ctx, "equipment", "missing"); ok {
		t.Fatal("missing id must be ok=false")
	}

	var got []query.SpatialMatch
	if err := s.Within(ctx, query.SpatialQuery{
		Entity: "equipment", Center: query.Geometry{WKT: "POINT(-122.42 37.77)", SRID: 4326},
		RadiusM: 5000, K: 10, Filter: map[string]string{"tag": "pump"},
	}, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "pump1" {
		t.Fatalf("filtered within want [pump1], got %#v", got)
	}
}

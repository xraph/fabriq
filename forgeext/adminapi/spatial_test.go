package adminapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

// placeSpec returns a dynamic "place" entity for spatial tests: a couple of text
// columns so hydration returns real row data alongside each geometry match.
func placeSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: "place", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_places",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "city", Type: registry.ColText},
			},
		},
	}
}

// seedPlace is one fixture: a name/city row plus its WGS84 coordinate.
type seedPlace struct {
	name, city string
	lng, lat   float64
}

// buildSpatialWorld registers the place entity, seeds the given fixtures as
// relational rows AND upserts each into the FakeSpatial store (SRID 4326, with
// lng/lat recorded in meta), all under the test tenant. It returns the World and
// a name→id map.
func buildSpatialWorld(t *testing.T, places []seedPlace) (*fabriqtest.World, map[string]string) {
	t.Helper()

	reg := registry.New()
	if err := reg.Register(placeSpec()); err != nil {
		t.Fatalf("register place: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}

	world := fabriqtest.NewWorld(reg)
	exec, err := command.NewExecutor(reg, world.Store)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	ctx, err := tenant.WithTenant(t.Context(), testTenantID)
	if err != nil {
		t.Fatalf("with tenant: %v", err)
	}

	ids := map[string]string{}
	for _, p := range places {
		res, execErr := exec.Exec(ctx, command.Command{
			Entity: "place", Op: command.OpCreate,
			Payload: map[string]any{"name": p.name, "city": p.city},
		})
		if execErr != nil {
			t.Fatalf("seed place %q: %v", p.name, execErr)
		}
		id := res.AggID
		ids[p.name] = id

		geom := query.Geometry{WKT: pointWKT(p.lng, p.lat), SRID: 4326}
		meta := map[string]any{"name": p.name, "city": p.city, "lng": p.lng, "lat": p.lat}
		if upErr := world.Spatial.Upsert(ctx, "place", id, geom, meta); upErr != nil {
			t.Fatalf("spatial upsert %q: %v", p.name, upErr)
		}
	}
	return world, ids
}

// pointWKT renders a WGS84 point WKT (the FakeSpatial WKT parser is tolerant of
// formatting; the test only needs parseable "POINT(lng lat)").
func pointWKT(lng, lat float64) string {
	return fmt.Sprintf("POINT(%v %v)", lng, lat)
}

// postSpatial issues a POST to the spatial within route with a JSON body and the
// test tenant header stamped.
func postSpatial(t *testing.T, srv *httptest.Server, body any) *http.Response {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/spatial/within", bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(testTenantHeader, testTenantID)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// sfFixtures are three points: two close together in San Francisco and one far
// away in Tokyo, so a tight radius around SF returns only the SF pair.
var sfFixtures = []seedPlace{
	{name: "Ferry Building", city: "San Francisco", lng: -122.3933, lat: 37.7955},
	{name: "Coit Tower", city: "San Francisco", lng: -122.4058, lat: 37.8024},
	{name: "Tokyo Tower", city: "Tokyo", lng: 139.7454, lat: 35.6586},
}

// TestSpatialWithin_RealFake exercises the radius search against FakeSpatial: a
// 50km radius centred on SF returns the two SF points (nearest-first), echoes
// their lng/lat from meta, hydrates their relational rows, and excludes Tokyo.
func TestSpatialWithin_RealFake(t *testing.T) {
	world, ids := buildSpatialWorld(t, sfFixtures)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postSpatial(t, srv, map[string]any{
		"entity": "place", "lng": -122.42, "lat": 37.77, "radiusM": 50000, "limit": 10,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got spatialWithinResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Matches) != 2 {
		t.Fatalf("matches len = %d, want 2 (SF pair, Tokyo excluded): %+v", len(got.Matches), got.Matches)
	}
	// Nearest-first: the Ferry Building is closer to the (-122.42,37.77) centre
	// than Coit Tower.
	if got.Matches[0].ID != ids["Ferry Building"] {
		t.Errorf("nearest id = %q, want Ferry Building %q", got.Matches[0].ID, ids["Ferry Building"])
	}
	if got.Matches[0].DistanceM <= 0 {
		t.Errorf("distanceM = %v, want > 0", got.Matches[0].DistanceM)
	}
	// Distances must be ascending.
	if got.Matches[0].DistanceM > got.Matches[1].DistanceM {
		t.Errorf("matches not nearest-first: %v > %v", got.Matches[0].DistanceM, got.Matches[1].DistanceM)
	}
	// lng/lat echoed from meta.
	if got.Matches[0].Lng == nil || got.Matches[0].Lat == nil {
		t.Fatalf("expected lng/lat echoed from meta, got %+v", got.Matches[0])
	}
	if *got.Matches[0].Lng != -122.3933 || *got.Matches[0].Lat != 37.7955 {
		t.Errorf("coords = (%v,%v), want (-122.3933,37.7955)", *got.Matches[0].Lng, *got.Matches[0].Lat)
	}
	// Data hydrated from the relational source of truth.
	if got.Matches[0].Data["name"] != "Ferry Building" {
		t.Errorf("hydrated name = %v, want Ferry Building", got.Matches[0].Data["name"])
	}
}

// TestSpatialWithin_TightRadius verifies a small radius returns fewer (or no)
// matches: a 100m circle around the SF centre catches neither SF point.
func TestSpatialWithin_TightRadius(t *testing.T) {
	world, _ := buildSpatialWorld(t, sfFixtures)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postSpatial(t, srv, map[string]any{
		"entity": "place", "lng": -122.42, "lat": 37.77, "radiusM": 100,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	var got spatialWithinResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Matches) != 0 {
		t.Fatalf("matches len = %d, want 0 for a 100m radius: %+v", len(got.Matches), got.Matches)
	}
}

// TestSpatialWithin_MissingEntity verifies omitting entity returns 400.
func TestSpatialWithin_MissingEntity(t *testing.T) {
	world, _ := buildSpatialWorld(t, sfFixtures)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postSpatial(t, srv, map[string]any{"lng": -122.42, "lat": 37.77, "radiusM": 1000})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestSpatialWithin_MissingLng verifies omitting lng returns 400 (a bare-missing
// field, distinct from an explicit 0).
func TestSpatialWithin_MissingLng(t *testing.T) {
	world, _ := buildSpatialWorld(t, sfFixtures)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postSpatial(t, srv, map[string]any{"entity": "place", "lat": 37.77, "radiusM": 1000})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestSpatialWithin_MissingLat verifies omitting lat returns 400.
func TestSpatialWithin_MissingLat(t *testing.T) {
	world, _ := buildSpatialWorld(t, sfFixtures)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postSpatial(t, srv, map[string]any{"entity": "place", "lng": -122.42, "radiusM": 1000})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestSpatialWithin_MissingRadius verifies a non-positive radius returns 400.
func TestSpatialWithin_MissingRadius(t *testing.T) {
	world, _ := buildSpatialWorld(t, sfFixtures)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postSpatial(t, srv, map[string]any{"entity": "place", "lng": -122.42, "lat": 37.77})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestSpatialWithin_ZeroCoordsHonored verifies an explicit lng:0/lat:0 (the
// null island) is treated as a real coordinate, not a missing field — the
// request reaches the query path and returns 200 (no points there, so empty).
func TestSpatialWithin_ZeroCoordsHonored(t *testing.T) {
	world, _ := buildSpatialWorld(t, sfFixtures)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postSpatial(t, srv, map[string]any{"entity": "place", "lng": 0, "lat": 0, "radiusM": 1000})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200 (explicit 0 coords honored), body = %s", resp.StatusCode, body)
	}
}

// TestMeta_SpatialCapability verifies the spatial.read capability slug is advertised.
func TestMeta_SpatialCapability(t *testing.T) {
	world, _ := buildSpatialWorld(t, sfFixtures)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/meta")
	defer resp.Body.Close()

	var got metaResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	found := false
	for _, c := range got.Capabilities {
		if c == "spatial.read" {
			found = true
		}
	}
	if !found {
		t.Error("capability \"spatial.read\" not advertised")
	}
}

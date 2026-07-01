package main

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// placeEntity is the demo dynamic entity that carries geometry. Each row is a
// relational record (name/category/city) AND a point in the spatial plane
// (fabriq_geometries via Spatial().Upsert), so the SPA can run radius queries.
const placeEntity = "place"

// placeCount is the number of place rows seeded per tenant. ~12 points spread
// across a handful of real-world cities so a radius query returns an interesting
// subset (a couple near the centre, the rest excluded).
const placeCount = 12

// place is one seed fixture: a name + category + city, anchored at a real-world
// WGS84 coordinate. The coordinate goes into the spatial plane (SRID 4326) and
// is echoed back through the match meta by the adminapi spatial endpoint.
type place struct {
	name, category, city string
	lng, lat             float64
}

// acmePlaces are acme-corp's 12 points, concentrated in San Francisco and New
// York (with a couple of outliers) so a radius around SF returns the SF cluster
// and excludes the rest.
var acmePlaces = []place{
	{"Ferry Building", "landmark", "San Francisco", -122.3933, 37.7955},
	{"Coit Tower", "landmark", "San Francisco", -122.4058, 37.8024},
	{"Golden Gate Park", "park", "San Francisco", -122.4862, 37.7694},
	{"Oracle Park", "stadium", "San Francisco", -122.3892, 37.7786},
	{"Mission Dolores", "landmark", "San Francisco", -122.4269, 37.7642},
	{"Palace of Fine Arts", "landmark", "San Francisco", -122.4485, 37.8030},
	{"Times Square", "landmark", "New York", -73.9855, 40.7580},
	{"Central Park", "park", "New York", -73.9654, 40.7829},
	{"Brooklyn Bridge", "landmark", "New York", -73.9969, 40.7061},
	{"Yankee Stadium", "stadium", "New York", -73.9262, 40.8296},
	{"Statue of Liberty", "landmark", "New York", -74.0445, 40.6892},
	{"The High Line", "park", "New York", -74.0048, 40.7480},
}

// globexPlaces are globex's 12 points, in a DIFFERENT set of cities (London,
// Berlin, Tokyo) so tenant isolation is visible: a radius around SF returns
// matches for acme-corp but nothing for globex.
var globexPlaces = []place{
	{"Big Ben", "landmark", "London", -0.1246, 51.5007},
	{"Tower Bridge", "landmark", "London", -0.0754, 51.5055},
	{"Hyde Park", "park", "London", -0.1657, 51.5073},
	{"Wembley Stadium", "stadium", "London", -0.2796, 51.5560},
	{"The Shard", "landmark", "London", -0.0865, 51.5045},
	{"Brandenburg Gate", "landmark", "Berlin", 13.3777, 52.5163},
	{"Tiergarten", "park", "Berlin", 13.3500, 52.5145},
	{"Olympiastadion", "stadium", "Berlin", 13.2394, 52.5147},
	{"Berlin TV Tower", "landmark", "Berlin", 13.4094, 52.5208},
	{"Tokyo Tower", "landmark", "Tokyo", 139.7454, 35.6586},
	{"Ueno Park", "park", "Tokyo", 139.7714, 35.7156},
	{"Tokyo Dome", "stadium", "Tokyo", 139.7519, 35.7056},
}

// placeSpec returns the demo dynamic entity spec for "place": a few text columns
// the SPA's entity browser can render. The geometry itself lives in the spatial
// plane (fabriq_geometries), not a dynamic column — the registry's neutral column
// type set has no geometry kind, so spatial participation is carried by the
// direct Spatial().Upsert write below, keyed by the same row id.
func placeSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: placeEntity,
		Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_places",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "category", Type: registry.ColText},
				{Name: "city", Type: registry.ColText},
			},
		},
	}
}

// placesFor returns the per-tenant fixture set, so each tenant's geometry lands
// in distinct cities (tenant isolation is visible in a radius query).
func placesFor(tid string) []place {
	if tid == "globex" {
		return globexPlaces
	}
	return acmePlaces
}

// seedPlaces idempotently ensures tenant tid has its place rows AND their
// geometry. It mirrors seedProducts: count existing rows, skip when already
// populated, otherwise insert each via the command executor under a
// tenant-stamped context. For every inserted row it ALSO upserts the point into
// the spatial plane via Spatial().Upsert (SRID 4326), recording lng/lat + name +
// category + city in the geometry meta so the adminapi spatial endpoint can echo
// coordinates back. The spatial upsert is ON CONFLICT DO UPDATE, so re-running on
// every startup is safe. It returns the number of geometry points seeded.
func seedPlaces(ctx context.Context, f *fabriq.Fabriq, tid string) (int, error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return 0, fmt.Errorf("admin-demo: seed places tenant %q: %w", tid, err)
	}

	fixtures := placesFor(tid)

	existing, err := countPlaces(tctx, f)
	if err != nil {
		return 0, fmt.Errorf("admin-demo: count places for %q: %w", tid, err)
	}
	if existing >= len(fixtures) {
		return 0, nil // already seeded — safe to re-run on every startup
	}

	seeded := 0
	for i := existing; i < len(fixtures); i++ {
		p := fixtures[i]
		res, execErr := f.Exec(tctx, command.Command{
			Entity: placeEntity,
			Op:     command.OpCreate,
			Payload: map[string]any{
				"name":     p.name,
				"category": p.category,
				"city":     p.city,
			},
		})
		if execErr != nil {
			return seeded, fmt.Errorf("admin-demo: seed place %q for %q: %w", p.name, tid, execErr)
		}

		geom := query.Geometry{WKT: fmt.Sprintf("POINT(%v %v)", p.lng, p.lat), SRID: 4326}
		meta := map[string]any{
			"name":     p.name,
			"category": p.category,
			"city":     p.city,
			"lng":      p.lng,
			"lat":      p.lat,
		}
		if upErr := f.Spatial().Upsert(tctx, placeEntity, res.AggID, geom, meta); upErr != nil {
			return seeded, fmt.Errorf("admin-demo: spatial upsert %q for %q: %w", p.name, tid, upErr)
		}
		seeded++
	}
	return seeded, nil
}

// countPlaces returns the number of place rows visible to the tenant in ctx.
func countPlaces(ctx context.Context, f *fabriq.Fabriq) (int, error) {
	var rows []map[string]any
	q := query.ListQuery{Limit: 1000}
	if err := f.Relational().List(ctx, placeEntity, q, &rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

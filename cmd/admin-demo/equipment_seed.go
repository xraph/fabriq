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

// siteEntity and equipmentEntity are the demo dynamic entities that anchor the
// asset-anchored + tag-filtered spatial search scenario: a site is a facility
// anchor (name/code) and equipment is a tagged asset (name/tag/site_id) that
// rings each site. Both are relational records AND points in the spatial
// plane (fabriq_geometries via Spatial().Upsert), mirroring placeEntity.
const (
	siteEntity      = "site"
	equipmentEntity = "equipment"
)

// siteFixture is one seed fixture: a facility anchor at a real-world WGS84
// coordinate, near that tenant's existing place cluster (see places_seed.go)
// so the demo's near-asset search has a meaningful neighborhood.
type siteFixture struct {
	name, code string
	lng, lat   float64
}

// sitesByTenant is one site per demo tenant, near that tenant's existing city
// cluster (acme-corp: San Francisco; globex: London).
var sitesByTenant = map[string][]siteFixture{
	"acme-corp": {{"Plant A", "PLA", -122.401, 37.792}},
	"globex":    {{"Plant B", "PLB", -0.118, 51.509}},
}

// equipmentTags are cycled around each site at small offsets so a tag filter
// (e.g. "pump") returns a subset of the ring.
var equipmentTags = []string{"pump", "valve", "sensor", "panel"}

// siteSpec returns the demo dynamic entity spec for "site": a facility anchor
// (name/code) the SPA's entity browser can render. Mirrors placeSpec.
func siteSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: siteEntity,
		Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_sites",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "code", Type: registry.ColText},
			},
		},
	}
}

// equipmentSpec returns the demo dynamic entity spec for "equipment": a
// tagged asset (name/tag/site_id) the SPA's entity browser can render.
// Mirrors placeSpec.
func equipmentSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: equipmentEntity,
		Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_equipment",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "tag", Type: registry.ColText},
				{Name: "site_id", Type: registry.ColText},
			},
		},
	}
}

// seedEquipment idempotently ensures tenant tid has its site + equipment rows
// AND their geometry. It mirrors seedPlaces: count existing equipment rows,
// skip when already populated, otherwise insert each site and its ring of
// equipment via the command executor under a tenant-stamped context. For
// every inserted row it ALSO upserts the point into the spatial plane via
// Spatial().Upsert (SRID 4326), recording lng/lat + name (+ tag/siteId for
// equipment) in the geometry meta so the adminapi spatial endpoint's
// near-asset + tag filter can be exercised. The spatial upsert is ON CONFLICT
// DO UPDATE, so re-running on every startup is safe. It returns the number of
// equipment geometry points seeded.
func seedEquipment(ctx context.Context, f *fabriq.Fabriq, tid string) (int, error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return 0, fmt.Errorf("admin-demo: seed equipment tenant %q: %w", tid, err)
	}

	existing, err := countRows(tctx, f, equipmentEntity)
	if err != nil {
		return 0, fmt.Errorf("admin-demo: count equipment for %q: %w", tid, err)
	}
	if existing > 0 {
		return 0, nil // already seeded — safe to re-run on every startup
	}

	seeded := 0
	for _, s := range sitesByTenant[tid] {
		siteRes, execErr := f.Exec(tctx, command.Command{
			Entity: siteEntity,
			Op:     command.OpCreate,
			Payload: map[string]any{
				"name": s.name,
				"code": s.code,
			},
		})
		if execErr != nil {
			return seeded, fmt.Errorf("admin-demo: seed site %q for %q: %w", s.name, tid, execErr)
		}

		sGeom := query.Geometry{WKT: fmt.Sprintf("POINT(%v %v)", s.lng, s.lat), SRID: 4326}
		sMeta := map[string]any{
			"name": s.name,
			"code": s.code,
			"lng":  s.lng,
			"lat":  s.lat,
		}
		if upErr := f.Spatial().Upsert(tctx, siteEntity, siteRes.AggID, sGeom, sMeta); upErr != nil {
			return seeded, fmt.Errorf("admin-demo: spatial upsert site %q for %q: %w", s.name, tid, upErr)
		}

		// Ring 8 equipment items around the site (~150m spacing), cycling tags,
		// so a tag filter returns a meaningful subset and the anchor site itself
		// is excluded from equipment-only results.
		for i := 0; i < 8; i++ {
			tag := equipmentTags[i%len(equipmentTags)]
			dLng := 0.0015 * float64(i%4-2)
			dLat := 0.0012 * float64(i/4)
			lng, lat := s.lng+dLng, s.lat+dLat
			name := fmt.Sprintf("%s-%s-%d", s.code, tag, i+1)

			eRes, eErr := f.Exec(tctx, command.Command{
				Entity: equipmentEntity,
				Op:     command.OpCreate,
				Payload: map[string]any{
					"name":    name,
					"tag":     tag,
					"site_id": siteRes.AggID,
				},
			})
			if eErr != nil {
				return seeded, fmt.Errorf("admin-demo: seed equipment %q for %q: %w", name, tid, eErr)
			}

			geom := query.Geometry{WKT: fmt.Sprintf("POINT(%v %v)", lng, lat), SRID: 4326}
			meta := map[string]any{
				"name":   name,
				"tag":    tag,
				"siteId": siteRes.AggID,
				"lng":    lng,
				"lat":    lat,
			}
			if upErr := f.Spatial().Upsert(tctx, equipmentEntity, eRes.AggID, geom, meta); upErr != nil {
				return seeded, fmt.Errorf("admin-demo: spatial upsert equipment %q for %q: %w", name, tid, upErr)
			}
			seeded++
		}
	}
	return seeded, nil
}

// countRows returns the number of rows of the given entity visible to the
// tenant in ctx. Mirrors countPlaces, generalized to any dynamic entity name.
func countRows(ctx context.Context, f *fabriq.Fabriq, entity string) (int, error) {
	var rows []map[string]any
	q := query.ListQuery{Limit: 1000}
	if err := f.Relational().List(ctx, entity, q, &rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/query"
)

// defaultSpatialLimit is the default cap on radius-search matches when the
// request omits "limit".
const defaultSpatialLimit = 25

// spatialWithinRequest is the request body for POST {BasePath}/spatial/within.
//
// It describes a radius search: find rows of Entity whose stored geometry lies
// within RadiusM metres of the (Lng, Lat) centre, nearest-first. Coordinates are
// WGS84 (SRID 4326), matching how the demo seed stores points.
type spatialWithinRequest struct {
	// Entity is the registered dynamic entity type name (e.g. "place").
	Entity string `json:"entity"`
	// Lng is the centre longitude (X), degrees, WGS84.
	Lng float64 `json:"lng"`
	// Lat is the centre latitude (Y), degrees, WGS84.
	Lat float64 `json:"lat"`
	// RadiusM is the search radius in metres.
	RadiusM float64 `json:"radiusM"`
	// Limit caps the number of returned matches (default 25).
	Limit int `json:"limit"`
	// CenterID, when set, makes the server resolve that entity's stored geometry
	// as the query center (lng/lat are then not required) and exclude it from
	// matches. CenterEntity defaults to Entity.
	CenterID     string `json:"centerId"`
	CenterEntity string `json:"centerEntity"`
	// Filter is AND-ed equality over the geometry meta (e.g. {"tag":"pump"}).
	Filter map[string]string `json:"filter"`
	// lngSet/latSet record whether the caller supplied the coordinate at all, so
	// a legitimate 0.0 (e.g. the prime meridian / equator) is not mistaken for a
	// missing field. They are populated by UnmarshalJSON.
	lngSet bool `json:"-"`
	latSet bool `json:"-"`
}

// UnmarshalJSON decodes the request while tracking whether lng/lat were present
// in the payload. A bare missing field stays false (→ 400), but an explicit 0
// is honoured as a real coordinate.
func (r *spatialWithinRequest) UnmarshalJSON(data []byte) error {
	var raw struct {
		Entity       string            `json:"entity"`
		Lng          *float64          `json:"lng"`
		Lat          *float64          `json:"lat"`
		RadiusM      float64           `json:"radiusM"`
		Limit        int               `json:"limit"`
		CenterID     string            `json:"centerId"`
		CenterEntity string            `json:"centerEntity"`
		Filter       map[string]string `json:"filter"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	r.Entity = raw.Entity
	r.RadiusM = raw.RadiusM
	r.Limit = raw.Limit
	r.CenterID = raw.CenterID
	r.CenterEntity = raw.CenterEntity
	r.Filter = raw.Filter
	if raw.Lng != nil {
		r.Lng = *raw.Lng
		r.lngSet = true
	}
	if raw.Lat != nil {
		r.Lat = *raw.Lat
		r.latSet = true
	}
	return nil
}

// spatialMatchItem is one radius-search hit, nearest first.
//
// DistanceM is the geodesic distance from the query centre in metres. Lng/Lat
// are echoed from the match's stored meta when the seed recorded them (the demo
// seed stores lng/lat in meta); they are omitted when the meta carries no
// coordinates. Data is hydrated best-effort from the relational source of truth
// and may be nil when the row could not be loaded (e.g. it was deleted after the
// geometry was upserted).
type spatialMatchItem struct {
	ID        string         `json:"id"`
	DistanceM float64        `json:"distanceM"`
	Lng       *float64       `json:"lng,omitempty"`
	Lat       *float64       `json:"lat,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
}

// spatialWithinResponse is the payload for POST {BasePath}/spatial/within.
type spatialWithinResponse struct {
	Matches []spatialMatchItem `json:"matches"`
}

// registerSpatialRoutes wires the spatial radius-search route onto the given
// router. It shares the same route options (auth/tenant middleware) as the rest
// of the admin surface so the host controls the security boundary uniformly.
func (c *adminController) registerSpatialRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	withinOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.spatial.within"),
		forge.WithSummary("Spatial radius search (body: {entity, lng, lat, radiusM, limit?})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/spatial/within", c.handleSpatialWithin, withinOpts...)
}

// handleSpatialWithin serves POST {BasePath}/spatial/within.
//
// Request body:
//
//	{ "entity": "<entityName>", "lng": -122.42, "lat": 37.77, "radiusM": 50000, "limit": 10 }
//
// Returns 501 when the instance has no spatial backend configured, and 400 when
// entity, lng, lat, or radiusM is missing/invalid.
func (c *adminController) handleSpatialWithin(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req spatialWithinRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}
	if req.Entity == "" {
		return forge.BadRequest("field 'entity' is required")
	}
	if req.CenterID == "" {
		if !req.lngSet {
			return forge.BadRequest("field 'lng' is required (or provide 'centerId')")
		}
		if !req.latSet {
			return forge.BadRequest("field 'lat' is required (or provide 'centerId')")
		}
	}
	if req.RadiusM <= 0 {
		return forge.BadRequest("field 'radiusM' must be a positive number")
	}

	limit := defaultSpatialLimit
	if req.Limit > 0 {
		limit = req.Limit
	}

	reqCtx := ctx.Request().Context()
	spatial := fab.Spatial()

	// Detect an unconfigured spatial backend BEFORE issuing the real query: the
	// notConfigured stub answers every Within with ErrStoreNotConfigured. Reuse
	// the same tenant-less, side-effect-free probe the capabilities endpoint uses.
	if !spatialConfigured(reqCtx, spatial) {
		return c.spatialNotConfigured(ctx)
	}

	// Resolve the query center: an explicit point, or an anchor asset's geometry.
	center := query.Geometry{WKT: fmt.Sprintf("POINT(%v %v)", req.Lng, req.Lat), SRID: 4326}
	excludeID := ""
	if req.CenterID != "" {
		centerEntity := req.CenterEntity
		if centerEntity == "" {
			centerEntity = req.Entity
		}
		geom, _, ok, gErr := spatial.Get(reqCtx, centerEntity, req.CenterID)
		if gErr != nil {
			return renderError(ctx, gErr)
		}
		if !ok {
			return forge.BadRequest(fmt.Sprintf("centerId %q not found in %q", req.CenterID, centerEntity))
		}
		center = geom
		if centerEntity == req.Entity {
			excludeID = req.CenterID // anchor is in the searched set → drop it
		}
	}

	// Over-fetch by one when excluding the anchor so the caller still gets `limit`.
	k := limit
	if excludeID != "" {
		k = limit + 1
	}

	var matches []query.SpatialMatch
	sq := query.SpatialQuery{Entity: req.Entity, Center: center, RadiusM: req.RadiusM, K: k, Filter: req.Filter}
	if withinErr := spatial.Within(reqCtx, sq, &matches); withinErr != nil {
		return renderError(ctx, withinErr)
	}

	items := c.hydrateSpatialMatches(reqCtx, fab, req.Entity, matches, excludeID, limit)
	return ctx.JSON(http.StatusOK, spatialWithinResponse{Matches: items})
}

// hydrateSpatialMatches builds the response items for each spatial match. It
// echoes the lng/lat the seed recorded in the match meta (when present) and
// loads the relational row best-effort. A row that cannot be loaded (deleted, or
// an unknown type) yields a match with nil Data rather than failing the whole
// request — the distances are still useful to a geo playground. Hydration reuses
// the map-native List path the rest of the admin surface uses for dynamic
// entities (mirrors hydrateMatches for vector search).
func (c *adminController) hydrateSpatialMatches(
	ctx context.Context, fab query.Fabric, entityType string, matches []query.SpatialMatch, excludeID string, limit int,
) []spatialMatchItem {
	items := make([]spatialMatchItem, 0, len(matches))
	for _, m := range matches {
		if excludeID != "" && m.ID == excludeID {
			continue
		}

		item := spatialMatchItem{ID: m.ID, DistanceM: m.DistanceM}

		// Echo coordinates back from the match meta when the seed stored them.
		// The meta is engine-neutral JSON, so numeric values arrive as float64.
		if lng, ok := metaFloat(m.Meta, "lng"); ok {
			item.Lng = &lng
		}
		if lat, ok := metaFloat(m.Meta, "lat"); ok {
			item.Lat = &lat
		}

		var rows []map[string]any
		q := query.ListQuery{Where: query.Where{query.Eq("id", m.ID)}, Limit: 1}
		if err := fab.Relational().List(ctx, entityType, q, &rows); err == nil && len(rows) > 0 {
			item.Data = rows[0]
		}
		items = append(items, item)
		if len(items) >= limit {
			break
		}
	}
	return items
}

// metaFloat reads a numeric coordinate from a spatial match's meta. Meta values
// round-trip through JSON, so a stored number arrives as float64; an integer
// arrives as float64 too. Returns ok=false when the key is absent or non-numeric.
func metaFloat(meta map[string]any, key string) (float64, bool) {
	if meta == nil {
		return 0, false
	}
	switch v := meta[key].(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

// spatialNotConfigured returns the 501 response used when the instance has no
// spatial backend wired. It mirrors the not-configured shape used across the
// admin surface so the SPA can branch on a stable error payload.
func (c *adminController) spatialNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "spatial not configured"})
}

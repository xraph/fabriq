package client

import (
	"context"
	"net/http"
)

// SpatialMatch is one within-radius search hit, nearest first. It mirrors
// adminapi's spatialMatchItem JSON exactly: {id, distanceM, lng, lat, data}.
type SpatialMatch struct {
	ID        string         `json:"id"`
	DistanceM float64        `json:"distanceM,omitempty"`
	Lng       *float64       `json:"lng,omitempty"`
	Lat       *float64       `json:"lat,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
}

// SpatialResult is the payload for POST {BasePath}/spatial/within. It
// mirrors adminapi's spatialWithinResponse JSON exactly: {matches}.
type SpatialResult struct {
	Matches []SpatialMatch `json:"matches"`
}

// SpatialWithinInput is the request body for SpatialWithin. It mirrors
// adminapi's spatialWithinRequest JSON exactly: {entity, lng, lat, radiusM,
// limit}. Coordinates are WGS84 (SRID 4326).
type SpatialWithinInput struct {
	// Entity is the registered dynamic entity type name (e.g. "place").
	Entity string `json:"entity"`
	// Lng is the centre longitude (X), degrees, WGS84.
	Lng float64 `json:"lng"`
	// Lat is the centre latitude (Y), degrees, WGS84.
	Lat float64 `json:"lat"`
	// RadiusM is the search radius in metres.
	RadiusM float64 `json:"radiusM"`
	// Limit caps the number of returned matches (server default 25). Zero
	// defers to the server default.
	Limit int `json:"limit,omitempty"`
}

// SpatialWithin performs a within-radius geo search: rows of Entity whose
// stored geometry lies within RadiusM metres of (Lng, Lat), nearest-first.
// It calls POST {BasePath}/spatial/within with body
// {entity, lng, lat, radiusM, limit}. Returns *APIError with Status 501 when
// the instance has no spatial backend configured, and Status 400 when
// entity, lng, lat, or radiusM is missing/invalid.
func (c *Client) SpatialWithin(ctx context.Context, input SpatialWithinInput) (SpatialResult, error) {
	var out SpatialResult
	if err := c.do(ctx, http.MethodPost, "/spatial/within", nil, input, &out); err != nil {
		return SpatialResult{}, err
	}
	return out, nil
}

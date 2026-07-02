package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_SpatialWithin(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody SpatialWithinInput

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		lng, lat := -122.42, 37.77
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SpatialResult{
			Matches: []SpatialMatch{
				{ID: "1", DistanceM: 123.4, Lng: &lng, Lat: &lat, Data: map[string]any{"name": "cafe"}},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.SpatialWithin(context.Background(), SpatialWithinInput{
		Entity:  "place",
		Lng:     -122.42,
		Lat:     37.77,
		RadiusM: 50000,
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("SpatialWithin() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/spatial/within" {
		t.Errorf("path = %q, want /admin/spatial/within", gotPath)
	}
	if gotBody.Entity != "place" {
		t.Errorf("request body entity = %q, want %q", gotBody.Entity, "place")
	}
	if gotBody.Lng != -122.42 {
		t.Errorf("request body lng = %v, want %v", gotBody.Lng, -122.42)
	}
	if gotBody.Lat != 37.77 {
		t.Errorf("request body lat = %v, want %v", gotBody.Lat, 37.77)
	}
	if gotBody.RadiusM != 50000 {
		t.Errorf("request body radiusM = %v, want %v", gotBody.RadiusM, 50000)
	}
	if gotBody.Limit != 10 {
		t.Errorf("request body limit = %d, want %d", gotBody.Limit, 10)
	}

	if len(result.Matches) != 1 {
		t.Fatalf("len(result.Matches) = %d, want 1", len(result.Matches))
	}
	m := result.Matches[0]
	if m.ID != "1" || m.DistanceM != 123.4 {
		t.Errorf("result.Matches[0] = %+v, want id=1 distanceM=123.4", m)
	}
	if m.Lng == nil || *m.Lng != -122.42 {
		t.Errorf("result.Matches[0].Lng = %v, want -122.42", m.Lng)
	}
	if m.Lat == nil || *m.Lat != 37.77 {
		t.Errorf("result.Matches[0].Lat = %v, want 37.77", m.Lat)
	}
	if m.Data["name"] != "cafe" {
		t.Errorf("result.Matches[0].Data[name] = %v, want cafe", m.Data["name"])
	}
}

func TestClient_SpatialWithin_OmitsZeroLimit(t *testing.T) {
	var gotBody map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(SpatialResult{})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.SpatialWithin(context.Background(), SpatialWithinInput{
		Entity:  "place",
		Lng:     0,
		Lat:     0,
		RadiusM: 1000,
	}); err != nil {
		t.Fatalf("SpatialWithin() unexpected error: %v", err)
	}

	if _, ok := gotBody["limit"]; ok {
		t.Errorf("request body has 'limit' key = %v, want omitted", gotBody["limit"])
	}
	// lng/lat are legitimate zero values (equator/prime meridian) and must
	// still be sent explicitly (no omitempty on these fields).
	if _, ok := gotBody["lng"]; !ok {
		t.Errorf("request body missing 'lng' key, want present (zero value)")
	}
	if _, ok := gotBody["lat"]; !ok {
		t.Errorf("request body missing 'lat' key, want present (zero value)")
	}
}

func TestClient_SpatialWithin_NotConfigured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "spatial not configured"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.SpatialWithin(context.Background(), SpatialWithinInput{
		Entity:  "place",
		Lng:     -122.42,
		Lat:     37.77,
		RadiusM: 1000,
	})
	if err == nil {
		t.Fatal("SpatialWithin() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("SpatialWithin() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}

func TestClient_SpatialWithin_BadRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]string{
				"code":    "invalid_input",
				"message": "field 'radiusM' must be a positive number",
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.SpatialWithin(context.Background(), SpatialWithinInput{
		Entity: "place",
		Lng:    -122.42,
		Lat:    37.77,
	})
	if err == nil {
		t.Fatal("SpatialWithin() expected error, got nil")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("SpatialWithin() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusBadRequest {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusBadRequest)
	}
}

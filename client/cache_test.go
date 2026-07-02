package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_GetCache(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(CacheInfo{
			Configured: true,
			Keyspaces: []CacheKeyspace{
				{
					Entity:     "widget",
					Name:       "widget:q",
					Partition:  "tenant",
					Mode:       "versioned",
					TTLSeconds: 60,
					Scoped:     false,
				},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	info, err := c.GetCache(context.Background())
	if err != nil {
		t.Fatalf("GetCache() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/cache" {
		t.Errorf("path = %q, want /admin/cache", gotPath)
	}
	if !info.Configured {
		t.Errorf("info.Configured = %v, want true", info.Configured)
	}
	if len(info.Keyspaces) != 1 {
		t.Fatalf("len(info.Keyspaces) = %d, want 1", len(info.Keyspaces))
	}
	if info.Keyspaces[0].Entity != "widget" || info.Keyspaces[0].TTLSeconds != 60 {
		t.Errorf("info.Keyspaces[0] = %+v, want entity=widget ttlSeconds=60", info.Keyspaces[0])
	}
}

func TestClient_GetCacheStats(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(CacheStats{
			Available:     true,
			Hits:          80,
			Misses:        20,
			Sets:          15,
			Invalidations: 3,
			HitRate:       0.8,
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	stats, err := c.GetCacheStats(context.Background())
	if err != nil {
		t.Fatalf("GetCacheStats() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/cache/stats" {
		t.Errorf("path = %q, want /admin/cache/stats", gotPath)
	}
	if !stats.Available {
		t.Errorf("stats.Available = %v, want true", stats.Available)
	}
	if stats.Hits != 80 || stats.Misses != 20 {
		t.Errorf("stats = %+v, want hits=80 misses=20", stats)
	}
	if stats.HitRate != 0.8 {
		t.Errorf("stats.HitRate = %v, want 0.8", stats.HitRate)
	}
}

func TestClient_GetCacheStats_NotConfigured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "cache not configured"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GetCacheStats(context.Background())
	if err == nil {
		t.Fatal("GetCacheStats() expected error, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("GetCacheStats() error type = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
}

func TestClient_CacheInvalidate(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("server: decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(CacheInvalidateResult{Invalidated: true})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	result, err := c.CacheInvalidate(context.Background(), "widget")
	if err != nil {
		t.Fatalf("CacheInvalidate() unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/admin/cache/invalidate" {
		t.Errorf("path = %q, want /admin/cache/invalidate", gotPath)
	}
	if gotBody["entity"] != "widget" {
		t.Errorf("request body entity = %q, want %q", gotBody["entity"], "widget")
	}
	if !result.Invalidated {
		t.Errorf("result.Invalidated = %v, want true", result.Invalidated)
	}
}

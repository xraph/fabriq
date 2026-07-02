package adminapi

import (
	"net/http"
	"testing"
)

// The fake-backed harness has no engine cache (nil parent → nil cache), so the
// status endpoint reports configured:false and invalidate returns 501. The
// populated keyspace + real invalidation are covered by the live admin-demo
// verification (product declares a CacheSpec there).

func TestCache_ReportsUnconfiguredOnFake(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/cache")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Configured bool `json:"configured"`
		Keyspaces  []struct {
			Entity string `json:"entity"`
		} `json:"keyspaces"`
	}
	decode(t, resp, &out)
	if out.Configured {
		t.Fatal("expected configured=false on the fake harness")
	}
	// widget declares no CacheSpec → no keyspaces.
	if len(out.Keyspaces) != 0 {
		t.Fatalf("keyspaces = %d, want 0", len(out.Keyspaces))
	}
}

func TestCacheInvalidate_NotConfigured(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/cache/invalidate",
		map[string]any{"entity": "widget"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}

func TestCacheStats_NotConfigured(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/cache/stats")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}

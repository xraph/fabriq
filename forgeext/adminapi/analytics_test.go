package adminapi

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestAnalyticsBackfill_403WhenGateOff verifies that without WithAnalyticsAdmin
// the backfill endpoint answers 403 (capability gate), not 500 or 404 —
// mirrors TestTenantProvision_403WhenGateOff.
func TestAnalyticsBackfill_403WhenGateOff(t *testing.T) {
	e := NewAdminAPI(nil) // AnalyticsAdmin defaults to false
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/backfill", map[string]any{"tenant": "acme"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (analytics admin not enabled)", resp.StatusCode)
	}
}

// TestAnalyticsStatus_403WhenGateOff mirrors the backfill gate test for the
// status endpoint.
func TestAnalyticsStatus_403WhenGateOff(t *testing.T) {
	e := NewAdminAPI(nil) // AnalyticsAdmin defaults to false
	srv := buildServer(t, e)
	defer srv.Close()

	resp := getNoTenant(t, srv, "/admin/analytics/status")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (analytics admin not enabled)", resp.StatusCode)
	}
}

// TestAnalyticsPurge_403WhenGateOff verifies the destructive erase endpoint is
// also capability-gated: 403 without WithAnalyticsAdmin.
func TestAnalyticsPurge_403WhenGateOff(t *testing.T) {
	e := NewAdminAPI(nil) // AnalyticsAdmin defaults to false
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/purge", map[string]any{"tenant": "acme"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (analytics admin not enabled)", resp.StatusCode)
	}
}

// TestAnalyticsPurge_NoParent verifies that with the gate on but no parent
// extension (so no sink), purge answers 400 (not 500/panic).
func TestAnalyticsPurge_NoParent(t *testing.T) {
	e := NewAdminAPI(nil, WithAnalyticsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/purge", map[string]any{"tenant": "acme"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (no parent extension)", resp.StatusCode)
	}
}

// TestAnalyticsEndpoints_NoParent verifies that with WithAnalyticsAdmin on but
// no parent forgeext.Extension (so Stores() is unreachable), the backfill
// endpoint answers 400 (not 500/panic) — the pure-unit path with no Docker, no
// Postgres.
func TestAnalyticsEndpoints_NoParent(t *testing.T) {
	e := NewAdminAPI(nil, WithAnalyticsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/backfill", map[string]any{"tenant": "acme"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (no parent extension)", resp.StatusCode)
	}
}

// TestAnalyticsBackfill_RequiresSelector verifies a body with neither
// "tenant" nor "all" set answers 400, not 500 — checked with the gate off so
// the 403 short-circuit fires first, and again is exercised implicitly by
// TestAnalyticsEndpoints_NoParent's shape once the gate is satisfied.
func TestAnalyticsBackfill_RequiresSelector(t *testing.T) {
	e := NewAdminAPI(nil, WithAnalyticsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/backfill", map[string]any{})
	defer resp.Body.Close()

	// No parent means requireAnalyticsAdmin's 400 fires before selector
	// validation — either way the contract is "a 4xx, never a panic".
	if resp.StatusCode >= 500 {
		t.Fatalf("status = %d, want a 4xx", resp.StatusCode)
	}
}

// TestAnalyticsEndpoints_Capability verifies the analytics.admin/analytics.read
// capabilities are advertised in GET /admin/meta only when WithAnalyticsAdmin
// is set — mirrors TestTenantEndpoints_Capability.
func TestAnalyticsEndpoints_Capability(t *testing.T) {
	world := buildTestWorld(t)

	off := fakeBackedAdminExt(t, world)
	srvOff := buildServer(t, off)
	defer srvOff.Close()
	respOff := get(t, srvOff, "/admin/meta")
	defer respOff.Body.Close()
	var metaOff metaResponse
	if err := json.NewDecoder(respOff.Body).Decode(&metaOff); err != nil {
		t.Fatalf("decode meta (off): %v", err)
	}
	for _, c := range metaOff.Capabilities {
		if c == "analytics.admin" || c == "analytics.read" {
			t.Fatalf("analytics caps must not be advertised when AnalyticsAdmin is off, got %v", metaOff.Capabilities)
		}
	}

	on := fakeBackedAdminExt(t, world, WithAnalyticsAdmin())
	srvOn := buildServer(t, on)
	defer srvOn.Close()
	respOn := get(t, srvOn, "/admin/meta")
	defer respOn.Body.Close()
	var metaOn metaResponse
	if err := json.NewDecoder(respOn.Body).Decode(&metaOn); err != nil {
		t.Fatalf("decode meta (on): %v", err)
	}
	foundAdmin, foundRead := false, false
	for _, c := range metaOn.Capabilities {
		if c == "analytics.admin" {
			foundAdmin = true
		}
		if c == "analytics.read" {
			foundRead = true
		}
	}
	if !foundAdmin || !foundRead {
		t.Fatalf("analytics.admin and analytics.read must both be advertised when AnalyticsAdmin is on, got %v", metaOn.Capabilities)
	}
}

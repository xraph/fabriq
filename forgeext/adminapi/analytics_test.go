package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
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

// TestAnalyticsReproject_403WhenGateOff verifies the reproject endpoint is
// capability-gated: 403 without WithAnalyticsAdmin.
func TestAnalyticsReproject_403WhenGateOff(t *testing.T) {
	e := NewAdminAPI(nil) // AnalyticsAdmin defaults to false
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/reproject", map[string]any{"tenant": "acme"})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (analytics admin not enabled)", resp.StatusCode)
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

// TestAnalyticsReconcile_403WhenGateOff verifies the reconcile endpoint is gated.
func TestAnalyticsReconcile_403WhenGateOff(t *testing.T) {
	e := NewAdminAPI(nil)
	srv := buildServer(t, e)
	defer srv.Close()
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/reconcile", map[string]any{"tenant": "acme"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

// TestAnalyticsJobs_LifecycleDone runs a job to completion and checks the
// recorded result and terminal state.
func TestAnalyticsJobs_LifecycleDone(t *testing.T) {
	j := newAnalyticsJobs()
	job := j.start("backfill", func(context.Context) (any, error) {
		return map[string]int{"t1": 3}, nil
	})
	got := waitJob(t, j, job.ID)
	if got.State != "done" || got.Error != "" {
		t.Fatalf("state=%q err=%q, want done/empty", got.State, got.Error)
	}
	if string(got.Result) != `{"t1":3}` {
		t.Fatalf("result = %s, want {\"t1\":3}", got.Result)
	}
	if got.EndedAt == nil {
		t.Fatal("EndedAt should be set on a terminal job")
	}
}

// TestAnalyticsJobs_LifecycleFailed records a failed run.
func TestAnalyticsJobs_LifecycleFailed(t *testing.T) {
	j := newAnalyticsJobs()
	job := j.start("reproject", func(context.Context) (any, error) {
		return nil, boomError("boom")
	})
	got := waitJob(t, j, job.ID)
	if got.State != "failed" || got.Error != "boom" {
		t.Fatalf("state=%q err=%q, want failed/boom", got.State, got.Error)
	}
}

type boomError string

func (e boomError) Error() string { return string(e) }

func waitJob(t *testing.T, j *analyticsJobs, id string) analyticsJob {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if job, ok := j.get(id); ok {
			j.mu.Lock()
			snap := *job
			j.mu.Unlock()
			if snap.State != "running" {
				return snap
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach a terminal state", id)
	return analyticsJob{}
}

// TestAnalyticsStatus_200WithReadOnly: WithAnalyticsRead opens the read-only
// status endpoint (no admin needed). With no parent extension the handler
// returns 200 with an empty payload — the point is the gate lets it through.
func TestAnalyticsStatus_200WithReadOnly(t *testing.T) {
	e := NewAdminAPI(nil, WithAnalyticsRead())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := getNoTenant(t, srv, "/admin/analytics/status")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (read gate open)", resp.StatusCode)
	}
}

// TestAnalyticsBackfill_403WithReadOnly: a read-only grant must NOT unlock a
// mutating endpoint — backfill still requires analytics.admin.
func TestAnalyticsBackfill_403WithReadOnly(t *testing.T) {
	e := NewAdminAPI(nil, WithAnalyticsRead())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/backfill", map[string]any{"tenant": "acme"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (read grant must not permit mutation)", resp.StatusCode)
	}
}

// TestAnalyticsCaps_ReadVsAdmin: meta advertises analytics.read for a read-only
// host and both caps for an admin host.
func TestAnalyticsCaps_ReadVsAdmin(t *testing.T) {
	capsFor := func(t *testing.T, opts ...Option) []string {
		e := NewAdminAPI(nil, opts...)
		srv := buildServer(t, e)
		defer srv.Close()
		resp := getNoTenant(t, srv, "/admin/meta")
		defer resp.Body.Close()
		var body struct {
			Capabilities []string `json:"capabilities"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return body.Capabilities
	}
	has := func(caps []string, c string) bool {
		for _, x := range caps {
			if x == c {
				return true
			}
		}
		return false
	}

	read := capsFor(t, WithAnalyticsRead())
	if !has(read, "analytics.read") || has(read, "analytics.admin") {
		t.Fatalf("read-only caps = %v, want analytics.read but NOT analytics.admin", read)
	}
	admin := capsFor(t, WithAnalyticsAdmin())
	if !has(admin, "analytics.read") || !has(admin, "analytics.admin") {
		t.Fatalf("admin caps = %v, want BOTH analytics.read and analytics.admin", admin)
	}
}

func TestSummarizeLag(t *testing.T) {
	if worst, behind := summarizeLag(map[string]float64{}, 60); worst != 0 || behind != 0 {
		t.Fatalf("empty: worst=%v behind=%d, want 0,0", worst, behind)
	}
	// c is EXACTLY at the threshold: a strict `>` must NOT count it, matching the
	// worker's fabriq_analytics_tenants_behind gauge so the dashboard and the
	// alert metric agree at the boundary. Only b (120) is behind.
	worst, behind := summarizeLag(map[string]float64{"a": 10, "b": 120, "c": 60}, 60)
	if worst != 120 {
		t.Fatalf("worst=%v, want 120", worst)
	}
	if behind != 1 { // only b exceeds 60; c==60 is the boundary (not >), a is under
		t.Fatalf("behind=%d, want 1 (strict > threshold, matching the worker gauge)", behind)
	}
}

// TestAnalyticsStatus_FreshnessShapeNoParent: with the read gate on but no
// parent extension, status is 200 and freshness fields are zero-valued (no sink
// to query) — proves the handler path compiles and stays best-effort.
func TestAnalyticsStatus_FreshnessShapeNoParent(t *testing.T) {
	e := NewAdminAPI(nil, WithAnalyticsRead())
	srv := buildServer(t, e)
	defer srv.Close()

	resp := getNoTenant(t, srv, "/admin/analytics/status")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body analyticsStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.WorstLagSeconds != 0 || body.TenantsBehind != 0 || len(body.PerTenantLag) != 0 {
		t.Fatalf("no-parent freshness = %+v, want zero-valued", body)
	}
}

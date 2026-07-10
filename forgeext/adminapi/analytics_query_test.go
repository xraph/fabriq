package adminapi

import (
	"net/http"
	"testing"
)

func TestAnalyticsQuery_403WhenReadOff(t *testing.T) {
	e := NewAdminAPI(nil) // no analytics.read
	srv := buildServer(t, e)
	defer srv.Close()
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/query", map[string]any{"sql": "SELECT 1"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (analytics.read not enabled)", resp.StatusCode)
	}
}

func TestAnalyticsQuery_400OnNonReadOnly(t *testing.T) {
	e := NewAdminAPI(nil, WithAnalyticsRead())
	srv := buildServer(t, e)
	defer srv.Close()
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/query", map[string]any{"sql": "DELETE FROM fabriq_analytics_facts"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (non-read-only)", resp.StatusCode)
	}
}

func TestAnalyticsQuery_400OnWriteCTE(t *testing.T) {
	e := NewAdminAPI(nil, WithAnalyticsRead())
	srv := buildServer(t, e)
	defer srv.Close()
	body := map[string]any{"sql": "WITH x AS (DELETE FROM fabriq_analytics_facts RETURNING 1) SELECT * FROM x"}
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/query", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (data-modifying CTE)", resp.StatusCode)
	}
}

func TestAnalyticsQuery_501WhenNoSink(t *testing.T) {
	e := NewAdminAPI(nil, WithAnalyticsRead())
	srv := buildServer(t, e)
	defer srv.Close()
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/query", map[string]any{"sql": "SELECT 1"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501 (no sink configured)", resp.StatusCode)
	}
}

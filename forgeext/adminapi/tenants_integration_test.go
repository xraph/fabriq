//go:build integration

package adminapi

// End-to-end tenant provisioning through the admin HTTP surface in catalog
// mode (db-per-tenant). Unlike tenants_test.go's unit gates — which drive a
// nil-parent Extension and only assert the capability/route/precedence
// behaviour — this test stands up a REAL forgeext.Extension in catalog mode
// against a testcontainer Postgres, then provisions a tenant purely through
// HTTP: POST /admin/tenants (async 202 + jobId) → poll GET
// /admin/tenants/jobs/:id to completion → GET /admin/tenants/:id and assert
// the tenant is active on its dedicated database.
//
// This is the regression guard for the forgeext.Extension.Start catalog-mode
// source-of-truth check: before the `|| cfg.Catalog.Enabled()` fix, Start
// rejected a catalog-only config with "a Postgres source of truth is
// required to serve", so a real Extension could not be started in catalog
// mode at all and this whole path was untestable end to end.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
)

// startCatalogAdmin boots a real forgeext.Extension in catalog mode over the
// given superuser DSN (used as BOTH the catalog control DB and the sole
// cluster's maintenance DSN, mirroring catalogmode_integration_test.go), then
// resolves the adminapi Extension over it with the tenants-admin gate on. It
// returns a test HTTP server serving the admin routes.
func startCatalogAdmin(t *testing.T, ctx context.Context, dsn string) *Extension {
	t.Helper()

	// A minimal validated registry is enough: provisioning routes through the
	// catalog + cluster ops, not the entity registry, so no tenant table is
	// ever touched by this path.
	reg := registry.New()
	if err := reg.Register(widgetSpec()); err != nil {
		t.Fatalf("register widget: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}

	cfg := fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN:            dsn,
			ClusterDSNs:    map[string]string{"c1": dsn},
			AllowSuperuser: true, // testcontainers hand out superuser creds
		},
	}

	// The regression surface: Start must accept a catalog-only config (no
	// postgres.dsn, no shards, no injected grove) and reach fabriq.Open.
	parent := forgeext.New(reg, forgeext.WithConfig(cfg))
	if err := parent.Start(ctx); err != nil {
		t.Fatalf("forgeext.Extension.Start (catalog mode): %v", err)
	}
	t.Cleanup(func() { _ = parent.Stop(context.Background()) })

	adminExt := NewAdminAPI(parent, WithTenantsAdmin())
	if err := adminExt.Start(ctx); err != nil {
		t.Fatalf("adminapi.Extension.Start: %v", err)
	}
	return adminExt
}

func TestTenantProvision_EndToEnd_CatalogMode(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t) // control DB + cluster maintenance DSN

	adminExt := startCatalogAdmin(t, ctx, dsn)
	srv := buildServer(t, adminExt)
	defer srv.Close()

	// 1. POST /admin/tenants — start the async provisioning job (202 + jobId).
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/tenants",
		map[string]any{"tenantId": "acme", "clusterId": "c1"})
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("POST /admin/tenants status = %d, want 202; body = %s", resp.StatusCode, body)
	}
	var provisioned struct {
		JobID string `json:"jobId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&provisioned); err != nil {
		resp.Body.Close()
		t.Fatalf("decode provision response: %v", err)
	}
	resp.Body.Close()
	if provisioned.JobID == "" {
		t.Fatal("POST /admin/tenants returned an empty jobId")
	}

	// 2. Poll GET /admin/tenants/jobs/:id until the job leaves "running".
	//    Provisioning issues CREATE DATABASE + the full migration chain against
	//    a fresh tenant database, so give it a generous ceiling.
	job := pollTenantJob(t, srv, provisioned.JobID, 90*time.Second)
	if job.State != "done" {
		t.Fatalf("job state = %q (error=%q), want \"done\"", job.State, job.Error)
	}
	if job.Entry == nil {
		t.Fatal("done job carries no tenant entry")
	}
	if job.Entry.State != "active" {
		t.Fatalf("job entry state = %q, want \"active\"", job.Entry.State)
	}

	// 3. GET /admin/tenants/:id — the tenant is now a routable, active catalog
	//    entry on its own dedicated database.
	getResp := get(t, srv, "/admin/tenants/acme")
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		t.Fatalf("GET /admin/tenants/acme status = %d, want 200; body = %s", getResp.StatusCode, body)
	}
	var view tenantView
	if err := json.NewDecoder(getResp.Body).Decode(&view); err != nil {
		t.Fatalf("decode tenant view: %v", err)
	}
	if view.TenantID != "acme" {
		t.Errorf("tenantId = %q, want \"acme\"", view.TenantID)
	}
	if view.ClusterID != "c1" {
		t.Errorf("clusterId = %q, want \"c1\"", view.ClusterID)
	}
	if view.Database != "fabriq_acme" {
		t.Errorf("database = %q, want \"fabriq_acme\"", view.Database)
	}
	if view.State != "active" {
		t.Errorf("state = %q, want \"active\"", view.State)
	}
	if view.Version == "" {
		t.Error("active tenant must carry a migrated schema version")
	}
}

// pollTenantJob polls GET /admin/tenants/jobs/:id until the job reaches a
// terminal state (done|failed) or the deadline elapses, returning the final
// snapshot. A job that never terminates fails the test rather than hanging.
func pollTenantJob(t *testing.T, srv *httptest.Server, id string, within time.Duration) tenantJob {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		resp := get(t, srv, "/admin/tenants/jobs/"+id)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("GET /admin/tenants/jobs/%s status = %d; body = %s", id, resp.StatusCode, body)
		}
		var job tenantJob
		if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
			resp.Body.Close()
			t.Fatalf("decode job: %v", err)
		}
		resp.Body.Close()
		if job.State != "running" {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant job %s still running after %s", id, within)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

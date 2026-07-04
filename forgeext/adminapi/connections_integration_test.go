//go:build integration

package adminapi

// End-to-end connection-info over the admin HTTP surface in catalog mode
// (db-per-tenant). It stands up a REAL forgeext.Extension against a
// testcontainer Postgres (used as BOTH the catalog control DB and the sole
// cluster's maintenance DSN, mirroring tenants_integration_test.go), provisions
// a tenant purely over HTTP, then exercises:
//
//   GET /admin/connections            — the redacted tier topology + health
//   GET /admin/tenants/:id/connection — the tenant's dedicated database
//
// The load-bearing assertion is the product-owner security requirement: the
// container's real Postgres password must appear in NEITHER response body.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
)

// startCatalogConnAdmin boots a real forgeext.Extension in catalog mode over
// dsn with BOTH the tenants-admin gate (to provision over HTTP) and the
// connections-read gate on.
func startCatalogConnAdmin(t *testing.T, ctx context.Context, dsn string) *Extension {
	t.Helper()
	return startConnAdmin(t, ctx, fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN:            dsn,
			ClusterDSNs:    map[string]string{"c1": dsn},
			AllowSuperuser: true, // testcontainers hand out superuser creds
		},
	})
}

// startConnAdmin boots a real forgeext.Extension over cfg with the
// tenants-admin and connections-read gates on, and returns the adminapi
// extension serving over it.
func startConnAdmin(t *testing.T, ctx context.Context, cfg fabriq.Config) *Extension {
	t.Helper()

	reg := registry.New()
	if err := reg.Register(widgetSpec()); err != nil {
		t.Fatalf("register widget: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}

	parent := forgeext.New(reg, forgeext.WithConfig(cfg))
	if err := parent.Start(ctx); err != nil {
		t.Fatalf("forgeext.Extension.Start (catalog mode): %v", err)
	}
	t.Cleanup(func() { _ = parent.Stop(context.Background()) })

	adminExt := NewAdminAPI(parent, WithTenantsAdmin(), WithConnectionsRead())
	if err := adminExt.Start(ctx); err != nil {
		t.Fatalf("adminapi.Extension.Start: %v", err)
	}
	return adminExt
}

// dsnPassword extracts the password from a postgres DSN so a test can assert it
// never appears in a response body.
func dsnPassword(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	pw, ok := u.User.Password()
	if !ok || pw == "" {
		t.Fatalf("test DSN carries no password to assert redaction against: %s", dsn)
	}
	return pw
}

func TestConnections_EndToEnd_CatalogMode(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	password := dsnPassword(t, dsn)

	// The container password ("fabriq") also legitimately appears as a username
	// and inside database names (fabriq_control, fabriq_acme), so a bare
	// substring check would false-positive. The SOUND signal that a secret
	// leaked is the DSN userinfo form ":password@" — it can only appear if a raw
	// connection string is serialized, never from separately-redacted fields.
	// (The distinctive-secret substring no-leak assertion lives in the unit
	// test TestBuildConnections_NoPasswordLeak.)
	leakMarker := ":" + password + "@"

	adminExt := startCatalogConnAdmin(t, ctx, dsn)
	srv := buildServer(t, adminExt)
	defer srv.Close()

	// Provision "acme" over HTTP so the tenant-connection endpoint has a real
	// dedicated database to describe.
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
	if job := pollTenantJob(t, srv, provisioned.JobID, 90*time.Second); job.State != "done" {
		t.Fatalf("provision job state = %q (error=%q), want done", job.State, job.Error)
	}

	// 1. GET /admin/connections — redacted topology + health, NO secret leak.
	connResp := get(t, srv, "/admin/connections")
	rawConn, _ := io.ReadAll(connResp.Body)
	connResp.Body.Close()
	if connResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/connections status = %d; body = %s", connResp.StatusCode, rawConn)
	}
	if strings.Contains(string(rawConn), leakMarker) {
		t.Fatalf("SECURITY: GET /admin/connections serialized a raw DSN (password leak)")
	}
	var conns connectionsResponse
	if err := json.Unmarshal(rawConn, &conns); err != nil {
		t.Fatalf("decode connections: %v", err)
	}
	if conns.Mode != "catalog" {
		t.Errorf("mode = %q, want catalog", conns.Mode)
	}
	if conns.Pool == nil {
		t.Error("catalog mode must report pool occupancy")
	} else if conns.Pool.Cap <= 0 {
		t.Errorf("pool cap = %d, want a positive ceiling", conns.Pool.Cap)
	}

	// The control DB and the sole cluster must both be present, redacted, and
	// reachable; a password-bearing DSN must surface only the masked constant.
	var sawControl, sawCluster bool
	for _, c := range conns.Connections {
		switch c.Name {
		case "catalog-control":
			sawControl = true
		case "cluster:c1":
			sawCluster = true
		}
		if c.Kind == "postgres" {
			if c.Password != "" && c.Password != maskedSecret {
				t.Errorf("connection %q password = %q, want the masked constant", c.Name, c.Password)
			}
			if c.Health == nil || !c.Health.Reachable {
				t.Errorf("connection %q health = %+v, want reachable", c.Name, c.Health)
			}
		}
	}
	if !sawControl {
		t.Error("connections missing the catalog-control entry")
	}
	if !sawCluster {
		t.Error("connections missing the cluster:c1 entry")
	}

	// 2. GET /admin/tenants/acme/connection — the dedicated DB, redacted + healthy.
	tcResp := get(t, srv, "/admin/tenants/acme/connection")
	rawTC, _ := io.ReadAll(tcResp.Body)
	tcResp.Body.Close()
	if tcResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/tenants/acme/connection status = %d; body = %s", tcResp.StatusCode, rawTC)
	}
	if strings.Contains(string(rawTC), leakMarker) {
		t.Fatalf("SECURITY: GET /admin/tenants/acme/connection serialized a raw DSN (password leak)")
	}
	var tc tenantConnectionView
	if err := json.Unmarshal(rawTC, &tc); err != nil {
		t.Fatalf("decode tenant connection: %v", err)
	}
	if tc.TenantID != "acme" {
		t.Errorf("tenantId = %q, want acme", tc.TenantID)
	}
	if tc.ClusterID != "c1" {
		t.Errorf("clusterId = %q, want c1", tc.ClusterID)
	}
	if tc.Database != "fabriq_acme" {
		t.Errorf("database = %q, want fabriq_acme", tc.Database)
	}
	if tc.Connection.Kind != "postgres" || tc.Connection.Database != "fabriq_acme" {
		t.Errorf("connection = %+v, want postgres/fabriq_acme", tc.Connection)
	}
	if tc.Connection.Password != "" && tc.Connection.Password != maskedSecret {
		t.Errorf("tenant connection password = %q, want the masked constant", tc.Connection.Password)
	}
	if tc.Connection.Health == nil || !tc.Connection.Health.Reachable {
		t.Errorf("tenant connection health = %+v, want reachable", tc.Connection.Health)
	}
}

// TestConnections_MultiStore_Health verifies GET /admin/connections lists a
// configured non-Postgres store (Redis) alongside the Postgres topology and
// reports it reachable — exercising the redis.Adapter.Ping health path on real
// infra, not just the Postgres probe.
func TestConnections_MultiStore_Health(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

	adminExt := startConnAdmin(t, ctx, fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN:            dsn,
			ClusterDSNs:    map[string]string{"c1": dsn},
			AllowSuperuser: true,
		},
		Redis: fabriq.RedisConfig{Addr: redisAddr},
	})
	srv := buildServer(t, adminExt)
	defer srv.Close()

	resp := get(t, srv, "/admin/connections")
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/connections status = %d; body = %s", resp.StatusCode, raw)
	}
	var conns connectionsResponse
	if err := json.Unmarshal(raw, &conns); err != nil {
		t.Fatalf("decode connections: %v", err)
	}

	var redis *connectionView
	for i := range conns.Connections {
		if conns.Connections[i].Kind == "redis" {
			redis = &conns.Connections[i]
		}
	}
	if redis == nil {
		t.Fatalf("connections missing the redis entry: %s", raw)
	}
	if redis.Host == "" {
		t.Error("redis connection missing host")
	}
	if redis.Health == nil || !redis.Health.Reachable {
		t.Errorf("redis health = %+v, want reachable", redis.Health)
	}
}

// TestTenantConnection_NotFound verifies an unknown tenant yields 404 from the
// tenant-connection endpoint (catalog present, entry absent).
func TestTenantConnection_NotFound(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)

	adminExt := startCatalogConnAdmin(t, ctx, dsn)
	srv := buildServer(t, adminExt)
	defer srv.Close()

	resp := get(t, srv, "/admin/tenants/ghost/connection")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /admin/tenants/ghost/connection status = %d, want 404; body = %s", resp.StatusCode, body)
	}
}

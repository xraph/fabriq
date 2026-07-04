package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/xraph/fabriq"
)

// TestParsePgDSN_RedactsPassword is the security-critical unit: parsePgDSN
// must extract the non-secret connection fields (host, port, database,
// username, sslmode) and DROP the password entirely — only reporting that one
// was present, never its value.
func TestParsePgDSN_RedactsPassword(t *testing.T) {
	parts, err := parsePgDSN("postgres://fabriq_app:sup3r-s3cret@pg-eu-1.internal:6432/fabriq_acme?sslmode=require")
	if err != nil {
		t.Fatalf("parsePgDSN: %v", err)
	}
	if parts.Host != "pg-eu-1.internal" {
		t.Errorf("host = %q, want pg-eu-1.internal", parts.Host)
	}
	if parts.Port != "6432" {
		t.Errorf("port = %q, want 6432", parts.Port)
	}
	if parts.Database != "fabriq_acme" {
		t.Errorf("database = %q, want fabriq_acme", parts.Database)
	}
	if parts.Username != "fabriq_app" {
		t.Errorf("username = %q, want fabriq_app", parts.Username)
	}
	if parts.SSLMode != "require" {
		t.Errorf("sslMode = %q, want require", parts.SSLMode)
	}
	if !parts.HasPassword {
		t.Error("HasPassword = false, want true (a password was present in the DSN)")
	}
	// The struct must not carry the secret anywhere.
	blob, _ := json.Marshal(parts)
	if strings.Contains(string(blob), "sup3r-s3cret") {
		t.Fatalf("parsed parts leak the password: %s", blob)
	}
}

// TestParsePgDSN_NoPassword reports HasPassword=false when the DSN carries no
// password, so the view can distinguish "secret configured but hidden" from
// "no secret at all".
func TestParsePgDSN_NoPassword(t *testing.T) {
	parts, err := parsePgDSN("postgres://fabriq_ctl@ctl-db:5432/fabriq_control")
	if err != nil {
		t.Fatalf("parsePgDSN: %v", err)
	}
	if parts.HasPassword {
		t.Error("HasPassword = true, want false")
	}
	if parts.Username != "fabriq_ctl" {
		t.Errorf("username = %q, want fabriq_ctl", parts.Username)
	}
}

// TestBuildConnections_NoPasswordLeak is the CENTRAL security assertion the
// product owner mandated: given a config whose every store carries a distinct
// secret, the serialized connection views must contain NONE of them. Where a
// secret exists it is represented only by the masked placeholder.
func TestBuildConnections_NoPasswordLeak(t *testing.T) {
	cfg := fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN: "postgres://ctl:CTL-SECRET@ctl-db:5432/fabriq_control",
			ClusterDSNs: map[string]string{
				"eu-1": "postgres://app:EU-CLUSTER-SECRET@pg-eu-1:5432/postgres",
				"us-1": "postgres://app:US-CLUSTER-SECRET@pg-us-1:5432/postgres",
			},
		},
		Redis:         fabriq.RedisConfig{Addr: "redis:6379", Username: "cache", Password: "REDIS-SECRET"},
		FalkorDB:      fabriq.FalkorDBConfig{Addr: "falkor:6379", Username: "graph", Password: "FALKOR-SECRET"},
		Elasticsearch: fabriq.ElasticsearchConfig{Addrs: []string{"https://es:9200"}, Username: "search", Password: "ELASTIC-SECRET"},
		Storage:       fabriq.StorageConfig{StorageDriver: "s3", DefaultBucket: "fabriq-blobs"},
	}
	secrets := []string{
		"CTL-SECRET", "EU-CLUSTER-SECRET", "US-CLUSTER-SECRET",
		"REDIS-SECRET", "FALKOR-SECRET", "ELASTIC-SECRET",
	}

	views := buildConnections(cfg)
	blob, err := json.Marshal(views)
	if err != nil {
		t.Fatalf("marshal views: %v", err)
	}
	for _, s := range secrets {
		if strings.Contains(string(blob), s) {
			t.Fatalf("connection views leak secret %q: %s", s, blob)
		}
	}

	// Sanity: the redacted topology is actually populated (not silently empty),
	// and password fields, where present, hold only the masked constant.
	if len(views) == 0 {
		t.Fatal("buildConnections returned no connections")
	}
	for _, v := range views {
		if v.Password != "" && v.Password != maskedSecret {
			t.Fatalf("connection %q password = %q, want the masked constant %q", v.Name, v.Password, maskedSecret)
		}
	}
}

// TestScrubSecrets_RedactsDSNUserinfo is defense-in-depth for the health path:
// a store's dial error can embed the raw DSN (with password). Since that string
// lands in the response's error field, scrubSecrets must mask the userinfo
// password before it is ever serialized.
func TestScrubSecrets_RedactsDSNUserinfo(t *testing.T) {
	cases := []struct {
		in       string
		mustDrop string
	}{
		{`dial error: postgres://app:sup3r-s3cret@pg-eu-1:5432/fabriq_acme?sslmode=require: connection refused`, "sup3r-s3cret"},
		{`redis://cache:redis-pw@redis:6379: i/o timeout`, "redis-pw"},
		{`https://search:es-pw@es:9200: no route to host`, "es-pw"},
	}
	for _, tc := range cases {
		got := scrubSecrets(tc.in)
		if strings.Contains(got, tc.mustDrop) {
			t.Errorf("scrubSecrets(%q) = %q, still contains secret %q", tc.in, got, tc.mustDrop)
		}
		// The host must survive so the operator can still diagnose reachability.
		if !strings.Contains(got, "refused") && !strings.Contains(got, "timeout") && !strings.Contains(got, "route") {
			t.Errorf("scrubSecrets(%q) = %q, dropped the diagnostic tail", tc.in, got)
		}
	}

	// A message with no embedded credentials is returned unchanged.
	plain := "context deadline exceeded"
	if got := scrubSecrets(plain); got != plain {
		t.Errorf("scrubSecrets(%q) = %q, want unchanged", plain, got)
	}
}

// TestProbePostgresDSN_UnreachableRedactsError exercises the real leak vector
// end to end: an unreachable Postgres DSN whose userinfo carries a password.
// The probe fails (connection refused), and the resulting health error — which
// the handler serializes — must NOT contain the password.
func TestProbePostgresDSN_UnreachableRedactsError(t *testing.T) {
	// Port 1 is unbindable, so the dial refuses immediately (bounded anyway).
	h := probePostgresDSN(context.Background(), "postgres://app:leak-me-not@127.0.0.1:1/db?sslmode=disable")
	if h.Reachable {
		t.Fatal("probe of an unreachable DSN reported reachable")
	}
	if strings.Contains(h.Error, "leak-me-not") {
		t.Fatalf("health error leaked the password: %q", h.Error)
	}
}

// TestConnectionEndpoints_GateOff verifies both connection endpoints answer
// 403 (capability gate) when WithConnectionsRead is not set.
func TestConnectionEndpoints_GateOff(t *testing.T) {
	e := NewAdminAPI(nil) // ConnectionsRead defaults to false
	srv := buildServer(t, e)
	defer srv.Close()

	for _, path := range []string{"/admin/connections", "/admin/tenants/acme/connection"} {
		resp := getNoTenant(t, srv, path)
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("GET %s status = %d, want 403 (connections.read not enabled)", path, resp.StatusCode)
		}
	}
}

// TestConnectionEndpoints_NoParent verifies that with the gate ON but no parent
// (fake harness), the endpoints answer 400 — not a 500 nil-panic — because the
// connection info is sourced from the started fabriq extension's config.
func TestConnectionEndpoints_NoParent(t *testing.T) {
	e := NewAdminAPI(nil, WithConnectionsRead())
	srv := buildServer(t, e)
	defer srv.Close()

	for _, path := range []string{"/admin/connections", "/admin/tenants/acme/connection"} {
		resp := getNoTenant(t, srv, path)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s status = %d, want 400 (no started fabriq extension)", path, resp.StatusCode)
		}
	}
}

// TestTenantConnectionRoute_Dispatch verifies GET /tenants/:id/connection
// reaches handleTenantConnection (proved by its distinct nil-parent 400 body)
// AND that registering it did not shadow the existing static /tenants/jobs/:id
// route (which must still answer with handleTenantJob's "no such job" body).
func TestTenantConnectionRoute_Dispatch(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithConnectionsRead(), WithTenantsAdmin())
	srv := buildServer(t, e)
	defer srv.Close()

	// /tenants/acme/connection → handleTenantConnection. Gate is on but parent
	// is nil, so requireConnectionsRead returns its distinct 400 body. A router
	// 404 or a tenant-lookup body here would mean the route never registered or
	// was shadowed.
	connResp := get(t, srv, "/admin/tenants/acme/connection")
	defer connResp.Body.Close()
	if connResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("GET /tenants/acme/connection status = %d, want 400", connResp.StatusCode)
	}
	var connBody map[string]any
	if err := json.NewDecoder(connResp.Body).Decode(&connBody); err != nil {
		t.Fatalf("decode connection 400 body: %v", err)
	}
	if connBody["error"] != "connection info requires a started fabriq extension" {
		t.Fatalf("GET /tenants/acme/connection body = %v, want the connections-read no-parent error (proves dispatch reached handleTenantConnection)", connBody)
	}

	// Conversely /tenants/jobs/nope must still reach handleTenantJob, unchanged.
	jobResp := get(t, srv, "/admin/tenants/jobs/nope")
	defer jobResp.Body.Close()
	if jobResp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /tenants/jobs/nope status = %d, want 404", jobResp.StatusCode)
	}
	var jobBody map[string]string
	if err := json.NewDecoder(jobResp.Body).Decode(&jobBody); err != nil {
		t.Fatalf("decode job 404 body: %v", err)
	}
	if jobBody["error"] != "no such job" {
		t.Fatalf("GET /tenants/jobs/nope body = %v, want {error: \"no such job\"} (adding /tenants/:id/connection must not shadow the static job route)", jobBody)
	}
}

// TestConnectionsCapability verifies connections.read is advertised in
// GET /admin/meta only when WithConnectionsRead is set (mirrors the
// tenants.admin / schema.admin dynamic-capability convention).
func TestConnectionsCapability(t *testing.T) {
	world := buildTestWorld(t)

	has := func(caps []string, want string) bool {
		for _, c := range caps {
			if c == want {
				return true
			}
		}
		return false
	}

	off := fakeBackedAdminExt(t, world)
	srvOff := buildServer(t, off)
	defer srvOff.Close()
	respOff := get(t, srvOff, "/admin/meta")
	defer respOff.Body.Close()
	var metaOff metaResponse
	if err := json.NewDecoder(respOff.Body).Decode(&metaOff); err != nil {
		t.Fatalf("decode meta (off): %v", err)
	}
	if has(metaOff.Capabilities, "connections.read") {
		t.Fatal("connections.read must not be advertised when ConnectionsRead is off")
	}

	on := fakeBackedAdminExt(t, world, WithConnectionsRead())
	srvOn := buildServer(t, on)
	defer srvOn.Close()
	respOn := get(t, srvOn, "/admin/meta")
	defer respOn.Body.Close()
	var metaOn metaResponse
	if err := json.NewDecoder(respOn.Body).Decode(&metaOn); err != nil {
		t.Fatalf("decode meta (on): %v", err)
	}
	if !has(metaOn.Capabilities, "connections.read") {
		t.Fatal("connections.read must be advertised when ConnectionsRead is on")
	}
}

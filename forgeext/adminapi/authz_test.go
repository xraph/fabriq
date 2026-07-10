package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

func TestFlagAuthorizer_Parity(t *testing.T) {
	cases := []struct {
		cfg  config
		cap  string
		want bool
	}{
		{config{AnalyticsAdmin: true}, "analytics.admin", true},
		{config{}, "analytics.admin", false},
		{config{AnalyticsAdmin: true}, "analytics.read", true}, // admin implies read
		{config{AnalyticsRead: true}, "analytics.read", true},
		{config{AnalyticsRead: true}, "analytics.admin", false},
		{config{}, "analytics.read", false},
		{config{SchemaAdmin: true}, "schema.admin", true},
		{config{}, "schema.admin", false},
		{config{TenantsAdmin: true}, "tenants.admin", true},
		{config{}, "tenants.admin", false},
		{config{ConnectionsRead: true}, "connections.read", true},
		{config{}, "connections.read", false},
		{config{}, "entities.read", true}, // base caps ungated
		{config{}, "query.raw", true},
	}
	for _, tc := range cases {
		cfg := tc.cfg
		got, err := flagAuthorizer(&cfg).Authorize(context.Background(), tc.cap)
		if err != nil || got != tc.want {
			t.Errorf("Authorize(%q) cfg=%+v = %v,%v want %v", tc.cap, tc.cfg, got, err, tc.want)
		}
	}
}

// A denying authorizer must 403 a mutating analytics endpoint EVEN with
// WithAnalyticsAdmin() set — the authorizer overrides the global flag.
func TestAuthorizer_DeniesAnalyticsAdminDespiteFlag(t *testing.T) {
	deny := AuthorizerFunc(func(_ context.Context, capName string) (bool, error) {
		return capName != "analytics.admin", nil
	})
	e := NewAdminAPI(nil, WithAnalyticsAdmin(), WithAuthorizer(deny))
	srv := buildServer(t, e)
	defer srv.Close()
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/analytics/backfill", map[string]any{"tenant": "acme"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (authorizer denies analytics.admin despite the flag)", resp.StatusCode)
	}
}

// A denying authorizer must 403 a schema-admin endpoint even with WithSchemaAdmin().
func TestAuthorizer_DeniesSchemaAdminDespiteFlag(t *testing.T) {
	deny := AuthorizerFunc(func(_ context.Context, capName string) (bool, error) {
		return capName != "schema.admin", nil
	})
	e := NewAdminAPI(nil, WithSchemaAdmin(), WithAuthorizer(deny))
	srv := buildServer(t, e)
	defer srv.Close()
	// The migrations run endpoint is schema.admin-gated.
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/migrations/up", map[string]any{})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (authorizer denies schema.admin despite the flag)", resp.StatusCode)
	}
}

func TestMeta_ReflectsAuthorizer(t *testing.T) {
	// Allow base caps + analytics.read; deny the other gated admin caps.
	auth := AuthorizerFunc(func(_ context.Context, capName string) (bool, error) {
		switch capName {
		case "analytics.admin", "schema.admin", "tenants.admin", "connections.read":
			return false, nil
		default:
			return true, nil // base caps + analytics.read
		}
	})
	e := NewAdminAPI(nil, WithAuthorizer(auth))
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
	has := func(c string) bool {
		for _, x := range body.Capabilities {
			if x == c {
				return true
			}
		}
		return false
	}
	if !has("analytics.read") {
		t.Fatalf("meta caps %v: want analytics.read (authorizer allowed it)", body.Capabilities)
	}
	if has("analytics.admin") || has("schema.admin") {
		t.Fatalf("meta caps %v: must NOT include denied gated caps", body.Capabilities)
	}
	if !has("entities.read") {
		t.Fatalf("meta caps %v: base caps must still be present", body.Capabilities)
	}
}

// An authorizer that errors must fail CLOSED: the gate returns 500, never allow.
func TestAuthorizer_ErrorFailsClosed(t *testing.T) {
	boom := AuthorizerFunc(func(_ context.Context, _ string) (bool, error) {
		return true, errors.New("authz backend down") // allowed=true but err set
	})
	e := NewAdminAPI(nil, WithAnalyticsRead(), WithAuthorizer(boom))
	srv := buildServer(t, e)
	defer srv.Close()
	resp := getNoTenant(t, srv, "/admin/analytics/status")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (authorizer error must fail closed, not allow)", resp.StatusCode)
	}
}

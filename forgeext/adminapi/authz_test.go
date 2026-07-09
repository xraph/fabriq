package adminapi

import (
	"context"
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
	deny := AuthorizerFunc(func(_ context.Context, cap string) (bool, error) {
		return cap != "analytics.admin", nil
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

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/tenant"
)

func TestPartitionResolve(t *testing.T) {
	base := context.Background()
	tctx, err := tenant.WithTenant(base, "acme")
	if err != nil {
		t.Fatal(err)
	}
	sctx, err := tenant.WithScope(tctx, "projA")
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		p    cache.Partition
		ctx  context.Context
		want string
	}{
		{"global ignores tenant", cache.Global, sctx, "g"},
		{"tenant", cache.Tenant, tctx, "t:acme"},
		{"tenant scope", cache.TenantScope, sctx, "t:acme:s:projA"},
		{"tenant scope empty scope", cache.TenantScope, tctx, "t:acme:s:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.p.Resolve(tc.ctx)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestPartitionResolveRequiresTenant(t *testing.T) {
	if _, err := cache.Tenant.Resolve(context.Background()); err == nil {
		t.Fatal("expected error when tenant missing for Tenant partition")
	}
	// Global never requires a tenant.
	if _, err := cache.Global.Resolve(context.Background()); err != nil {
		t.Fatalf("Global must not require tenant: %v", err)
	}
}

func TestJSONCodecRoundTrip(t *testing.T) {
	type row struct {
		Name string `json:"name"`
		N    int    `json:"n"`
	}
	var c cache.JSON
	in := row{Name: "x", N: 7}
	b, err := c.Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	var out row
	if err := c.Decode(b, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestFingerprintDeterministicAndSensitive(t *testing.T) {
	type q struct {
		Entity string
		Where  map[string]any
		Limit  int
	}
	a, err := cache.Fingerprint(q{Entity: "asset", Where: map[string]any{"site": "s1", "ok": true}, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	// Same logical query (map order differs in literal, json sorts keys) → same fp.
	b, err := cache.Fingerprint(q{Entity: "asset", Where: map[string]any{"ok": true, "site": "s1"}, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("expected stable fingerprint, got %q vs %q", a, b)
	}
	// Different query → different fp.
	c, err := cache.Fingerprint(q{Entity: "asset", Where: map[string]any{"site": "s2"}, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if a == c {
		t.Fatal("expected different fingerprint for different query")
	}
}

// Compile-time assertion that Keyspace/Policy/Cache types exist with the
// expected shape (kept here so later tasks can rely on the names).
var _ = func() cache.Keyspace {
	return cache.Keyspace{
		Name:      "asset.byid",
		Version:   1,
		Partition: cache.TenantScope,
		Policy:    cache.Policy{Mode: cache.EventEvict, TTL: 10 * time.Minute},
	}
}

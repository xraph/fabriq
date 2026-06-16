package tenant_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/tenant"
)

func TestScope_RoundTrip(t *testing.T) {
	ctx := context.Background()
	if s, ok := tenant.ScopeFromContext(ctx); ok || s != "" {
		t.Fatalf("unscoped ctx: want ('',false), got (%q,%v)", s, ok)
	}
	if got := tenant.ScopeOrEmpty(ctx); got != "" {
		t.Fatalf("ScopeOrEmpty unscoped: want '', got %q", got)
	}
	ctx2, err := tenant.WithScope(ctx, "proj_A")
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := tenant.ScopeFromContext(ctx2); !ok || s != "proj_A" {
		t.Fatalf("scoped ctx: want ('proj_A',true), got (%q,%v)", s, ok)
	}
	if got := tenant.ScopeOrEmpty(ctx2); got != "proj_A" {
		t.Fatalf("ScopeOrEmpty: want 'proj_A', got %q", got)
	}
}

func TestScope_Validation(t *testing.T) {
	if _, err := tenant.WithScope(context.Background(), "bad/scope"); err == nil {
		t.Fatal("invalid scope must error")
	}
	ctx, _ := tenant.WithTenant(context.Background(), "ws_1")
	ctx, _ = tenant.WithScope(ctx, "proj_A")
	if tid, _ := tenant.FromContext(ctx); tid != "ws_1" {
		t.Fatalf("tenant clobbered: %q", tid)
	}
}

package shard

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/pathctx"
	"github.com/xraph/fabriq/core/tenant"
)

func schemaRouterOverStub() *SchemaRouter {
	// A degenerate one-shard Set stands in for the shared consolidation
	// database (a DynamicSet in production); the decorator's job is the ctx
	// stamp, which is independent of how inner resolves the shard.
	inner := Single(Shard{ID: "c1/pool_a"})
	return NewSchemaRouter(inner)
}

func TestSchemaRouter_StampsResolvedSchema(t *testing.T) {
	r := schemaRouterOverStub()
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	sh, sctx, release, err := r.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer release()
	if sh.ID != "c1/pool_a" {
		t.Fatalf("shard id = %q", sh.ID)
	}
	if got := pathctx.SchemaOrEmpty(sctx); got != "tenant_acme" {
		t.Fatalf("search_path schema = %q, want tenant_acme", got)
	}
	// The input ctx must be untouched — only the returned ctx carries it.
	if pathctx.SchemaOrEmpty(ctx) != "" {
		t.Fatal("input ctx was mutated")
	}
}

func TestSchemaRouter_AcquireForUsesArgTenant(t *testing.T) {
	r := schemaRouterOverStub()
	// No tenant on ctx; AcquireFor takes it explicitly (the worker/sweeper path).
	_, sctx, release, err := r.AcquireFor(context.Background(), "beta-co")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer release()
	if got := pathctx.SchemaOrEmpty(sctx); got != "tenant_beta_co" {
		t.Fatalf("schema = %q, want tenant_beta_co (hyphen folded)", got)
	}
}

func TestSchemaRouter_RejectsUnmappableTenant(t *testing.T) {
	r := schemaRouterOverStub()
	// Uppercase ids cannot map to a legal lowercase schema — fail the route,
	// never stamp a colliding path.
	if _, _, _, err := r.AcquireFor(context.Background(), "ACME"); err == nil {
		t.Fatal("expected unmappable tenant to fail the route")
	}
}

func TestSchemaRouter_MissingTenantOnAcquire(t *testing.T) {
	r := schemaRouterOverStub()
	if _, _, _, err := r.Acquire(context.Background()); err == nil {
		t.Fatal("Acquire without a tenant on ctx must fail")
	} else if fabriqerr.CodeOf(err) == fabriqerr.CodeUnavailable {
		t.Fatalf("unexpected code for missing tenant: %v", err)
	}
}

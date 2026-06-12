package tenant_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/tenant"
)

func TestWithTenant_RoundTrip(t *testing.T) {
	ctx, err := tenant.WithTenant(context.Background(), "acme-01")
	if err != nil {
		t.Fatalf("WithTenant: %v", err)
	}
	got, err := tenant.FromContext(ctx)
	if err != nil {
		t.Fatalf("FromContext: %v", err)
	}
	if got != "acme-01" {
		t.Fatalf("FromContext = %q, want %q", got, "acme-01")
	}
}

func TestFromContext_MissingTenant(t *testing.T) {
	_, err := tenant.FromContext(context.Background())
	if !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("want ErrNoTenant, got %v", err)
	}
}

func TestWithTenant_RejectsInvalidIDs(t *testing.T) {
	cases := []struct {
		name string
		id   string
	}{
		{"empty", ""},
		{"whitespace", "ten ant"},
		{"colon breaks channel derivation", "ten:ant"},
		{"slash breaks index naming", "ten/ant"},
		{"quote breaks cypher params", `ten"ant`},
		{"too long", strings.Repeat("a", 65)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tenant.WithTenant(context.Background(), tc.id)
			if err == nil {
				t.Fatalf("WithTenant(%q) accepted invalid tenant id", tc.id)
			}
		})
	}
}

func TestWithTenant_AcceptsTypicalIDs(t *testing.T) {
	for _, id := range []string{"t1", "acme_prod", "01HXYZABC", "a-b-c", strings.Repeat("a", 64)} {
		if _, err := tenant.WithTenant(context.Background(), id); err != nil {
			t.Fatalf("WithTenant(%q) rejected valid tenant id: %v", id, err)
		}
	}
}

func TestRequire_IsTheStructuralGuard(t *testing.T) {
	// Require is the single enforcement point: same behavior as FromContext,
	// re-exported under the guard name so call sites read as assertions.
	if _, err := tenant.Require(context.Background()); !errors.Is(err, tenant.ErrNoTenant) {
		t.Fatalf("Require on unstamped ctx: want ErrNoTenant, got %v", err)
	}
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	id, err := tenant.Require(ctx)
	if err != nil || id != "acme" {
		t.Fatalf("Require = (%q, %v), want (acme, nil)", id, err)
	}
}

func TestMustWithTenant_PanicsOnInvalid(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustWithTenant must panic on invalid id")
		}
	}()
	tenant.MustWithTenant(context.Background(), "bad tenant!")
}

func BenchmarkFromContext(b *testing.B) {
	ctx, _ := tenant.WithTenant(context.Background(), "acme-benchmark")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := tenant.FromContext(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWithTenant(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := tenant.WithTenant(ctx, "acme-benchmark"); err != nil {
			b.Fatal(err)
		}
	}
}

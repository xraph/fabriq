package registry_test

// Phase 1 of the db-per-tenant design (spec 2026-07-03): hosts embedding
// fabriq next to their own tables can namespace fabriq's STATIC entity
// tables with registry.WithTablePrefix. fabriq_* infra and ds_* dynamic
// tables are already namespaces and are never double-prefixed.

import (
	"strings"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

type prefixWidget struct {
	grove.BaseModel `grove:"table:widgets"`
	ID              string `grove:"id,pk" json:"id"`
	TenantID        string `grove:"tenant_id" json:"tenantId"`
	Version         int64  `grove:"version" json:"version"`
	Name            string `grove:"name" json:"name"`
}

type prefixedInfra struct {
	grove.BaseModel `grove:"table:fabriq_links"`
	ID              string `grove:"id,pk" json:"id"`
	TenantID        string `grove:"tenant_id" json:"tenantId"`
	Version         int64  `grove:"version" json:"version"`
}

func widgetSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name:  "widget",
		Kind:  registry.KindAggregate,
		Model: (*prefixWidget)(nil),
	}
}

func TestTablePrefix_AppliedToStaticEntities(t *testing.T) {
	reg := registry.New(registry.WithTablePrefix("acme_"))
	if err := reg.Register(widgetSpec()); err != nil {
		t.Fatal(err)
	}
	ent, ok := reg.Get("widget")
	if !ok {
		t.Fatal("widget not registered")
	}
	if ent.Binding.Table != "acme_widgets" {
		t.Fatalf("table = %q, want acme_widgets", ent.Binding.Table)
	}
}

func TestTablePrefix_EmptyIsNoop(t *testing.T) {
	reg := registry.New()
	if err := reg.Register(widgetSpec()); err != nil {
		t.Fatal(err)
	}
	ent, _ := reg.Get("widget")
	if ent.Binding.Table != "widgets" {
		t.Fatalf("table = %q, want widgets", ent.Binding.Table)
	}
}

func TestTablePrefix_NeverDoublePrefixesFabriqTables(t *testing.T) {
	reg := registry.New(registry.WithTablePrefix("acme_"))
	if err := reg.Register(registry.EntitySpec{
		Name:  "link",
		Kind:  registry.KindAggregate,
		Model: (*prefixedInfra)(nil),
	}); err != nil {
		t.Fatal(err)
	}
	ent, _ := reg.Get("link")
	if ent.Binding.Table != "fabriq_links" {
		t.Fatalf("fabriq_* tables must not be prefixed; got %q", ent.Binding.Table)
	}
}

func TestTablePrefix_RejectsInvalid(t *testing.T) {
	for _, bad := range []string{"Acme_", "1acme_", "acme", "acme-", `acme"; DROP TABLE x;--_`} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("WithTablePrefix(%q) must panic (wiring-time misconfiguration)", bad)
				}
			}()
			registry.New(registry.WithTablePrefix(bad))
		}()
	}
	// Valid shapes do not panic.
	for _, ok := range []string{"", "acme_", "twin_os2_"} {
		reg := registry.New(registry.WithTablePrefix(ok))
		if reg == nil {
			t.Fatalf("WithTablePrefix(%q) returned nil registry", ok)
		}
	}
	_ = strings.TrimSpace("")
}

// BenchmarkRegistryBind_WithPrefix pins the cost of prefix application at
// registration time (the request path only ever reads Binding.Table, so
// the prefix must cost nothing after Register).
func BenchmarkRegistryBind_WithPrefix(b *testing.B) {
	for _, prefix := range []string{"", "acme_"} {
		name := "prefix=" + prefix
		if prefix == "" {
			name = "prefix=none"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				reg := registry.New(registry.WithTablePrefix(prefix))
				if err := reg.Register(widgetSpec()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRegistryLookup_HotPath pins the per-request cost: Get must not
// regress with a prefix configured (it is applied once at Register).
func BenchmarkRegistryLookup_HotPath(b *testing.B) {
	reg := registry.New(registry.WithTablePrefix("acme_"))
	if err := reg.Register(widgetSpec()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ent, ok := reg.Get("widget")
		if !ok || ent.Binding.Table != "acme_widgets" {
			b.Fatal("lookup failed")
		}
	}
}

package registry_test

import (
	"strings"
	"testing"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

// Test models mirror the TWINOS shapes without importing domain/ (core stays
// domain-agnostic; these live only in this test).
type testSite struct {
	grove.BaseModel `grove:"table:sites"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
}

type testAsset struct {
	grove.BaseModel `grove:"table:assets"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
	SiteID   string `grove:"site_id"`
	ParentID string `grove:"parent_id"`
}

type testNoVersion struct {
	grove.BaseModel `grove:"table:no_versions"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Name     string `grove:"name"`
}

type testNoTenant struct {
	grove.BaseModel `grove:"table:no_tenants"`

	ID      string `grove:"id,pk"`
	Version int64  `grove:"version,notnull"`
}

func siteSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name:      "site",
		Kind:      registry.KindAggregate,
		Model:     (*testSite)(nil),
		GraphNode: "Site",
		Search:    registry.SearchSpec{Index: "sites", Fields: []string{"name"}},
		Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
	}
}

func assetSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name:      "asset",
		Kind:      registry.KindAggregate,
		Model:     (*testAsset)(nil),
		GraphNode: "Asset",
		Edges: []registry.EdgeSpec{
			{Field: "site_id", Rel: "LOCATED_AT", Target: "site"},
			{Field: "parent_id", Rel: "CHILD_OF", Target: "asset"},
		},
		Search:    registry.SearchSpec{Index: "assets", Fields: []string{"name"}},
		Subscribe: []registry.Scope{registry.ByID, registry.ByField("site", "site_id"), registry.ByTenant},
	}
}

func TestRegister_BindsGroveModel(t *testing.T) {
	r := registry.New()
	if err := r.Register(siteSpec()); err != nil {
		t.Fatalf("Register(site): %v", err)
	}

	ent, ok := r.Get("site")
	if !ok {
		t.Fatal("Get(site) not found after Register")
	}
	b := ent.Binding
	if b.Table != "sites" {
		t.Errorf("Table = %q, want sites", b.Table)
	}
	if b.PK != "id" || b.TenantColumn != "tenant_id" || b.VersionColumn != "version" {
		t.Errorf("structural columns = (%q,%q,%q)", b.PK, b.TenantColumn, b.VersionColumn)
	}
	for _, col := range []string{"id", "tenant_id", "version", "name"} {
		if !b.HasColumn(col) {
			t.Errorf("HasColumn(%q) = false", col)
		}
	}
}

func TestRegister_Failures(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*registry.EntitySpec)
		wantSub string
	}{
		{"empty name", func(s *registry.EntitySpec) { s.Name = "" }, "name"},
		{"nil model", func(s *registry.EntitySpec) { s.Model = nil }, "model"},
		{"search field not a column", func(s *registry.EntitySpec) { s.Search.Fields = []string{"nope"} }, "nope"},
		{"scope field not a column", func(s *registry.EntitySpec) {
			s.Subscribe = []registry.Scope{registry.ByField("x", "nope")}
		}, "nope"},
		{"graph node without label", func(s *registry.EntitySpec) { s.GraphNode = "" ; s.Edges = []registry.EdgeSpec{{Field: "name", Rel: "R", Target: "site"}} }, "GraphNode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := siteSpec()
			tc.mutate(&spec)
			err := registry.New().Register(spec)
			if err == nil {
				t.Fatalf("Register accepted invalid spec")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

func TestRegister_AggregateRequiresStructuralColumns(t *testing.T) {
	r := registry.New()

	noVersion := registry.EntitySpec{Name: "nv", Kind: registry.KindAggregate, Model: (*testNoVersion)(nil)}
	if err := r.Register(noVersion); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("want version-column error, got %v", err)
	}

	noTenant := registry.EntitySpec{Name: "nt", Kind: registry.KindAggregate, Model: (*testNoTenant)(nil)}
	if err := r.Register(noTenant); err == nil || !strings.Contains(err.Error(), "tenant_id") {
		t.Fatalf("want tenant-column error, got %v", err)
	}
}

func TestRegister_DuplicateName(t *testing.T) {
	r := registry.New()
	if err := r.Register(siteSpec()); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(siteSpec()); err == nil {
		t.Fatal("duplicate Register must fail")
	}
}

func TestRegister_EdgeFieldMustBeColumn(t *testing.T) {
	spec := assetSpec()
	spec.Edges[0].Field = "not_a_column"
	if err := registry.New().Register(spec); err == nil {
		t.Fatal("edge with unknown field must fail registration")
	}
}

func TestValidate_EdgeTargetsMustBeRegistered(t *testing.T) {
	r := registry.New()
	if err := r.Register(assetSpec()); err != nil {
		t.Fatalf("Register(asset): %v", err)
	}
	// site not registered: Validate must fail mentioning the missing target.
	err := r.Validate()
	if err == nil || !strings.Contains(err.Error(), "site") {
		t.Fatalf("want missing-target error mentioning site, got %v", err)
	}
	if err := r.Register(siteSpec()); err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("Validate after registering target: %v", err)
	}
}

func TestRegister_DocumentKindRequiresCRDTSpec(t *testing.T) {
	doc := registry.EntitySpec{
		Name:  "page",
		Kind:  registry.KindDocument,
		Model: (*testSite)(nil), // shape is irrelevant here
	}
	if err := registry.New().Register(doc); err == nil || !strings.Contains(err.Error(), "CRDT") {
		t.Fatalf("document kind without CRDT spec must fail, got %v", err)
	}
	doc.CRDT = &registry.CRDTSpec{Engine: "grove-crdt", SnapshotEvery: 100, QuietWindow: 2 * time.Second}
	if err := registry.New().Register(doc); err != nil {
		t.Fatalf("document kind with CRDT spec: %v", err)
	}
}

func TestAll_SortedByName(t *testing.T) {
	r := registry.New()
	if err := r.Register(assetSpec()); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(siteSpec()); err != nil {
		t.Fatal(err)
	}
	all := r.All()
	if len(all) != 2 || all[0].Spec.Name != "asset" || all[1].Spec.Name != "site" {
		t.Fatalf("All() not sorted by name: %v", []string{all[0].Spec.Name, all[1].Spec.Name})
	}
}

func TestValuesByColumn(t *testing.T) {
	r := registry.New()
	if err := r.Register(siteSpec()); err != nil {
		t.Fatal(err)
	}
	ent, _ := r.Get("site")
	vals, err := ent.Binding.ValuesByColumn(&testSite{
		ID: "01H", TenantID: "acme", Version: 3, Name: "Plant A",
	})
	if err != nil {
		t.Fatalf("ValuesByColumn: %v", err)
	}
	want := map[string]any{"id": "01H", "tenant_id": "acme", "version": int64(3), "name": "Plant A"}
	for k, v := range want {
		if vals[k] != v {
			t.Errorf("vals[%q] = %v, want %v", k, vals[k], v)
		}
	}
}

func TestValuesByColumn_RejectsWrongType(t *testing.T) {
	r := registry.New()
	if err := r.Register(siteSpec()); err != nil {
		t.Fatal(err)
	}
	ent, _ := r.Get("site")
	if _, err := ent.Binding.ValuesByColumn(&testAsset{}); err == nil {
		t.Fatal("ValuesByColumn must reject a model of the wrong type")
	}
}

func BenchmarkRegistryGet(b *testing.B) {
	r := registry.New()
	if err := r.Register(siteSpec()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := r.Get("site"); !ok {
			b.Fatal("missing")
		}
	}
}

func BenchmarkValuesByColumn(b *testing.B) {
	r := registry.New()
	if err := r.Register(siteSpec()); err != nil {
		b.Fatal(err)
	}
	ent, _ := r.Get("site")
	model := &testSite{ID: "01H", TenantID: "acme", Version: 3, Name: "Plant A"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ent.Binding.ValuesByColumn(model); err != nil {
			b.Fatal(err)
		}
	}
}

package registry_test

import (
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

type analyticsModel struct {
	grove.BaseModel `grove:"table:analytics_test"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name"`
}

func analyticsSpec(a *registry.AnalyticsSpec) registry.EntitySpec {
	return registry.EntitySpec{
		Name:      "widget",
		Kind:      registry.KindAggregate,
		Model:     analyticsModel{},
		Analytics: a,
	}
}

func TestRegister_AnalyticsSpecRecorded(t *testing.T) {
	r := registry.New()
	if err := r.Register(analyticsSpec(&registry.AnalyticsSpec{Include: []string{"name"}})); err != nil {
		t.Fatalf("register: %v", err)
	}
	ent, ok := r.Get("widget")
	if !ok {
		t.Fatal("entity not found")
	}
	if ent.Spec.Analytics == nil || len(ent.Spec.Analytics.Include) != 1 || ent.Spec.Analytics.Include[0] != "name" {
		t.Fatalf("analytics spec not recorded: %+v", ent.Spec.Analytics)
	}
}

func TestRegister_AnalyticsNilByDefault(t *testing.T) {
	r := registry.New()
	if err := r.Register(analyticsSpec(nil)); err != nil {
		t.Fatalf("register: %v", err)
	}
	ent, _ := r.Get("widget")
	if ent.Spec.Analytics != nil {
		t.Fatal("expected nil analytics spec by default")
	}
}

func TestRegister_AnalyticsEmptySpecRejected(t *testing.T) {
	r := registry.New()
	err := r.Register(analyticsSpec(&registry.AnalyticsSpec{})) // neither IncludeAll nor Include
	if err == nil {
		t.Fatal("expected error for empty analytics spec")
	}
}

package forgeext

import (
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

type amodel struct {
	grove.BaseModel `grove:"table:amodels"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
}

func TestHasAnalyticsEntity(t *testing.T) {
	empty := registry.New()
	if err := empty.Register(registry.EntitySpec{Name: "a", Kind: registry.KindAggregate, Model: (*amodel)(nil)}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if hasAnalyticsEntity(empty) {
		t.Fatal("no marked entity => false")
	}
	marked := registry.New()
	if err := marked.Register(registry.EntitySpec{Name: "a", Kind: registry.KindAggregate, Model: (*amodel)(nil), Analytics: &registry.AnalyticsSpec{IncludeAll: true}}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !hasAnalyticsEntity(marked) {
		t.Fatal("marked entity => true")
	}
}

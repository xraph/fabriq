package registry_test

import (
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

type liveAsset struct {
	grove.BaseModel `grove:"table:live_assets"`

	ID       string `grove:"id,pk" json:"id"`
	TenantID string `grove:"tenant_id,notnull" json:"tenant_id"`
	Version  int64  `grove:"version,notnull" json:"version"`
	Name     string `grove:"name,notnull" json:"name"`
	Status   string `grove:"status" json:"status"`
}

func TestLiveSpecValidation(t *testing.T) {
	r := registry.New()
	err := r.Register(registry.EntitySpec{
		Name:  "asset",
		Kind:  registry.KindAggregate,
		Model: (*liveAsset)(nil),
		Live:  &registry.LiveSpec{Sortable: []string{"name"}, MaxWindow: 200},
	})
	if err != nil {
		t.Fatalf("valid live spec rejected: %v", err)
	}

	bad := registry.New()
	err = bad.Register(registry.EntitySpec{
		Name:  "asset",
		Kind:  registry.KindAggregate,
		Model: (*liveAsset)(nil),
		Live:  &registry.LiveSpec{Sortable: []string{"nonexistent"}},
	})
	if err == nil {
		t.Fatal("expected rejection of non-existent sortable column")
	}
}

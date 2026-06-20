// core/registry/distill_spec_test.go
package registry_test

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func TestRegistry_DistillSpecValidation(t *testing.T) {
	r := registry.New()
	// SourceFields referencing a non-column must fail.
	r.MustRegister(registry.EntitySpec{
		Name: "dnbad", Kind: registry.KindAggregate, Model: (*embDoc)(nil),
		Distill: &registry.DistillSpec{SourceFields: []string{"nope"}},
	})
	if err := r.Validate(); err == nil {
		t.Fatal("expected validation error for Distill field that is not a column")
	}

	// Text override with no SourceFields must pass.
	r2 := registry.New()
	r2.MustRegister(registry.EntitySpec{
		Name: "dnok", Kind: registry.KindAggregate, Model: (*embDoc)(nil),
		Distill: &registry.DistillSpec{Text: func(map[string]any) string { return "x" }, Scopes: []string{"site"}},
	})
	if err := r2.Validate(); err != nil {
		t.Fatalf("valid Distill spec rejected: %v", err)
	}

	// nil Text with empty SourceFields must fail (neither source provided).
	r3 := registry.New()
	r3.MustRegister(registry.EntitySpec{
		Name: "dnempty", Kind: registry.KindAggregate, Model: (*embDoc)(nil),
		Distill: &registry.DistillSpec{SourceFields: []string{}},
	})
	if err := r3.Validate(); err == nil {
		t.Fatal("expected validation error for Distill with nil Text and empty SourceFields")
	}
}

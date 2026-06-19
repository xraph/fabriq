// core/registry/embed_spec_test.go
package registry_test

import (
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

type embDoc struct {
	grove.BaseModel `grove:"table:embdocs"`
	ID              string `grove:"id,pk"`
	TenantID        string `grove:"tenant_id,notnull"`
	Version         int64  `grove:"version,notnull"`
	Title           string `grove:"title,notnull"`
}

func TestValidate_EmbedFieldsMustBeColumns(t *testing.T) {
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "embdoc", Kind: registry.KindAggregate, Model: (*embDoc)(nil),
		Embed: &registry.EmbedSpec{Fields: []string{"nope"}}, // not a column
	})
	if err := r.Validate(); err == nil {
		t.Fatal("want validation error for embed field that is not a column")
	}
}

func TestValidate_EmbedValidFieldsOK(t *testing.T) {
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "embdoc", Kind: registry.KindAggregate, Model: (*embDoc)(nil),
		Embed: &registry.EmbedSpec{Fields: []string{"title"}},
	})
	if err := r.Validate(); err != nil {
		t.Fatalf("valid embed spec rejected: %v", err)
	}
}

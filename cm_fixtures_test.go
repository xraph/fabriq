//go:build integration || loadtest

package fabriq_test

// Shared catalog-mode test fixtures: a minimal aggregate + CRDT document
// registry and the app-owned DDL applied inside each tenant database.

import (
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/registry"
)

type cmWidget struct {
	grove.BaseModel `grove:"table:cm_widgets"`
	ID              string `grove:"id,pk" json:"id"`
	TenantID        string `grove:"tenant_id,notnull" json:"tenantId"`
	Version         int64  `grove:"version,notnull" json:"version"`
	Name            string `grove:"name,notnull" json:"name"`
}

type cmNote struct {
	grove.BaseModel `grove:"table:cm_notes"`
	ID              string `grove:"id,pk" json:"id"`
	TenantID        string `grove:"tenant_id,notnull" json:"tenantId"`
	Version         int64  `grove:"version,notnull" json:"version"`
	Title           string `grove:"title" json:"title"`
}

func cmRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name: "cmwidget", Kind: registry.KindAggregate, Model: (*cmWidget)(nil),
		Search: registry.SearchSpec{Index: "cmwidgets", Fields: []string{"name"}},
		Live:   &registry.LiveSpec{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(registry.EntitySpec{
		Name: "cmnote", Kind: registry.KindDocument, Model: (*cmNote)(nil),
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt", SnapshotEvery: 64, QuietWindow: 0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatal(err)
	}
	return reg
}

func cmRegistryArchive(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := reg.Register(registry.EntitySpec{
		Name: "cmnote", Kind: registry.KindDocument, Model: (*cmNote)(nil),
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt", SnapshotEvery: 2, QuietWindow: 0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatal(err)
	}
	return reg
}

// cmDDL creates the app-owned entity tables inside one tenant database.
func cmDDL() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS cm_widgets (
			id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, version BIGINT NOT NULL, name TEXT NOT NULL)`,
		`ALTER TABLE cm_widgets ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE cm_widgets FORCE ROW LEVEL SECURITY`,
		`DROP POLICY IF EXISTS tenant_isolation ON cm_widgets`,
		`CREATE POLICY tenant_isolation ON cm_widgets
			USING (tenant_id = current_setting('app.tenant_id', true))
			WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`,
		`CREATE TABLE IF NOT EXISTS cm_notes (
			id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, version BIGINT NOT NULL, title TEXT NOT NULL DEFAULT '')`,
	}
}

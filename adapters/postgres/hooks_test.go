package postgres

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func TestBackstopAddRemoveTable(t *testing.T) {
	b := newTenantBackstop(registry.New(), nil)
	if b.isTenantTable("ds_widget") {
		t.Fatal("unexpected guarded table before Add")
	}
	b.AddTable("ds_widget")
	if !b.isTenantTable("ds_widget") {
		t.Fatal("AddTable did not guard the table")
	}
	b.RemoveTable("ds_widget")
	if b.isTenantTable("ds_widget") {
		t.Fatal("RemoveTable did not unguard the table")
	}
}

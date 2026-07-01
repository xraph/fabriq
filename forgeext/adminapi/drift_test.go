package adminapi

import (
	"reflect"
	"sort"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
)

func TestComputeDrift(t *testing.T) {
	expected := []string{"id", "tenant_id", "version", "name", "price"}
	physical := []postgres.ColumnInfo{
		{Name: "id"}, {Name: "tenant_id"}, {Name: "version"}, {Name: "name"},
		{Name: "sku"}, // extra: physical but not expected
		// "price" is missing: expected but not physical
	}
	missing, extra := computeDrift(expected, physical)
	sort.Strings(missing)
	sort.Strings(extra)
	if !reflect.DeepEqual(missing, []string{"price"}) {
		t.Errorf("missing = %v, want [price]", missing)
	}
	if !reflect.DeepEqual(extra, []string{"sku"}) {
		t.Errorf("extra = %v, want [sku]", extra)
	}

	// Fully in-sync → no missing, no extra.
	m2, e2 := computeDrift([]string{"id", "name"}, []postgres.ColumnInfo{{Name: "id"}, {Name: "name"}})
	if len(m2) != 0 || len(e2) != 0 {
		t.Errorf("in-sync drift = missing %v extra %v, want empty", m2, e2)
	}
}

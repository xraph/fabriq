package postgres

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func TestAssertMutableColumnRejectsStructural(t *testing.T) {
	for _, c := range []string{registry.ColumnID, registry.ColumnTenant, registry.ColumnVersion} {
		if err := assertMutableColumn(c); err == nil {
			t.Fatalf("expected structural column %q to be rejected", c)
		}
	}
}

func TestAssertMutableColumnRejectsInvalidIdent(t *testing.T) {
	if err := assertMutableColumn("drop table users;--"); err == nil {
		t.Fatal("expected invalid identifier to be rejected")
	}
}

func TestAssertMutableColumnAllowsDomainColumn(t *testing.T) {
	if err := assertMutableColumn("colour"); err != nil {
		t.Fatalf("domain column rejected: %v", err)
	}
}

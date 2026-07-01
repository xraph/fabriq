package adminapi

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func TestKindToColumnType(t *testing.T) {
	cases := map[string]registry.ColumnType{
		"string": registry.ColText, "number": registry.ColFloat,
		"boolean": registry.ColBool, "time": registry.ColTime, "object": registry.ColJSON,
	}
	for kind, want := range cases {
		got, err := kindToColumnType(kind)
		if err != nil || got != want {
			t.Fatalf("kind %q: got (%v,%v) want %v", kind, got, err, want)
		}
	}
	if _, err := kindToColumnType("bogus"); err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestValidateDefaultExpr(t *testing.T) {
	ok := []string{"", "42", "-3.14", "true", "FALSE", "null", "now()", "NOW()", "'pending'", "''"}
	for _, s := range ok {
		if err := validateDefaultExpr(s); err != nil {
			t.Fatalf("expected %q allowed, got %v", s, err)
		}
	}
	bad := []string{"nextval('x')", "'a''b'", "1;DROP TABLE t", "now() + 1", "gen_random_uuid()", "'x'||'y'"}
	for _, s := range bad {
		if err := validateDefaultExpr(s); err == nil {
			t.Fatalf("expected %q rejected", s)
		}
	}
}

func TestValidSchemaIdent(t *testing.T) {
	if !validSchemaIdent("order") || !validSchemaIdent("order_line2") {
		t.Fatal("valid idents rejected")
	}
	for _, s := range []string{"", "2bad", "a-b", "drop table", "a;b"} {
		if validSchemaIdent(s) {
			t.Fatalf("invalid ident %q accepted", s)
		}
	}
}

func TestTableFor(t *testing.T) {
	if tableFor("order") != "ds_order" {
		t.Fatalf("tableFor(order) = %q", tableFor("order"))
	}
}

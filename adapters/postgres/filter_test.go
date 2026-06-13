package postgres

import (
	"testing"

	"github.com/xraph/fabriq/core/query"
)

func TestCondSQL(t *testing.T) {
	cases := []struct {
		name     string
		cond     query.Cond
		wantSQL  string
		wantArgs int
	}{
		{"eq", query.Eq("name", "Pump"), `"name" = ?`, 1},
		{"ne", query.Ne("kind", "pump"), `"kind" != ?`, 1},
		{"gt", query.Gt("version", 2), `"version" > ?`, 1},
		{"gte", query.Gte("version", 2), `"version" >= ?`, 1},
		{"lt", query.Lt("version", 9), `"version" < ?`, 1},
		{"lte", query.Lte("version", 9), `"version" <= ?`, 1},
		{"like", query.Like("name", "%pump%"), `"name" LIKE ?`, 1},
		{"ilike", query.ILike("name", "%PUMP%"), `"name" ILIKE ?`, 1},
		{"in", query.In("kind", []string{"pump", "valve"}), `"kind" = ANY(?)`, 1},
		{"notin", query.NotIn("kind", []string{"pump"}), `NOT ("kind" = ANY(?))`, 1},
		{"isnull", query.IsNull("parent_id"), `"parent_id" IS NULL`, 0},
		{"isnotnull", query.IsNotNull("site_id"), `"site_id" IS NOT NULL`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, args, err := condSQL(tc.cond)
			if err != nil {
				t.Fatal(err)
			}
			if sql != tc.wantSQL {
				t.Fatalf("sql = %q, want %q", sql, tc.wantSQL)
			}
			if len(args) != tc.wantArgs {
				t.Fatalf("args = %d, want %d", len(args), tc.wantArgs)
			}
		})
	}
}

func TestCondSQL_OrGroup(t *testing.T) {
	sql, args, err := condSQL(query.Or(
		query.Eq("kind", "pump"),
		query.Eq("kind", "valve"),
		query.IsNull("kind"),
	))
	if err != nil {
		t.Fatal(err)
	}
	want := `("kind" = ? OR "kind" = ? OR "kind" IS NULL)`
	if sql != want {
		t.Fatalf("sql = %q, want %q", sql, want)
	}
	if len(args) != 2 { // IsNull contributes no arg
		t.Fatalf("args = %d, want 2", len(args))
	}
}

func TestCondSQL_NestedOr(t *testing.T) {
	// site_id = ? AND (kind = ? OR (version > ? AND ... )) style nesting.
	sql, _, err := condSQL(query.Or(
		query.Eq("kind", "pump"),
		query.Or(query.Gt("version", 5), query.Lt("version", 2)),
	))
	if err != nil {
		t.Fatal(err)
	}
	want := `("kind" = ? OR ("version" > ? OR "version" < ?))`
	if sql != want {
		t.Fatalf("sql = %q, want %q", sql, want)
	}
}

// The column is the only interpolated token; a malicious column never
// reaches SQL because List validates against the binding first, but the
// quoter is the second line of defence.
func TestCondSQL_QuotesColumn(t *testing.T) {
	sql, _, err := condSQL(query.Eq(`evil" OR 1=1 --`, "x"))
	if err != nil {
		t.Fatal(err)
	}
	if sql != `"evil OR 1=1 --" = ?` {
		t.Fatalf("quoter did not strip the quote: %q", sql)
	}
}

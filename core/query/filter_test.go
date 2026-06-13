package query_test

import (
	"testing"

	"github.com/xraph/fabriq/core/query"
)

func TestCond_Constructors(t *testing.T) {
	if c := query.Eq("name", "x"); c.Column != "name" || c.Op != query.OpEq || c.Value != "x" {
		t.Fatalf("Eq = %+v", c)
	}
	if c := query.Ne("kind", "pump"); c.Op != query.OpNe {
		t.Fatalf("Ne = %+v", c)
	}
	if c := query.Gt("version", 3); c.Op != query.OpGt || c.Value != 3 {
		t.Fatalf("Gt = %+v", c)
	}
	if c := query.In("kind", []string{"pump", "valve"}); c.Op != query.OpIn {
		t.Fatalf("In = %+v", c)
	}
	if c := query.Like("name", "%pump%"); c.Op != query.OpLike || c.Value != "%pump%" {
		t.Fatalf("Like = %+v", c)
	}
	if c := query.ILike("name", "%PUMP%"); c.Op != query.OpILike {
		t.Fatalf("ILike = %+v", c)
	}
	if c := query.IsNull("parent_id"); c.Op != query.OpIsNull || c.Value != nil {
		t.Fatalf("IsNull = %+v", c)
	}
	if c := query.IsNotNull("site_id"); c.Op != query.OpIsNotNull {
		t.Fatalf("IsNotNull = %+v", c)
	}
	or := query.Or(query.Eq("kind", "pump"), query.Eq("kind", "valve"))
	if len(or.Or) != 2 {
		t.Fatalf("Or group = %+v", or)
	}
}

func TestValidateConds(t *testing.T) {
	has := func(c string) bool { return c == "name" || c == "kind" || c == "version" }

	good := []query.Cond{
		query.Eq("name", "Pump"),
		query.Gt("version", 2),
		query.In("kind", []string{"pump", "valve"}),
		query.Or(query.Eq("kind", "pump"), query.IsNull("name")),
	}
	if err := query.ValidateConds(good, has); err != nil {
		t.Fatalf("valid conds rejected: %v", err)
	}

	cases := []struct {
		name string
		cond query.Cond
	}{
		{"unknown column", query.Eq("nope", "x")},
		{"unknown column in OR group", query.Or(query.Eq("kind", "p"), query.Eq("nope", "x"))},
		{"IN without a slice", query.In("kind", "pump")},
		{"IN with empty slice", query.In("kind", []string{})},
		{"eq without value", query.Cond{Column: "name", Op: query.OpEq}},
		{"unknown operator", query.Cond{Column: "name", Op: query.Op("DROP TABLE")}},
		{"empty column", query.Eq("", "x")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := query.ValidateConds([]query.Cond{tc.cond}, has); err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			}
		})
	}
}

func TestValidateConds_IsNullIgnoresValue(t *testing.T) {
	has := func(string) bool { return true }
	if err := query.ValidateConds([]query.Cond{query.IsNull("x"), query.IsNotNull("y")}, has); err != nil {
		t.Fatalf("null checks must not require a value: %v", err)
	}
}

func TestEqs(t *testing.T) {
	conds := query.Eqs(map[string]any{"site_id": "S1", "kind": "pump"})
	if len(conds) != 2 {
		t.Fatalf("Eqs len = %d", len(conds))
	}
	// Deterministic order (sorted by column) so generated SQL is stable.
	if conds[0].Column != "kind" || conds[1].Column != "site_id" {
		t.Fatalf("Eqs not sorted: %v", conds)
	}
	for _, c := range conds {
		if c.Op != query.OpEq {
			t.Fatalf("Eqs produced non-eq op: %+v", c)
		}
	}
	if query.Eqs(nil) != nil && len(query.Eqs(nil)) != 0 {
		t.Fatal("Eqs(nil) should be empty")
	}
}

package fabriqtest_test

import (
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/fabriqtest"
)

// seedAssets creates assets with the given (name, site) pairs and returns
// the world. Versions are all 1; callers that need version variety bump
// via update.
func seedAssets(t *testing.T) (*fabriqtest.World, func(name, site string) string) {
	t.Helper()
	w := fabriqtest.NewWorld(ftRegistry(t))
	x, err := command.NewExecutor(w.Registry, w.Store)
	if err != nil {
		t.Fatal(err)
	}
	ctx := ftCtx(t, "acme")
	create := func(name, site string) string {
		res, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
			Payload: &ftAsset{Name: name, SiteID: site}})
		if err != nil {
			t.Fatal(err)
		}
		return res.AggID
	}
	return w, create
}

func listAssets(t *testing.T, w *fabriqtest.World, q query.ListQuery) []string {
	t.Helper()
	var got []*ftAsset
	if err := w.Rel.List(ftCtx(t, "acme"), "asset", q, &got); err != nil {
		t.Fatalf("List: %v", err)
	}
	names := make([]string, len(got))
	for i, a := range got {
		names[i] = a.Name
	}
	return names
}

func TestFakeRelational_RichFilter_OperatorsAndOr(t *testing.T) {
	w, create := seedAssets(t)
	create("Main Pump", "S1")
	create("Backup Pump", "S1")
	create("Inlet Valve", "S2")
	create("Spare Motor", "S2")

	// LIKE.
	if names := listAssets(t, w, query.ListQuery{
		Where: []query.Cond{query.Like("name", "%Pump")}, OrderBy: "name",
	}); len(names) != 2 || names[0] != "Backup Pump" || names[1] != "Main Pump" {
		t.Fatalf("LIKE %%Pump = %v", names)
	}

	// ILIKE (case-insensitive).
	if names := listAssets(t, w, query.ListQuery{
		Where: []query.Cond{query.ILike("name", "%pump")},
	}); len(names) != 2 {
		t.Fatalf("ILIKE = %v", names)
	}

	// IN over a non-key column (site_id).
	if names := listAssets(t, w, query.ListQuery{
		Where: []query.Cond{query.In("site_id", []string{"S2"})}, OrderBy: "name",
	}); len(names) != 2 || names[0] != "Inlet Valve" {
		t.Fatalf("IN site_id S2 = %v", names)
	}

	// OR group: name LIKE %Valve OR name LIKE %Motor.
	if names := listAssets(t, w, query.ListQuery{
		Where:   []query.Cond{query.Or(query.Like("name", "%Valve"), query.Like("name", "%Motor"))},
		OrderBy: "name",
	}); len(names) != 2 || names[0] != "Inlet Valve" || names[1] != "Spare Motor" {
		t.Fatalf("OR group = %v", names)
	}

	// Filter (equality shorthand) AND Where, combined.
	if names := listAssets(t, w, query.ListQuery{
		Filter: map[string]any{"site_id": "S1"},
		Where:  []query.Cond{query.Like("name", "Main%")},
	}); len(names) != 1 || names[0] != "Main Pump" {
		t.Fatalf("Filter AND Where = %v", names)
	}

	// NotIn.
	if names := listAssets(t, w, query.ListQuery{
		Where: []query.Cond{query.NotIn("site_id", []string{"S1"})}, OrderBy: "name",
	}); len(names) != 2 || names[0] != "Inlet Valve" {
		t.Fatalf("NOT IN S1 = %v", names)
	}
}

func TestFakeRelational_RichFilter_Comparisons(t *testing.T) {
	w, create := seedAssets(t)
	x, _ := command.NewExecutor(w.Registry, w.Store)
	ctx := ftCtx(t, "acme")

	a := create("A", "S1")
	create("B", "S1")
	// Bump A to version 3.
	for i := 0; i < 2; i++ {
		if _, err := x.Exec(ctx, command.Command{Entity: "asset", Op: command.OpUpdate, AggID: a,
			Payload: &ftAsset{Name: "A", SiteID: "S1"}}); err != nil {
			t.Fatal(err)
		}
	}

	if names := listAssets(t, w, query.ListQuery{Where: []query.Cond{query.Gt("version", 1)}}); len(names) != 1 || names[0] != "A" {
		t.Fatalf("version > 1 = %v", names)
	}
	if names := listAssets(t, w, query.ListQuery{Where: []query.Cond{query.Lte("version", 1)}}); len(names) != 1 || names[0] != "B" {
		t.Fatalf("version <= 1 = %v", names)
	}
	if names := listAssets(t, w, query.ListQuery{Where: []query.Cond{query.Gte("version", 1)}}); len(names) != 2 {
		t.Fatalf("version >= 1 = %v", names)
	}
}

func TestFakeRelational_RichFilter_RejectsUnknownColumn(t *testing.T) {
	w, create := seedAssets(t)
	create("X", "S1")
	var got []*ftAsset
	err := w.Rel.List(ftCtx(t, "acme"), "asset", query.ListQuery{
		Where: []query.Cond{query.Eq("nonexistent", "x")},
	}, &got)
	if err == nil {
		t.Fatal("unknown filter column must be rejected (injection guard)")
	}
}

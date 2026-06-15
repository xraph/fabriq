package livequery_test

import (
	"testing"

	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
)

func has(cols ...string) func(string) bool {
	set := map[string]bool{}
	for _, c := range cols {
		set[c] = true
	}
	return func(c string) bool { return set[c] }
}

func TestLiveQueryValidate(t *testing.T) {
	h := has("id", "site_id", "status", "name")
	sortable := has("name", "id")

	ok := livequery.LiveQuery{
		Entity: "asset",
		Where:  query.Where{query.Eq("site_id", "S1"), query.Eq("status", "active")},
		Sort:   []livequery.SortKey{{Column: "name"}},
		Limit:  50,
	}
	if err := ok.Validate(h, sortable); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}

	badCol := livequery.LiveQuery{Entity: "asset", Where: query.Where{query.Eq("nope", 1)}, Limit: 10}
	if err := badCol.Validate(h, sortable); err == nil {
		t.Fatal("expected unknown-column rejection")
	}

	badSort := livequery.LiveQuery{Entity: "asset", Sort: []livequery.SortKey{{Column: "status"}}, Limit: 10}
	if err := badSort.Validate(h, sortable); err == nil {
		t.Fatal("expected non-sortable column rejection")
	}

	noLimit := livequery.LiveQuery{Entity: "asset"}
	if err := noLimit.Validate(h, sortable); err == nil {
		t.Fatal("expected limit>0 requirement")
	}
}

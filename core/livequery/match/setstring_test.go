package match_test

import (
	"testing"

	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/query"
)

func TestSetStringOrOps(t *testing.T) {
	row := map[string]any{"kind": "pump", "name": "Alpha-7", "tags": "north"}
	cases := []struct {
		name string
		w    query.Where
		want bool
	}{
		{"in", query.Where{query.In("kind", []string{"valve", "pump"})}, true},
		{"in no", query.Where{query.In("kind", []string{"valve"})}, false},
		{"notin", query.Where{query.NotIn("kind", []string{"valve"})}, true},
		{"like prefix", query.Where{query.Like("name", "Alpha%")}, true},
		{"like mid", query.Where{query.Like("name", "%pha-%")}, true},
		{"like underscore", query.Where{query.Like("name", "Alpha-_")}, true},
		{"like no", query.Where{query.Like("name", "Beta%")}, false},
		{"ilike", query.Where{query.ILike("name", "alpha-7")}, true},
		{"or", query.Where{query.Or(query.Eq("kind", "valve"), query.Eq("tags", "north"))}, true},
		{"or none", query.Where{query.Or(query.Eq("kind", "valve"), query.Eq("tags", "south"))}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, _ := match.Compile(c.w)
			if got := p.Eval(row); got != c.want {
				t.Fatalf("Eval=%v want %v", got, c.want)
			}
		})
	}
}

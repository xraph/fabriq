package match_test

import (
	"testing"

	"github.com/xraph/fabriq/core/livequery/match"
	"github.com/xraph/fabriq/core/query"
)

func TestScalarOps(t *testing.T) {
	row := map[string]any{"temp": 80.0, "status": "active", "count": int64(3)}
	cases := []struct {
		name string
		w    query.Where
		want bool
	}{
		{"eq str", query.Where{query.Eq("status", "active")}, true},
		{"eq str no", query.Where{query.Eq("status", "idle")}, false},
		{"ne", query.Where{query.Ne("status", "idle")}, true},
		{"gt num", query.Where{query.Gt("temp", 79.0)}, true},
		{"gt num no", query.Where{query.Gt("temp", 80.0)}, false},
		{"gte", query.Where{query.Gte("temp", 80.0)}, true},
		{"lt cross-int-float", query.Where{query.Lt("count", 4.0)}, true},
		{"and", query.Where{query.Eq("status", "active"), query.Gt("temp", 50.0)}, true},
		{"and one fails", query.Where{query.Eq("status", "active"), query.Gt("temp", 200.0)}, false},
		{"isnull absent", query.Where{query.IsNull("missing")}, true},
		{"isnotnull present", query.Where{query.IsNotNull("status")}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := match.Compile(c.w)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			if got := p.Eval(row); got != c.want {
				t.Fatalf("Eval=%v want %v", got, c.want)
			}
		})
	}
}

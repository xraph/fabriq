package livequery_test

import (
	"testing"

	"github.com/xraph/fabriq/core/livequery"
)

func TestOrderCursor(t *testing.T) {
	sort := []livequery.SortKey{{Column: "name"}, {Column: "temp", Desc: true}}
	rowA := map[string]any{"id": "a", "name": "Alpha", "temp": 80.0}
	rowB := map[string]any{"id": "b", "name": "Alpha", "temp": 90.0}
	rowC := map[string]any{"id": "c", "name": "Beta", "temp": 10.0}

	ca := livequery.SortKeyOf(rowA, sort, "a")
	cb := livequery.SortKeyOf(rowB, sort, "b")
	cc := livequery.SortKeyOf(rowC, sort, "c")

	// name asc: Alpha < Beta; within Alpha, temp DESC: 90 before 80 → b < a.
	if livequery.CompareCursors(cb, ca, sort) >= 0 {
		t.Fatal("b should sort before a (temp desc)")
	}
	if livequery.CompareCursors(ca, cc, sort) >= 0 {
		t.Fatal("a (Alpha) should sort before c (Beta)")
	}
	// total order: equal sort values fall back to id asc.
	rowD := map[string]any{"id": "d", "name": "Alpha", "temp": 80.0}
	cd := livequery.SortKeyOf(rowD, sort, "d")
	if livequery.CompareCursors(ca, cd, sort) >= 0 {
		t.Fatal("tiebreak by id: a < d")
	}
}

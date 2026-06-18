package livequery_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/livequery"
)

func TestWindowEvictionEmitsLeave(t *testing.T) {
	// N=2, full visible [Alpha, Charlie], with a deeper Echo for refill.
	seed := []livequery.Row{row("a", "Alpha", 1), row("c", "Charlie", 1)}
	all := []livequery.Row{row("a", "Alpha", 1), row("b", "Bravo", 1), row("c", "Charlie", 1), row("e", "Echo", 1)}
	w := newWindow(t, 2, seed, all)

	// Inserting Bravo at index 1 evicts Charlie (was visible index 1) → it must LEAVE.
	d := w.Apply(context.Background(), change("b", "Bravo", 1, "active"))
	var sawEnterB, sawLeaveC bool
	for _, x := range d {
		if x.Op == livequery.OpEnter && x.AggID == "b" && x.NewIndex == 1 {
			sawEnterB = true
		}
		if x.Op == livequery.OpLeave && x.AggID == "c" {
			sawLeaveC = true
		}
	}
	if !sawEnterB || !sawLeaveC {
		t.Fatalf("insert into full window must emit enter(b)+leave(c); got %+v", d)
	}
	vis := w.Visible()
	if len(vis) != 2 || vis[0].AggID != "a" || vis[1].AggID != "b" {
		t.Fatalf("visible window wrong: %+v", vis)
	}
}

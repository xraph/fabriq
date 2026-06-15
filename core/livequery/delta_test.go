package livequery_test

import (
	"testing"

	"github.com/xraph/fabriq/core/livequery"
)

func TestDeltaOpString(t *testing.T) {
	cases := map[livequery.DeltaOp]string{
		livequery.OpEnter:   "enter",
		livequery.OpLeave:   "leave",
		livequery.OpMove:    "move",
		livequery.OpUpdate:  "update",
		livequery.OpReset:   "reset",
		livequery.OpMatch:   "match",
		livequery.OpUnmatch: "unmatch",
	}
	for op, want := range cases {
		if got := op.String(); got != want {
			t.Fatalf("op %d = %q want %q", op, got, want)
		}
	}
}

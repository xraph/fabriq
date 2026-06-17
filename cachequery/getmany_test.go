package cachequery_test

import (
	"testing"

	"github.com/xraph/fabriq/cachequery"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestCachedGetMany_WarmRowsAcrossCalls(t *testing.T) {
	fr := &fakeRel{rows: map[string]row{
		"a1": {ID: "a1", Name: "Pump"},
		"a2": {ID: "a2", Name: "Valve"},
		"a3": {ID: "a3", Name: "Motor"},
	}}
	cr := cachequery.New(fr, fabriqtest.NewFakeCache(), reg(t, true))
	ctx := tctx(t)

	var first []row
	if err := cr.GetMany(ctx, "asset", []string{"a1", "a2"}, &first); err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || fr.getMany != 1 {
		t.Fatalf("first GetMany: rows=%d getMany=%d", len(first), fr.getMany)
	}
	// a1,a2 are now warm; a3 is cold. Second call loads ONLY a3.
	var second []row
	if err := cr.GetMany(ctx, "asset", []string{"a1", "a2", "a3"}, &second); err != nil {
		t.Fatal(err)
	}
	if len(second) != 3 {
		t.Fatalf("second GetMany rows=%d (want 3)", len(second))
	}
	// Exactly one more underlying call, and it carried only the missing id.
	if fr.getMany != 2 {
		t.Fatalf("underlying GetMany calls=%d (want 2 total)", fr.getMany)
	}
	// Order follows requested ids.
	if second[0].ID != "a1" || second[1].ID != "a2" || second[2].ID != "a3" {
		t.Fatalf("order not preserved: %+v", second)
	}
}

func TestCachedGetMany_AllWarmSkipsUnderlying(t *testing.T) {
	fr := &fakeRel{rows: map[string]row{"a1": {ID: "a1", Name: "Pump"}}}
	cr := cachequery.New(fr, fabriqtest.NewFakeCache(), reg(t, true))
	ctx := tctx(t)
	var a []row
	_ = cr.GetMany(ctx, "asset", []string{"a1"}, &a)
	var b []row
	if err := cr.GetMany(ctx, "asset", []string{"a1"}, &b); err != nil {
		t.Fatal(err)
	}
	if fr.getMany != 1 {
		t.Fatalf("all-warm GetMany must not call underlying again: getMany=%d", fr.getMany)
	}
}

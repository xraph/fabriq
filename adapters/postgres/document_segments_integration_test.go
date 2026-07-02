//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestListSegmentsAfterCompact(t *testing.T) {
	ctx := context.Background()
	_, app := newDocScopeHarness(t)
	ds := app.Documents()
	ds.EnableArchive(fabriqtest.NewFakeBlob(), true)
	tctx, _ := tenant.WithTenant(ctx, "t1")
	docID := "page/" + event.NewID()

	if err := ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", "a", 100, "n1")); err != nil {
		t.Fatal(err)
	}
	if err := ds.ApplyUpdate(tctx, docID, crdtLWWUpdate(t, "pages", docID, "title", "b", 200, "n1")); err != nil {
		t.Fatal(err)
	}
	if err := ds.Compact(tctx, docID); err != nil {
		t.Fatalf("compact: %v", err)
	}

	segs, err := ds.ListSegments(tctx, docID)
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(segs) != 1 {
		t.Fatalf("want 1 segment, got %d", len(segs))
	}
	s := segs[0]
	if s.SeqLo != 1 || s.SeqHi != 2 || s.UpdateCount != 2 {
		t.Fatalf("segment = %+v, want SeqLo=1 SeqHi=2 UpdateCount=2", s)
	}
	if s.ByteSize <= 0 {
		t.Fatalf("ByteSize = %d, want > 0", s.ByteSize)
	}
}

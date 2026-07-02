package fabriqtest_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestFakeDocSegmentsAndHistory(t *testing.T) {
	fd := &fabriqtest.FakeDocumentStore{}
	fd.SeedSegments("page/x", []document.SegmentInfo{{SegSeq: 1, SeqLo: 1, SeqHi: 2, UpdateCount: 2, ByteSize: 10}})
	fd.SeedHistory("page/x", []document.HistoryUpdate{{Seq: 1, Update: json.RawMessage(`[]`)}})
	ctx, _ := tenant.WithTenant(context.Background(), "t1")

	segs, err := fd.ListSegments(ctx, "page/x")
	if err != nil || len(segs) != 1 || segs[0].SeqLo != 1 {
		t.Fatalf("ListSegments = %+v err=%v", segs, err)
	}
	hist, err := fd.ReadHistory(ctx, "page/x", 1, 2)
	if err != nil || len(hist) != 1 || hist[0].Seq != 1 {
		t.Fatalf("ReadHistory = %+v err=%v", hist, err)
	}
	if err := fd.DeleteHistory(ctx, "page/x"); err != nil {
		t.Fatal(err)
	}
	if !fd.DeletedHistory("page/x") {
		t.Fatal("DeleteHistory should have been recorded")
	}
	// DeleteHistory should also clear seeded state.
	segsAfter, err := fd.ListSegments(ctx, "page/x")
	if err != nil || len(segsAfter) != 0 {
		t.Fatalf("ListSegments after delete = %+v err=%v", segsAfter, err)
	}
}

func TestWorldFakeDocAccessor(t *testing.T) {
	world := fabriqtest.NewWorld(registry.New())
	if world.Docs == nil {
		t.Fatal("World.Docs is nil")
	}
	world.Docs.SeedSegments("page/y", []document.SegmentInfo{{SegSeq: 1, SeqLo: 5, SeqHi: 6}})
	fab := fabriqtest.NewFabric(world)
	ctx, _ := tenant.WithTenant(context.Background(), "t1")
	lister, ok := fab.Document().(document.SegmentLister)
	if !ok {
		t.Fatal("fab.Document() does not implement document.SegmentLister")
	}
	segs, err := lister.ListSegments(ctx, "page/y")
	if err != nil || len(segs) != 1 {
		t.Fatalf("ListSegments = %+v err=%v", segs, err)
	}
}

//go:build integration

package fabriq_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

// TestE2E_LiveQueryExactWindow drives a maintained result set end-to-end:
// snapshot from Postgres, then live enter/leave deltas via the real
// command -> outbox -> relay -> redis -> live-feed path, with exact top-N
// maintenance (an in-window ENTER evicts the boundary row).
func TestE2E_LiveQueryExactWindow(t *testing.T) {
	f, _, _ := e2e(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	mk := func(name, kind string) string {
		t.Helper()
		res, eerr := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
			Payload: &domain.Asset{Name: name, Kind: kind, SiteID: "S1"}})
		if eerr != nil {
			t.Fatalf("create %s: %v", name, eerr)
		}
		return res.AggID
	}

	// Seed BEFORE subscribing so the snapshot contains them.
	aID := mk("Alpha", "pump")
	cID := mk("Charlie", "pump")
	bID := mk("Bravo", "valve") // not a pump yet → excluded from the window

	q := livequery.LiveQuery{
		Entity: "asset",
		Where:  query.Where{query.Eq("kind", "pump")},
		Sort:   []livequery.SortKey{{Column: "name"}},
		Limit:  2,
	}
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	snap, deltas, lcancel, err := f.LiveQuery(subCtx, q)
	if err != nil {
		t.Fatalf("LiveQuery: %v", err)
	}
	defer lcancel()

	if len(snap.Rows) != 2 || snap.Rows[0].AggID != aID || snap.Rows[1].AggID != cID {
		t.Fatalf("snapshot = %+v want [Alpha(%s), Charlie(%s)]", snap.Rows, aID, cID)
	}
	if snap.SubID == "" {
		t.Fatal("snapshot must carry a SubID")
	}

	time.Sleep(400 * time.Millisecond) // let the live feed attach before churning

	// Promote Bravo valve -> pump. It now matches and sorts to index 1
	// (Alpha < Bravo < Charlie), entering the window and evicting Charlie.
	if _, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpUpdate, AggID: bID,
		Payload: &domain.Asset{Name: "Bravo", Kind: "pump", SiteID: "S1"}}); err != nil {
		t.Fatalf("promote bravo: %v", err)
	}

	var sawEnterB, sawLeaveC bool
	deadline := time.After(10 * time.Second)
	for !(sawEnterB && sawLeaveC) {
		select {
		case d := <-deltas:
			switch {
			case d.Op == livequery.OpEnter && d.AggID == bID && d.NewIndex == 1:
				sawEnterB = true
			case d.Op == livequery.OpLeave && d.AggID == cID:
				sawLeaveC = true
			}
		case <-deadline:
			t.Fatalf("did not observe enter(Bravo)+leave(Charlie); enterB=%v leaveC=%v", sawEnterB, sawLeaveC)
		}
	}
}

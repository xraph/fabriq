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

// TestE2E_LiveQueryFieldDeltas drives the field delta through the REAL path:
// a command, the outbox, the relay, redis, the live feed, and the maintained
// window — against a real Postgres and a real entity.
//
// The unit tests prove the window computes a delta from two rows it holds.
// They cannot prove that a row arriving over the wire still has a before-image
// to be compared against by the time it gets there: the feed rebuilds `Vals`
// by unmarshalling an event payload, and the window's copy came from a
// Postgres snapshot. Two paths, two shapes, and only this test crosses both.
func TestE2E_LiveQueryFieldDeltas(t *testing.T) {
	f, _, _ := e2e(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	res, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Alpha", Kind: "pump", SiteID: "S1"}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	aID := res.AggID

	q := livequery.LiveQuery{
		Entity:      "asset",
		Where:       query.Where{query.Eq("kind", "pump")},
		Sort:        []livequery.SortKey{{Column: "name"}},
		Limit:       10,
		FieldDeltas: true,
	}
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	snap, deltas, lh, err := f.LiveQuery(subCtx, q)
	if err != nil {
		t.Fatalf("LiveQuery: %v", err)
	}
	defer lh.Close()
	if len(snap.Rows) != 1 {
		t.Fatalf("snapshot rows = %d, want 1", len(snap.Rows))
	}

	time.Sleep(400 * time.Millisecond) // let the live feed attach

	// Move ONE field. Name and kind are unchanged, so the row stays in the
	// window at the same position: an in-place update, which is the only op
	// that can carry a field delta.
	if _, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpUpdate, AggID: aID,
		Payload: &domain.Asset{Name: "Alpha", Kind: "pump", SiteID: "S2"}}); err != nil {
		t.Fatalf("move site: %v", err)
	}

	deadline := time.After(15 * time.Second)
	for {
		select {
		case d := <-deltas:
			if d.Op != livequery.OpUpdate || d.AggID != aID {
				continue // enter/leave churn from the seed; not what this asserts
			}
			if d.Changes == nil {
				t.Fatal("an update over the real path carried no field delta, " +
					"though the subscription asked for one")
			}
			if got, ok := d.Changes["site_id"]; !ok || got != "S2" {
				t.Fatalf("Changes = %#v, want site_id=S2 — the field that actually moved", d.Changes)
			}
			// ONLY what moved. The whole row still ships in `Row`, so a delta
			// naming every column would be indistinguishable from no delta at
			// all for a client deciding which cell to repaint.
			if _, named := d.Changes["name"]; named {
				t.Errorf("Changes named `name`, which did not move: %#v", d.Changes)
			}
			if len(d.Row) == 0 {
				t.Error("the whole row must still ship beside the delta")
			}
			return
		case <-deadline:
			t.Fatal("no update delta for the changed row within 15s")
		}
	}
}

/*
GUARDS the opt-in over the real path. Without FieldDeltas the same update must
still arrive — the row changed — carrying no Changes, so a subscriber that did
not ask pays nothing and sees exactly what it saw before this existed.
*/
func TestE2E_LiveQueryWithoutFieldDeltas(t *testing.T) {
	f, _, _ := e2e(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	res, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Alpha", Kind: "pump", SiteID: "S1"}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	aID := res.AggID

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, deltas, lh, err := f.LiveQuery(subCtx, livequery.LiveQuery{
		Entity: "asset",
		Where:  query.Where{query.Eq("kind", "pump")},
		Sort:   []livequery.SortKey{{Column: "name"}},
		Limit:  10,
		// FieldDeltas deliberately absent.
	})
	if err != nil {
		t.Fatalf("LiveQuery: %v", err)
	}
	defer lh.Close()

	time.Sleep(400 * time.Millisecond)

	if _, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpUpdate, AggID: aID,
		Payload: &domain.Asset{Name: "Alpha", Kind: "pump", SiteID: "S2"}}); err != nil {
		t.Fatalf("move site: %v", err)
	}

	deadline := time.After(15 * time.Second)
	for {
		select {
		case d := <-deltas:
			if d.Op != livequery.OpUpdate || d.AggID != aID {
				continue
			}
			if d.Changes != nil {
				t.Fatalf("a subscription that did not ask for field deltas got %#v", d.Changes)
			}
			if len(d.Row) == 0 {
				t.Error("the update must still carry the row")
			}
			return
		case <-deadline:
			t.Fatal("no update delta within 15s")
		}
	}
}

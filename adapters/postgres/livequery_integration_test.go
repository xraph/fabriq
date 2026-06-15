//go:build integration

package postgres_test

import (
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

func TestPGLiveSnapshotKeyset(t *testing.T) {
	h := newHarness(t)
	ctx := tctx(t, "acme")

	mk := func(name, kind, site string) string {
		t.Helper()
		res, err := h.X.Exec(ctx, command.Command{
			Entity:  "asset",
			Op:      command.OpCreate,
			Payload: &domain.Asset{Name: name, Kind: kind, SiteID: site},
		})
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		return res.AggID
	}

	aID := mk("Alpha", "pump", "S1")
	bID := mk("Bravo", "pump", "S1")
	_ = mk("Charlie", "valve", "S1") // filtered out (kind=valve)
	_ = mk("Delta", "pump", "S2")    // filtered out (site=S2)

	store := h.A.NewLiveStore()
	q := livequery.LiveQuery{
		Entity: "asset",
		Where:  query.Where{query.Eq("kind", "pump"), query.Eq("site_id", "S1")},
		Sort:   []livequery.SortKey{{Column: "name"}},
		Limit:  10,
	}

	rows, err := store.Snapshot(ctx, q, 10)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("snapshot len = %d want 2: %+v", len(rows), rows)
	}
	if rows[0].AggID != aID || rows[1].AggID != bID {
		t.Fatalf("snapshot order wrong: got %s,%s want %s,%s", rows[0].AggID, rows[1].AggID, aID, bID)
	}
	if name, _ := rows[0].Vals["name"].(string); name != "Alpha" {
		t.Fatalf("row0 name = %q want Alpha", name)
	}

	more, err := store.After(ctx, q, rows[0].Cursor, 10)
	if err != nil {
		t.Fatalf("after: %v", err)
	}
	if len(more) != 1 || more[0].AggID != bID {
		t.Fatalf("keyset after wrong: %+v", more)
	}
}

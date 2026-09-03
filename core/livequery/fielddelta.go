package livequery

import "reflect"

// diffVals returns the fields that differ between two column maps: the
// per-field delta a client needs to know WHICH cell moved, rather than only
// that the row did.
//
// Why the engine has to compute it at all: an event envelope carries the
// column-keyed JSON of the aggregate AFTER the change (see core/event), and
// nothing upstream carries a before-image. The only place a before-image
// exists is wherever the previous row is still held — the maintained window,
// or a streamed view that has been asked to remember one.
//
// The rules match twinos' own live differ (extensions/query/internal/services,
// DiffResultsByKey → diffRow) on purpose. Two live-query engines feeding one
// client that disagreed about what "changed" means would make a cell flash or
// not depending on which backend answered, which is worse than either rule.
//
//   - a field whose value moved, or that the new row added, carries its NEW value
//   - a field the new row DROPPED carries nil, so a client can tell "gone" from
//     "unchanged" rather than silently keeping a stale cell
//   - a row that moved nothing yields nil, never an empty non-nil map: an empty
//     delta says "something changed, but nothing you can see", which reads to a
//     client as traffic it must repaint
//
// A nil `prev` yields nil, NOT "every field changed". No baseline is not the
// same as a total change, and reporting one would flash a whole row on the
// first update after a re-anchor or a failover. The consumer falls back to the
// full row in `Row`, which is always sent.
//
// Equality is reflect.DeepEqual rather than a JSON round-trip: these maps come
// from json.Unmarshal, so the values are float64 / string / bool / nil / []any
// / map[string]any, and DeepEqual is exact over all of them without the
// allocation a marshal-per-field costs on a hot dispatch path.
func diffVals(prev, next map[string]any) map[string]any {
	if prev == nil || next == nil {
		return nil
	}

	var changes map[string]any
	for k, nv := range next {
		pv, had := prev[k]
		if had && reflect.DeepEqual(pv, nv) {
			continue
		}
		if changes == nil {
			changes = make(map[string]any)
		}
		changes[k] = nv
	}
	for k := range prev {
		if _, still := next[k]; still {
			continue
		}
		if changes == nil {
			changes = make(map[string]any)
		}
		changes[k] = nil
	}
	return changes
}

// withChanges attaches a field delta to a delta that describes a row the
// client was already holding.
//
// Only enter/leave/match/unmatch are excluded, and for the same reason in each
// case: the client either had no previous state for that row or is losing it,
// so there is nothing for a field delta to be relative to.
func withChanges(d LiveDelta, changes map[string]any) LiveDelta {
	d.Changes = changes
	return d
}

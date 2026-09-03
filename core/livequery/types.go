// Package livequery is fabriq's maintained-result-set live query engine:
// a client supplies a filter + sort + limit/cursor and receives a snapshot
// followed by a live stream of enter/leave/move/update deltas. Engine-neutral
// by construction — no pgx/grove/redis types appear here.
package livequery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/query"
)

// Mode selects the per-subscription delivery policy over one matcher.
type Mode int

const (
	// ModeMaintained keeps an ordered window + cushion and emits
	// enter/leave/move/update; exact top-N via Postgres boundary refill.
	ModeMaintained Mode = iota
	// ModeStreamed forwards matched change events; the client orders. (P2.)
	ModeStreamed
)

// SortKey is one ordering term. Fabriq always appends id ASC as the final
// unique tiebreak so (Sort…, id) is a TOTAL order — required for keyset.
type SortKey struct {
	Column string
	Desc   bool
}

// Cursor is a keyset anchor: one value per SortKey plus the trailing id.
type Cursor struct {
	Values []any `json:"values"`
}

// LiveQuery is a registered, server-maintained window over an entity.
type LiveQuery struct {
	Entity string
	Where  query.Where
	Sort   []SortKey
	Limit  int // window size N
	Cursor *Cursor
	Mode   Mode
	// FieldDeltas asks the engine to report WHICH fields moved on an update,
	// not only that the row did. Off by default, and opt-in because the two
	// modes pay for it very differently.
	//
	// ModeMaintained gets it for nothing: the window already holds the
	// previous row in order to splice it, so the before-image is in hand at
	// the moment the update is emitted.
	//
	// ModeStreamed holds only an ID-set — membership, no payloads — so a
	// before-image means retaining the last row per matching aggregate for as
	// long as it matches. That is real memory on a view that was deliberately
	// built to hold none, and a subscriber that does not render cells should
	// not pay it.
	FieldDeltas bool
}

// Validate checks the query against an entity: filter columns must exist
// (the injection guard, via query.ValidateConds), sort columns must be
// declared sortable, and Limit must be positive.
func (q LiveQuery) Validate(hasColumn, isSortable func(string) bool) error {
	if q.Entity == "" {
		return fmt.Errorf("fabriq: live query has empty entity")
	}
	if q.Limit <= 0 {
		return fmt.Errorf("fabriq: live query requires Limit > 0")
	}
	if err := query.ValidateConds(q.Where, hasColumn); err != nil {
		return err
	}
	for _, s := range q.Sort {
		if s.Column == "" {
			return fmt.Errorf("fabriq: live query sort key with empty column")
		}
		if !isSortable(s.Column) {
			return fmt.Errorf("fabriq: column %q is not sortable for live queries", s.Column)
		}
	}
	return nil
}

// DeltaOp is the maintained-result-set delta vocabulary.
type DeltaOp int

const (
	OpEnter   DeltaOp = iota // row entered the visible window
	OpLeave                  // row left the visible window
	OpMove                   // row stayed visible, position changed
	OpUpdate                 // row stayed at same position, payload changed
	OpReset                  // discard window and re-snapshot (reanchor/failover/overflow)
	OpMatch                  // ModeStreamed: row now matches (P2)
	OpUnmatch                // ModeStreamed: row no longer matches (P2)
)

// String returns the SSE event name for a delta op.
func (op DeltaOp) String() string {
	switch op {
	case OpEnter:
		return "enter"
	case OpLeave:
		return "leave"
	case OpMove:
		return "move"
	case OpUpdate:
		return "update"
	case OpReset:
		return "reset"
	case OpMatch:
		return "match"
	case OpUnmatch:
		return "unmatch"
	}
	return "delta"
}

// LiveDelta is one change to a maintained window.
type LiveDelta struct {
	Op       DeltaOp         `json:"op"`
	AggID    string          `json:"agg_id,omitempty"`
	Version  int64           `json:"version,omitempty"`
	Row      json.RawMessage `json:"row,omitempty"`
	OldIndex int             `json:"old_index"`
	NewIndex int             `json:"new_index"`
	Cursor   Cursor          `json:"cursor,omitempty"`
	StreamID string          `json:"stream_id,omitempty"`
	At       time.Time       `json:"at"`
	// Changes names the fields that moved, when the subscription asked for
	// field deltas (LiveQuery.FieldDeltas) and a before-image was available.
	//
	// ADDITIVE, never a replacement: Row still carries the whole row, so a
	// client that ignores this keeps working unchanged, and one that reads it
	// can repaint a single cell instead of a row. Absent on enter/leave/match/
	// unmatch, which have no previous state to be relative to, and absent when
	// nothing moved.
	Changes map[string]any `json:"changes,omitempty"`
}

// Row is a result row carried by the snapshot/refill ports.
type Row struct {
	AggID   string
	Version int64
	Cursor  Cursor
	Raw     json.RawMessage
	Vals    map[string]any
}

// Snapshot is the initial result of a live subscription.
type Snapshot struct {
	SubID     string
	Rows      []Row
	Watermark string // event-stream id at snapshot time; live applied strictly after
}

// Change is one event handed to the matcher (column-keyed, from event.Envelope).
type Change struct {
	AggID    string
	Version  int64
	Deleted  bool
	Vals     map[string]any // nil when Deleted
	Raw      json.RawMessage
	StreamID string
	At       time.Time
}

// AuthzFunc authorizes (and may later constrain) a live query before snapshot.
type AuthzFunc func(ctx context.Context, q LiveQuery) error

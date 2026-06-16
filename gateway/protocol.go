// Package gateway is the transport shell that turns the in-process sharded
// live-query Gateway into a deployable edge tier: it terminates client SSE and
// WebSocket connections and forwards the maintained/streamed delta stream over
// them. It carries NO live-query logic — it speaks to a Backend seam (satisfied
// by core/livequery/cluster.Gateway) and writes to two tiny transport
// interfaces (SSESink, WSConn) that forge's Stream/Connection — and test fakes —
// satisfy. This keeps the package free of any web framework or Redis dependency
// and fully unit-testable. The forge-native binding lives in forgeext.
package gateway

import (
	"encoding/json"

	"github.com/xraph/fabriq/core/livequery"
)

// Frame is one delta as the client sees it: livequery.LiveDelta minus the
// server-only timestamp, with the op rendered as a stable string ("enter",
// "leave", "move", "update", "reset") so SSE *and* WebSocket clients get the
// same friendly wire shape (the raw LiveDelta serialises op as an integer).
type Frame struct {
	Op       string           `json:"op"`
	AggID    string           `json:"agg_id,omitempty"`
	Version  int64            `json:"version,omitempty"`
	Row      json.RawMessage  `json:"row,omitempty"`
	OldIndex int              `json:"old_index"`
	NewIndex int              `json:"new_index"`
	Cursor   livequery.Cursor `json:"cursor,omitempty"`
	StreamID string           `json:"stream_id,omitempty"`
}

// frameOf projects a delta onto the client wire frame.
func frameOf(d livequery.LiveDelta) Frame {
	return Frame{
		Op:       d.Op.String(),
		AggID:    d.AggID,
		Version:  d.Version,
		Row:      d.Row,
		OldIndex: d.OldIndex,
		NewIndex: d.NewIndex,
		Cursor:   d.Cursor,
		StreamID: d.StreamID,
	}
}

// Client→server command actions (WebSocket only; SSE is delivery-only).
const (
	ActionSubscribe   = "subscribe"
	ActionReanchor    = "reanchor"
	ActionUnsubscribe = "unsubscribe"
)

// ClientCommand is a control frame a WebSocket client sends upstream. The first
// command on a connection must be a subscribe carrying the query; reanchor and
// unsubscribe then operate on the established subscription.
type ClientCommand struct {
	Action string               `json:"action"`
	Query  *livequery.LiveQuery `json:"query,omitempty"`  // subscribe
	Cursor *livequery.Cursor    `json:"cursor,omitempty"` // reanchor
	Limit  int                  `json:"limit,omitempty"`  // reanchor
}

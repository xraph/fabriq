package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/subscribe"
)

// defaultLiveLimit is the live-query window size (and the snapshot row cap)
// when the request omits "limit".
const defaultLiveLimit = 50

// maxLiveLimit caps the live-query window the caller may request.
const maxLiveLimit = 200

// liveHeartbeat is the SSE keep-alive interval for the live stream.
const liveHeartbeat = 15 * time.Second

// liveRequest is the POST {BasePath}/live body: which entity to watch, an
// optional equality filter, and the window size.
type liveRequest struct {
	// Entity is the entity type name to watch (required).
	Entity string `json:"entity"`
	// Filter is an optional column => value equality filter (AND-ed). Columns
	// must be declared Filterable on the entity's LiveSpec.
	Filter map[string]any `json:"filter,omitempty"`
	// Limit bounds the maintained window and the snapshot rows (default 50, max 200).
	Limit int `json:"limit,omitempty"`
}

// liveSnapshotEvent is the first SSE event of a live stream: the initial
// matching rows, capped at the request limit.
type liveSnapshotEvent struct {
	Type string    `json:"type"` // always "snapshot"
	Rows []liveRow `json:"rows"`
}

// liveRow is one row in the snapshot event.
type liveRow struct {
	ID  string          `json:"id"`
	Row json.RawMessage `json:"row,omitempty"`
}

// liveDeltaEvent is a per-change SSE event mapped from a livequery.LiveDelta.
type liveDeltaEvent struct {
	Type     string          `json:"type"` // always "delta"
	Op       string          `json:"op"`   // enter|leave|move|update|reset
	ID       string          `json:"id,omitempty"`
	Row      json.RawMessage `json:"row,omitempty"`
	OldIndex int             `json:"oldIndex"`
	NewIndex int             `json:"newIndex"`
}

// registerLiveRoutes wires the live-query SSE endpoint onto r.
func (c *adminController) registerLiveRoutes(r forge.Router) error {
	opts := append([]forge.RouteOption{
		forge.WithMethod(http.MethodPost),
		forge.WithName("fabriq.admin.live"),
		forge.WithSummary("Live query (SSE: snapshot + maintained-window deltas)"),
		forge.WithTags("Fabriq", "Admin", "SSE"),
	}, c.ext.cfg.RouteOptions...)
	return r.SSE(c.ext.cfg.BasePath+"/live", c.handleLive, opts...)
}

// handleLive serves POST {BasePath}/live as an SSE stream.
//
// Body: {"entity": "<type>", "filter": {col: val, ...}?, "limit": N?}
//
// It subscribes the maintained-result-set live engine for the request tenant,
// emits a "snapshot" event with the initial rows, then one "delta" event per
// enter/leave/move/update, with periodic heartbeats. It degrades to 501 with
// {"error":"live queries not configured"} when the live engine is unwired (no
// Redis tailer / relational oracle) or the entity declares no LiveSpec.
func (c *adminController) handleLive(ctx forge.Context) error {
	fab, err := c.ext.resolveFabriq()
	if err != nil {
		// No concrete facade (e.g. fake-backed tests) — live queries are not
		// available. Report the same not-configured contract as a nil engine.
		return liveNotConfigured(ctx)
	}

	var req liveRequest
	if derr := json.NewDecoder(ctx.Request().Body).Decode(&req); derr != nil {
		return forge.BadRequest("invalid live request: " + derr.Error())
	}
	if strings.TrimSpace(req.Entity) == "" {
		return forge.BadRequest("field 'entity' is required")
	}

	limit := defaultLiveLimit
	if req.Limit > 0 {
		limit = req.Limit
	}
	if limit > maxLiveLimit {
		limit = maxLiveLimit
	}

	lq := livequery.LiveQuery{
		Entity: req.Entity,
		Limit:  limit,
		Mode:   livequery.ModeMaintained,
	}
	if len(req.Filter) > 0 {
		lq.Where = query.Eqs(req.Filter)
	}

	reqCtx := ctx.Request().Context()
	snap, deltas, handle, serr := fab.LiveQuery(reqCtx, lq)
	if serr != nil {
		// The live engine is unwired (no Redis tailer / relational oracle) or the
		// entity is not opted into live queries — degrade to a 501 with the
		// not-configured contract. Genuine request errors (unknown column,
		// non-sortable sort, bad entity) are client errors → 400.
		if isLiveNotConfiguredErr(serr) {
			return liveNotConfigured(ctx)
		}
		return forge.BadRequest(serr.Error())
	}
	defer handle.Close()

	sse, werr := subscribe.NewSSEWriter(ctx.Response())
	if werr != nil {
		return forge.InternalError(werr)
	}

	// First event: the initial snapshot rows, capped at the window limit.
	if eerr := writeLiveSnapshot(sse, snap, limit); eerr != nil {
		return eerr
	}

	return streamLiveDeltas(reqCtx, sse, deltas, liveHeartbeat)
}

// writeLiveSnapshot emits the "snapshot" SSE event with up to limit rows.
func writeLiveSnapshot(sse *subscribe.SSEWriter, snap livequery.Snapshot, limit int) error {
	rows := snap.Rows
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]liveRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, liveRow{ID: r.AggID, Row: r.Raw})
	}
	return sse.WriteEvent(snap.Watermark, "snapshot", liveSnapshotEvent{Type: "snapshot", Rows: out})
}

// streamLiveDeltas forwards live deltas as SSE "delta" events until ctx is done
// or the channel closes, with periodic heartbeats. OpReset is forwarded so the
// client knows to discard its window and re-snapshot (reanchor/failover/overflow).
func streamLiveDeltas(ctx context.Context, sse *subscribe.SSEWriter, deltas <-chan livequery.LiveDelta, heartbeat time.Duration) error {
	if heartbeat <= 0 {
		heartbeat = liveHeartbeat
	}
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deltas:
			if !ok {
				return nil
			}
			ev := liveDeltaEvent{
				Type:     "delta",
				Op:       d.Op.String(),
				ID:       d.AggID,
				Row:      d.Row,
				OldIndex: d.OldIndex,
				NewIndex: d.NewIndex,
			}
			if eerr := sse.WriteEvent(d.AggID, "delta", ev); eerr != nil {
				return eerr
			}
		case <-ticker.C:
			if eerr := sse.Heartbeat(); eerr != nil {
				return eerr
			}
		}
	}
}

// isLiveNotConfiguredErr reports whether a fab.LiveQuery error means the live
// plane is unavailable rather than the request being malformed: the engine is
// unwired (ErrStoreNotConfigured — no Redis tailer / relational oracle) or the
// entity declares no LiveSpec. Both degrade to 501; everything else is a 400.
func isLiveNotConfiguredErr(err error) bool {
	if errors.Is(err, fabriq.ErrStoreNotConfigured) {
		return true
	}
	return err != nil && strings.Contains(err.Error(), "does not declare a LiveSpec")
}

// liveNotConfigured writes the 501 not-configured response (used when the
// concrete facade is unavailable, e.g. in fake-backed tests).
func liveNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "live queries not configured"})
}

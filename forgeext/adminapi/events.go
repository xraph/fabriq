package adminapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/xraph/forge"
)

// defaultEventLimit / maxEventLimit bound the outbox page size.
const (
	defaultEventLimit = 50
	maxEventLimit     = 200
)

// eventItem is one row of the transactional outbox (the durable event log).
// fabriq's command plane appends one envelope per aggregate write; the relay
// stamps published_at when it forwards the event to the change feed but never
// deletes the row, so the outbox is a durable, append-only event history.
type eventItem struct {
	ID                   string          `json:"id"`
	Aggregate            string          `json:"aggregate"`
	AggID                string          `json:"aggId"`
	Version              int64           `json:"version"`
	Type                 string          `json:"type"`
	At                   string          `json:"at"`
	PayloadSchemaVersion int             `json:"payloadSchemaVersion"`
	Published            bool            `json:"published"`
	StreamID             string          `json:"streamId,omitempty"`
	Payload              json.RawMessage `json:"payload"`
}

// eventScanRow mirrors the SELECT below (grove scans by column tag). Payload
// and At are read as text and re-projected onto eventItem.
type eventScanRow struct {
	ID                   string `grove:"id"`
	Aggregate            string `grove:"aggregate"`
	AggID                string `grove:"agg_id"`
	Version              int64  `grove:"version"`
	Type                 string `grove:"type"`
	At                   string `grove:"at"`
	PayloadSchemaVersion int    `grove:"payload_schema_version"`
	Published            bool   `grove:"published"`
	StreamID             string `grove:"stream_id"`
	Payload              string `grove:"payload"`
}

// eventListResponse is the payload for GET {BasePath}/events.
type eventListResponse struct {
	Items      []eventItem `json:"items"`
	NextCursor string      `json:"nextCursor"`
}

// eventBacklogResponse is the payload for GET {BasePath}/events/backlog.
type eventBacklogResponse struct {
	Unpublished int64 `json:"unpublished"`
}

// registerEventRoutes wires the outbox/event-log read routes.
func (c *adminController) registerEventRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	listOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.events.list"),
		forge.WithSummary("List outbox events (filters: aggregate, type, aggId, published; paged)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/events", c.handleListEvents, listOpts...); err != nil {
		return err
	}

	backlogOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.events.backlog"),
		forge.WithSummary("Report the unpublished outbox depth"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.GET(base+"/events/backlog", c.handleEventBacklog, backlogOpts...)
}

// handleListEvents serves GET {BasePath}/events.
//
// Recent-first (ULID id descending), keyset-paginated via ?cursor=<lastId>.
// Optional filters: ?aggregate= ?type= ?aggId= ?published=(true|false). The
// outbox has NO row-level security, so the scan is scoped to the request tenant
// explicitly via the app.tenant_id GUC the relational tenant-tx sets.
func (c *adminController) handleListEvents(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	limit := defaultEventLimit
	if lStr := ctx.Query("limit"); lStr != "" {
		l, perr := strconv.Atoi(lStr)
		if perr != nil || l < 1 {
			return forge.BadRequest("query param 'limit' must be a positive integer")
		}
		if l > maxEventLimit {
			l = maxEventLimit
		}
		limit = l
	}

	// Build the WHERE clause. Values are bound parameters ($N) — never
	// interpolated — so filter inputs cannot inject. The tenant predicate uses
	// the GUC (current_setting returns '' when unset → matches no rows).
	var (
		conds = []string{"tenant_id = current_setting('app.tenant_id', true)"}
		args  []any
	)
	bind := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if v := strings.TrimSpace(ctx.Query("aggregate")); v != "" {
		conds = append(conds, "aggregate = "+bind(v))
	}
	if v := strings.TrimSpace(ctx.Query("type")); v != "" {
		conds = append(conds, "type = "+bind(v))
	}
	if v := strings.TrimSpace(ctx.Query("aggId")); v != "" {
		conds = append(conds, "agg_id = "+bind(v))
	}
	switch strings.TrimSpace(ctx.Query("published")) {
	case "true":
		conds = append(conds, "published_at IS NOT NULL")
	case "false":
		conds = append(conds, "published_at IS NULL")
	case "":
		// no filter
	default:
		return forge.BadRequest("query param 'published' must be 'true' or 'false'")
	}
	// Keyset cursor: fetch rows strictly older (lexically smaller ULID) than the
	// last id returned. id is the primary key, so this is a stable, index-friendly
	// pagination that tolerates concurrent inserts.
	if cur := strings.TrimSpace(ctx.Query("cursor")); cur != "" {
		conds = append(conds, "id < "+bind(cur))
	}

	sql := fmt.Sprintf(`SELECT id, aggregate, agg_id, version, type, at::text AS at,
			payload_schema_version, (published_at IS NOT NULL) AS published,
			stream_id, payload::text AS payload
		FROM fabriq_outbox
		WHERE %s
		ORDER BY id DESC
		LIMIT %d`, strings.Join(conds, " AND "), limit+1)

	reqCtx := ctx.Request().Context()
	var rows []eventScanRow
	if qErr := fab.Relational().Query(reqCtx, &rows, sql, args...); qErr != nil {
		return mapQueryError(qErr)
	}

	nextCursor := ""
	if len(rows) > limit {
		rows = rows[:limit]
		nextCursor = rows[len(rows)-1].ID
	}

	items := make([]eventItem, 0, len(rows))
	for _, r := range rows {
		payload := json.RawMessage(r.Payload)
		if len(payload) == 0 {
			payload = json.RawMessage("{}")
		}
		items = append(items, eventItem{
			ID:                   r.ID,
			Aggregate:            r.Aggregate,
			AggID:                r.AggID,
			Version:              r.Version,
			Type:                 r.Type,
			At:                   r.At,
			PayloadSchemaVersion: r.PayloadSchemaVersion,
			Published:            r.Published,
			StreamID:             r.StreamID,
			Payload:              payload,
		})
	}

	return ctx.JSON(http.StatusOK, eventListResponse{Items: items, NextCursor: nextCursor})
}

// handleEventBacklog serves GET {BasePath}/events/backlog — the unpublished
// outbox depth for the request tenant (the relay's Backlog, tenant-scoped).
func (c *adminController) handleEventBacklog(ctx forge.Context) error {
	n, err := c.unpublishedOutboxCount(ctx.Request().Context())
	if err != nil {
		return mapQueryError(err)
	}
	return ctx.JSON(http.StatusOK, eventBacklogResponse{Unpublished: n})
}

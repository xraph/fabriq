package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
)

// Event is a single row of the transactional outbox — the durable event log
// behind the command plane. It mirrors adminapi's eventItem JSON exactly:
// {id, aggregate, aggId, version, type, at, payloadSchemaVersion, published,
// streamId, payload}.
type Event struct {
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

// EventPage is a page of outbox events, recent-first. It mirrors adminapi's
// eventListResponse JSON exactly: {items, nextCursor}.
type EventPage struct {
	Items      []Event `json:"items"`
	NextCursor string  `json:"nextCursor"`
}

// EventsBacklog is the payload for EventsBacklog. It mirrors adminapi's
// eventBacklogResponse JSON exactly: {unpublished}.
type EventsBacklog struct {
	Unpublished int64 `json:"unpublished"`
}

// EventFacets is the payload for EventFacets. It mirrors adminapi's
// eventFacetsResponse JSON exactly: {aggregates, types}.
type EventFacets struct {
	Aggregates []string `json:"aggregates"`
	Types      []string `json:"types"`
}

// ListEventsParams are the query parameters for ListEvents. Aggregate and
// Type are multi-valued (OR / SQL IN): each value is sent as a repeated
// query parameter (?aggregate=a&aggregate=b), mirroring the TS client and
// the server's IN-filter. Published is a tri-state: nil omits the filter,
// non-nil sends "true" or "false".
type ListEventsParams struct {
	// Aggregate matches any of these aggregate types (OR / SQL IN).
	Aggregate []string
	// Type matches any of these event types (OR / SQL IN).
	Type []string
	// AggID filters to a single aggregate instance id.
	AggID string
	// Published filters to published (true) or unpublished (false) events.
	// Nil omits the filter.
	Published *bool
	// Limit caps the page size (server default 50, max 200). Zero omits the
	// query param and defers to the server default.
	Limit int
	// Cursor resumes a previous listing (server emits this as
	// EventPage.NextCursor). Empty starts from the first page.
	Cursor string
}

// ListEvents pages the transactional outbox (durable event log),
// recent-first. It calls GET {BasePath}/events with optional repeated
// ?aggregate= and ?type= filters, ?aggId, ?published, ?limit, and ?cursor.
func (c *Client) ListEvents(ctx context.Context, params ListEventsParams) (EventPage, error) {
	q := url.Values{}
	for _, a := range params.Aggregate {
		if a != "" {
			q.Add("aggregate", a)
		}
	}
	for _, t := range params.Type {
		if t != "" {
			q.Add("type", t)
		}
	}
	if params.AggID != "" {
		q.Set("aggId", params.AggID)
	}
	if params.Published != nil {
		q.Set("published", strconv.FormatBool(*params.Published))
	}
	if params.Limit > 0 {
		q.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.Cursor != "" {
		q.Set("cursor", params.Cursor)
	}

	var out EventPage
	if err := c.do(ctx, http.MethodGet, "/events", q, nil, &out); err != nil {
		return EventPage{}, err
	}
	return out, nil
}

// GetEventsBacklog reports the unpublished outbox depth for the active
// tenant (the relay backlog). It calls GET {BasePath}/events/backlog.
func (c *Client) GetEventsBacklog(ctx context.Context) (EventsBacklog, error) {
	var out EventsBacklog
	if err := c.do(ctx, http.MethodGet, "/events/backlog", nil, nil, &out); err != nil {
		return EventsBacklog{}, err
	}
	return out, nil
}

// GetEventFacets lists the distinct aggregate types and event types present
// in the active tenant's outbox, for populating the events filter
// comboboxes. It calls GET {BasePath}/events/facets.
func (c *Client) GetEventFacets(ctx context.Context) (EventFacets, error) {
	var out EventFacets
	if err := c.do(ctx, http.MethodGet, "/events/facets", nil, nil, &out); err != nil {
		return EventFacets{}, err
	}
	return out, nil
}

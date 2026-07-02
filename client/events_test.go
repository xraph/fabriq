package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_ListEvents(t *testing.T) {
	var gotMethod, gotPath string
	var gotQuery map[string][]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = map[string][]string(r.URL.Query())

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EventPage{
			Items: []Event{
				{
					ID:                   "01H",
					Aggregate:            "order",
					AggID:                "42",
					Version:              3,
					Type:                 "order.created",
					At:                   "2026-07-01T00:00:00Z",
					PayloadSchemaVersion: 1,
					Published:            true,
					StreamID:             "stream-1",
					Payload:              json.RawMessage(`{"status":"open"}`),
				},
			},
			NextCursor: "01G",
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	published := true
	page, err := c.ListEvents(context.Background(), ListEventsParams{
		Aggregate: []string{"order", "invoice"},
		Type:      []string{"order.created", "order.updated"},
		AggID:     "42",
		Published: &published,
		Limit:     25,
		Cursor:    "01Z",
	})
	if err != nil {
		t.Fatalf("ListEvents() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/events" {
		t.Errorf("path = %q, want /admin/events", gotPath)
	}

	wantAggregate := []string{"order", "invoice"}
	if got := gotQuery["aggregate"]; !stringSlicesEqual(got, wantAggregate) {
		t.Errorf("query[aggregate] = %v, want %v", got, wantAggregate)
	}
	wantType := []string{"order.created", "order.updated"}
	if got := gotQuery["type"]; !stringSlicesEqual(got, wantType) {
		t.Errorf("query[type] = %v, want %v", got, wantType)
	}
	if got := gotQuery["aggId"]; len(got) != 1 || got[0] != "42" {
		t.Errorf("query[aggId] = %v, want [42]", got)
	}
	if got := gotQuery["published"]; len(got) != 1 || got[0] != "true" {
		t.Errorf("query[published] = %v, want [true]", got)
	}
	if got := gotQuery["limit"]; len(got) != 1 || got[0] != "25" {
		t.Errorf("query[limit] = %v, want [25]", got)
	}
	if got := gotQuery["cursor"]; len(got) != 1 || got[0] != "01Z" {
		t.Errorf("query[cursor] = %v, want [01Z]", got)
	}

	if len(page.Items) != 1 {
		t.Fatalf("len(page.Items) = %d, want 1", len(page.Items))
	}
	if page.Items[0].ID != "01H" || page.Items[0].Aggregate != "order" {
		t.Errorf("page.Items[0] = %+v, want id=01H aggregate=order", page.Items[0])
	}
	if page.NextCursor != "01G" {
		t.Errorf("page.NextCursor = %q, want %q", page.NextCursor, "01G")
	}
}

func TestClient_ListEvents_NoFilters(t *testing.T) {
	var gotQuery map[string][]string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string][]string(r.URL.Query())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EventPage{})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.ListEvents(context.Background(), ListEventsParams{}); err != nil {
		t.Fatalf("ListEvents() unexpected error: %v", err)
	}

	if len(gotQuery) != 0 {
		t.Errorf("query = %v, want empty", gotQuery)
	}
}

func TestClient_GetEventsBacklog(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EventsBacklog{Unpublished: 7})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	backlog, err := c.GetEventsBacklog(context.Background())
	if err != nil {
		t.Fatalf("GetEventsBacklog() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/events/backlog" {
		t.Errorf("path = %q, want /admin/events/backlog", gotPath)
	}
	if backlog.Unpublished != 7 {
		t.Errorf("backlog.Unpublished = %d, want 7", backlog.Unpublished)
	}
}

func TestClient_GetEventFacets(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(EventFacets{
			Aggregates: []string{"order", "invoice"},
			Types:      []string{"order.created", "invoice.paid"},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	facets, err := c.GetEventFacets(context.Background())
	if err != nil {
		t.Fatalf("GetEventFacets() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/events/facets" {
		t.Errorf("path = %q, want /admin/events/facets", gotPath)
	}
	if !stringSlicesEqual(facets.Aggregates, []string{"order", "invoice"}) {
		t.Errorf("facets.Aggregates = %v, want [order invoice]", facets.Aggregates)
	}
	if !stringSlicesEqual(facets.Types, []string{"order.created", "invoice.paid"}) {
		t.Errorf("facets.Types = %v, want [order.created invoice.paid]", facets.Types)
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

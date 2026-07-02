package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_GetCrdtDocument(t *testing.T) {
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(CrdtDocument{
			DocID:    "page/welcome",
			Version:  7,
			Snapshot: json.RawMessage(`{"title":"Welcome","body":"hello"}`),
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	doc, err := c.GetCrdtDocument(context.Background(), "page/welcome")
	if err != nil {
		t.Fatalf("GetCrdtDocument() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/crdt/page/welcome" {
		t.Errorf("path = %q, want /admin/crdt/page/welcome", gotPath)
	}
	if doc.DocID != "page/welcome" {
		t.Errorf("doc.DocID = %q, want %q", doc.DocID, "page/welcome")
	}
	if doc.Version != 7 {
		t.Errorf("doc.Version = %d, want 7", doc.Version)
	}

	var snap map[string]any
	if err := json.Unmarshal(doc.Snapshot, &snap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snap["title"] != "Welcome" {
		t.Errorf("snapshot[title] = %v, want Welcome", snap["title"])
	}
}

func TestClient_GetCrdtDocument_EncodesEachSegment(t *testing.T) {
	var gotRequestURI, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequestURI = r.RequestURI
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(CrdtDocument{DocID: "page/a b"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.GetCrdtDocument(context.Background(), "page/a b"); err != nil {
		t.Fatalf("GetCrdtDocument() unexpected error: %v", err)
	}

	// The space within the "id" segment must be percent-escaped on the wire
	// (RequestURI, which preserves the raw encoding)...
	if gotRequestURI != "/admin/crdt/page/a%20b" {
		t.Errorf("RequestURI = %q, want /admin/crdt/page/a%%20b", gotRequestURI)
	}
	// ...while the "/" separator between entity and id must be preserved as
	// a real path separator (not escaped to %2F), which is visible once the
	// server decodes the URL.
	if gotPath != "/admin/crdt/page/a b" {
		t.Errorf("decoded path = %q, want /admin/crdt/page/a b", gotPath)
	}
}

func TestClient_GetCrdtUpdates(t *testing.T) {
	var gotMethod, gotPath, gotLimit string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotLimit = r.URL.Query().Get("limit")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(CrdtUpdatePage{
			DocID:        "page/welcome",
			HighWaterSeq: 42,
			HasSnapshot:  true,
			Items: []CrdtUpdate{
				{Index: 0, Size: 128, Preview: "aGVsbG8="},
			},
		})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	page, err := c.GetCrdtUpdates(context.Background(), GetCrdtUpdatesParams{
		DocID: "page/welcome",
		Limit: 25,
	})
	if err != nil {
		t.Fatalf("GetCrdtUpdates() unexpected error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/admin/crdt/page/welcome/updates" {
		t.Errorf("path = %q, want /admin/crdt/page/welcome/updates", gotPath)
	}
	if gotLimit != "25" {
		t.Errorf("query param limit = %q, want %q", gotLimit, "25")
	}

	if page.HighWaterSeq != 42 {
		t.Errorf("page.HighWaterSeq = %d, want 42", page.HighWaterSeq)
	}
	if !page.HasSnapshot {
		t.Error("page.HasSnapshot = false, want true")
	}
	if len(page.Items) != 1 || page.Items[0].Preview != "aGVsbG8=" {
		t.Errorf("page.Items = %+v, want one item with preview aGVsbG8=", page.Items)
	}
}

func TestClient_GetCrdtUpdates_OmitsOptionalQueryParams(t *testing.T) {
	var gotRawQuery string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(CrdtUpdatePage{})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	if _, err := c.GetCrdtUpdates(context.Background(), GetCrdtUpdatesParams{DocID: "page/welcome"}); err != nil {
		t.Fatalf("GetCrdtUpdates() unexpected error: %v", err)
	}

	if gotRawQuery != "" {
		t.Errorf("raw query = %q, want empty", gotRawQuery)
	}
}

func TestClient_GetCrdtDocument_NotConfiguredReturnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "document/CRDT plane not configured"})
	}))
	defer srv.Close()

	c := testClient(t, srv, "")

	_, err := c.GetCrdtDocument(context.Background(), "page/welcome")
	if err == nil {
		t.Fatal("GetCrdtDocument() expected error, got nil")
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %T, want *APIError", err)
	}
	if apiErr.Status != http.StatusNotImplemented {
		t.Errorf("apiErr.Status = %d, want %d", apiErr.Status, http.StatusNotImplemented)
	}
	if apiErr.Message != "document/CRDT plane not configured" {
		t.Errorf("apiErr.Message = %q, want %q", apiErr.Message, "document/CRDT plane not configured")
	}
}

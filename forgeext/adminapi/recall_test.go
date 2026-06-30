package adminapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// postRecall issues a POST to the recall route with a JSON body and the test
// tenant header stamped.
func postRecall(t *testing.T, srv *httptest.Server, body any) *http.Response {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/recall", bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(testTenantHeader, testTenantID)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// TestRecall_MissingQuery verifies that a body with an embedder configured but
// no "query" returns 400. The not-configured gate runs first, so the embedder
// must be present to reach the query validation.
func TestRecall_MissingQuery(t *testing.T) {
	world, _ := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world, WithEmbedder(stubEmbedder{dims: 3}))
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postRecall(t, srv, map[string]any{"entities": []string{"widget"}, "k": 5})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 400, body = %s", resp.StatusCode, body)
	}
}

// TestRecall_NotConfigured verifies that recall with NO embedder wired returns
// 501 with the documented {"error":"recall not configured"} body — the vector
// (semantic) channel cannot run without an embedder, so hybrid recall is gated
// off entirely.
func TestRecall_NotConfigured(t *testing.T) {
	world, _ := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world) // no WithEmbedder
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postRecall(t, srv, map[string]any{"query": "widget", "entities": []string{"widget"}})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501, body = %s", resp.StatusCode, body)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"] != "recall not configured" {
		t.Errorf("error = %q, want %q", body["error"], "recall not configured")
	}
}

// TestRecall_FusedItems exercises the full hybrid-recall pipeline against the
// fake world: the searchable+embedded widget participates in BOTH the vector and
// search channels, so a fused item must carry both "vector" and "search" in its
// source list and be hydrated from the relational source of truth.
func TestRecall_FusedItems(t *testing.T) {
	world, ids := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world, WithEmbedder(stubEmbedder{dims: 3}))
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postRecall(t, srv, map[string]any{
		"query":    "sprocket",
		"entities": []string{"widget"},
		"k":        5,
		"budget":   2000,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}

	var got recallResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Items) == 0 {
		t.Fatal("expected at least one fused recall item")
	}

	// Sprocket is the search hit for "sprocket" AND aligns with the stub query
	// vector, so it must appear and carry both channels in its source.
	var sprocket *recallItem
	for i := range got.Items {
		if got.Items[i].ID == ids["Sprocket"] {
			sprocket = &got.Items[i]
			break
		}
	}
	if sprocket == nil {
		t.Fatalf("Sprocket (%s) not in recall items: %+v", ids["Sprocket"], got.Items)
	}
	if sprocket.Entity != "widget" {
		t.Errorf("entity = %q, want widget", sprocket.Entity)
	}
	if sprocket.Score <= 0 {
		t.Errorf("score = %v, want > 0", sprocket.Score)
	}
	if !contains(sprocket.Source, "vector") || !contains(sprocket.Source, "search") {
		t.Errorf("source = %v, want both vector and search channels", sprocket.Source)
	}
	// Row must be a real JSON object carrying the hydrated row, not a quoted string.
	var row map[string]any
	if err := json.Unmarshal(sprocket.Row, &row); err != nil {
		t.Fatalf("row is not a JSON object: %v (raw=%s)", err, sprocket.Row)
	}
	if row["name"] != "Sprocket" {
		t.Errorf("row name = %v, want Sprocket (hydration)", row["name"])
	}
}

// TestRecall_DefaultEntities verifies that omitting "entities" defaults to every
// registered dynamic entity type, so a bare {query} still recalls the widget.
func TestRecall_DefaultEntities(t *testing.T) {
	world, ids := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world, WithEmbedder(stubEmbedder{dims: 3}))
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postRecall(t, srv, map[string]any{"query": "sprocket", "k": 5})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, body)
	}
	var got recallResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	found := false
	for _, it := range got.Items {
		if it.ID == ids["Sprocket"] {
			found = true
		}
	}
	if !found {
		t.Errorf("Sprocket not recalled with defaulted entities: %+v", got.Items)
	}
}

// TestMeta_RecallCapability verifies the recall.read capability slug is advertised.
func TestMeta_RecallCapability(t *testing.T) {
	world, _ := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/meta")
	defer resp.Body.Close()

	var got metaResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !contains(got.Capabilities, "recall.read") {
		t.Errorf("recall.read capability not advertised: %v", got.Capabilities)
	}
}

// contains reports whether s contains v.
func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

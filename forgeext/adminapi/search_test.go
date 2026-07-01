package adminapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

// searchableWidgetSpec returns a dynamic widget spec that participates in both
// the search projection (Search.Index/Fields) and vector embedding (Embed).
// Used to exercise the real fake-backed search and vector paths.
func searchableWidgetSpec() registry.EntitySpec {
	s := widgetSpec()
	s.Search = registry.SearchSpec{Index: "ds_widgets", Fields: []string{"name", "colour"}}
	s.Embed = &registry.EmbedSpec{Fields: []string{"name"}}
	return s
}

// stubEmbedder is a deterministic Embedder: it returns one fixed-length vector
// per input string, all equal to fixedVec, so a text query resolves to a stable
// embedding the FakeVector can score against.
type stubEmbedder struct{ dims int }

func (e stubEmbedder) Dims() int { return e.dims }

func (e stubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		v := make([]float32, e.dims)
		v[0] = 1 // unit vector along the first axis
		out[i] = v
	}
	return out, nil
}

// buildSearchWorld registers the searchable+embedded widget, seeds two
// relational rows, indexes them into the search fake, and upserts an embedding
// per row into the vector fake — all under the test tenant.
func buildSearchWorld(t *testing.T) (*fabriqtest.World, map[string]string) {
	t.Helper()

	reg := registry.New()
	if err := reg.Register(searchableWidgetSpec()); err != nil {
		t.Fatalf("register widget: %v", err)
	}
	if err := reg.Validate(); err != nil {
		t.Fatalf("validate registry: %v", err)
	}

	world := fabriqtest.NewWorld(reg)
	exec, err := command.NewExecutor(reg, world.Store)
	if err != nil {
		t.Fatalf("new executor: %v", err)
	}

	ctx, err := tenant.WithTenant(t.Context(), testTenantID)
	if err != nil {
		t.Fatalf("with tenant: %v", err)
	}

	ids := map[string]string{} // name -> id
	for _, payload := range []map[string]any{
		{"name": "Sprocket", "colour": "blue"},
		{"name": "Cog", "colour": "red"},
	} {
		res, execErr := exec.Exec(ctx, command.Command{Entity: "widget", Op: command.OpCreate, Payload: payload})
		if execErr != nil {
			t.Fatalf("seed widget %v: %v", payload, execErr)
		}
		id := res.AggID
		ids[payload["name"].(string)] = id

		// Index the row into the search projection. The doc must carry the
		// tenant column so FakeSearch scopes it to the test tenant.
		doc := map[string]any{
			registry.ColumnID:     id,
			registry.ColumnTenant: testTenantID,
			"name":                payload["name"],
			"colour":              payload["colour"],
		}
		if applyErr := world.Search.ApplyMutations(ctx, "ds_widgets", []projection.Mutation{
			projection.DocIndex{Index: "ds_widgets", ID: id, Doc: doc, Version: 1},
		}); applyErr != nil {
			t.Fatalf("index search doc: %v", applyErr)
		}

		// Upsert an embedding. Sprocket points along axis 0 (matches the stub
		// embedder's query vector); Cog points along axis 1 (orthogonal), so the
		// text query ranks Sprocket first.
		emb := []float32{0, 1, 0}
		if payload["name"] == "Sprocket" {
			emb = []float32{1, 0, 0}
		}
		if upErr := world.Vector.Upsert(ctx, "widget", id, emb, map[string]any{"name": payload["name"]}); upErr != nil {
			t.Fatalf("upsert embedding: %v", upErr)
		}
	}

	return world, ids
}

// postVector issues a POST to the vector search route with a JSON body and the
// test tenant header stamped.
func postVector(t *testing.T, srv *httptest.Server, body any) *http.Response {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/search/vector", bytes.NewReader(buf))
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

// TestSearch_RealFake exercises the full-text path against the FakeSearch
// backend: a query for "sprocket" returns the seeded Sprocket row.
func TestSearch_RealFake(t *testing.T) {
	world, _ := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/search?type=widget&q=sprocket")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got searchResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Items) != 1 {
		t.Fatalf("items len = %d, want 1 (%+v)", len(got.Items), got.Items)
	}
	if got.Items[0].Type != "widget" {
		t.Errorf("type = %q, want widget", got.Items[0].Type)
	}
	if got.Items[0].Data["name"] != "Sprocket" {
		t.Errorf("name = %v, want Sprocket", got.Items[0].Data["name"])
	}
}

// TestSearch_MissingType verifies that omitting ?type= returns 400.
func TestSearch_MissingType(t *testing.T) {
	world, _ := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/search?q=sprocket")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestSearch_MissingQuery verifies that omitting ?q= returns 400.
func TestSearch_MissingQuery(t *testing.T) {
	world, _ := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/search?type=widget")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestVectorSearch_TextMode exercises the TEXT vector path with a stub
// embedder: the query embeds to a vector closest to Sprocket, which ranks first
// and is hydrated from the relational source of truth.
func TestVectorSearch_TextMode(t *testing.T) {
	world, ids := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world, WithEmbedder(stubEmbedder{dims: 3}))
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postVector(t, srv, map[string]any{
		"type": "widget", "query": "spinny thing", "k": 5,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got vectorSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Matches) == 0 {
		t.Fatal("expected at least one match")
	}
	// Sprocket's embedding aligns with the stub query vector, so it ranks first.
	if got.Matches[0].ID != ids["Sprocket"] {
		t.Errorf("top match id = %q, want Sprocket id %q", got.Matches[0].ID, ids["Sprocket"])
	}
	if got.Matches[0].Data["name"] != "Sprocket" {
		t.Errorf("top match data name = %v, want Sprocket (hydration)", got.Matches[0].Data["name"])
	}
}

// TestVectorSearch_SimilarToEntity exercises the SIMILAR-TO-ENTITY path: it
// passes a stored row id, the handler fetches that embedding and finds the row
// itself as the closest match (cosine 1.0 with itself).
func TestVectorSearch_SimilarToEntity(t *testing.T) {
	world, ids := buildSearchWorld(t)
	// No embedder: this path must not need one.
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postVector(t, srv, map[string]any{
		"type": "widget", "id": ids["Cog"], "k": 5,
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got vectorSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Matches) == 0 {
		t.Fatal("expected at least one match")
	}
	// The row is most similar to itself.
	if got.Matches[0].ID != ids["Cog"] {
		t.Errorf("top match id = %q, want Cog id %q", got.Matches[0].ID, ids["Cog"])
	}
}

// TestVectorSearch_TextNoEmbedder verifies a TEXT query with no embedder
// configured returns 501 with the documented error message.
func TestVectorSearch_TextNoEmbedder(t *testing.T) {
	world, _ := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world) // no WithEmbedder
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postVector(t, srv, map[string]any{
		"type": "widget", "query": "anything",
	})
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 501, body = %s", resp.StatusCode, body)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"] == "" {
		t.Error("expected non-empty error message")
	}
}

// TestVectorSearch_MissingType verifies a body with no type returns 400.
func TestVectorSearch_MissingType(t *testing.T) {
	world, _ := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postVector(t, srv, map[string]any{"query": "x"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestVectorSearch_NeitherQueryNorID verifies a body with type but no query/id
// returns 400.
func TestVectorSearch_NeitherQueryNorID(t *testing.T) {
	world, _ := buildSearchWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postVector(t, srv, map[string]any{"type": "widget"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestMeta_SearchCapabilities verifies the new capability slugs are advertised.
func TestMeta_SearchCapabilities(t *testing.T) {
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
	want := map[string]bool{"search.read": false, "vector.read": false}
	for _, c := range got.Capabilities {
		if _, ok := want[c]; ok {
			want[c] = true
		}
	}
	for slug, found := range want {
		if !found {
			t.Errorf("capability %q not advertised", slug)
		}
	}
}

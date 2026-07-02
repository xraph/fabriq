package adminapi

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestCrdtEntities_ListsDocumentEntities(t *testing.T) {
	world := buildDocWorld(t) // existing helper that registers a KindDocument entity
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/crdt/entities")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body=%s", resp.StatusCode, body)
	}
	var out struct {
		Items []struct {
			Entity string `json:"entity"`
			Kind   string `json:"kind"`
			Engine string `json:"engine"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Items) == 0 {
		t.Fatal("expected at least one document entity")
	}
	found := false
	for _, it := range out.Items {
		if it.Kind == "document" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no document-kind entity in %+v", out.Items)
	}
}

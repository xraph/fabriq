package adminapi

import (
	"net/http"
	"testing"
)

// The embedding inspect/delete happy paths need a real pgvector adapter, so they
// are covered by the live admin-demo verification. These unit tests cover the
// request-validation branches, which return before touching the vector port.

func TestVectorDeleteByMeta_MissingEntity(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/vector/delete-by-meta", testTenantID,
		map[string]any{"filter": map[string]any{"status": "x"}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestVectorDeleteByMeta_EmptyFilterWithoutAll(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	// Empty filter is the wipe-all path — rejected unless {all:true}.
	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/vector/delete-by-meta", testTenantID,
		map[string]any{"entity": "product"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

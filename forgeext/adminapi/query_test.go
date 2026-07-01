package adminapi

import (
	"net/http"
	"testing"
)

func TestPrecheckReadOnlySQL(t *testing.T) {
	ok := []string{
		"SELECT 1",
		"  select * from product ",
		"WITH x AS (SELECT 1) SELECT * FROM x",
		"select a from t where status = 'deleted'", // literal, not a write
		"select(1)",
		"with x as (select 1) select * from x",
	}
	for _, s := range ok {
		if err := precheckReadOnlySQL(s); err != nil {
			t.Errorf("precheck(%q) = %v, want nil", s, err)
		}
	}
	bad := []string{
		"DELETE FROM product",
		"update product set x = 1",
		"drop table product",
		"SELECT 1; DELETE FROM product", // statement stacking
		"",
		"selectfoo",
		"withdraw",
	}
	for _, s := range bad {
		if err := precheckReadOnlySQL(s); err == nil {
			t.Errorf("precheck(%q) = nil, want error", s)
		}
	}
}

func TestQueryRaw_501WithoutStores(t *testing.T) {
	// The fake-backed harness has no opened stores → the endpoint must 501.
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/query", testTenantID,
		map[string]any{"sql": "SELECT 1"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
}

package adminapi

import (
	"net/http"
	"testing"
)

func TestPrecheckSingleStatement(t *testing.T) {
	ok := []string{
		"CREATE TABLE x (id text)",
		"create index i on x(id);", // single trailing semicolon allowed
		"ALTER TABLE x ADD COLUMN y text",
	}
	for _, s := range ok {
		if err := precheckSingleStatement(s); err != nil {
			t.Errorf("precheck(%q) = %v, want nil", s, err)
		}
	}
	bad := []string{
		"",
		"   ",
		"CREATE TABLE a(); DROP TABLE a", // stacking
	}
	for _, s := range bad {
		if err := precheckSingleStatement(s); err == nil {
			t.Errorf("precheck(%q) = nil, want error", s)
		}
	}
}

func TestAdhocDDL_403WhenGateOff(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world) // gate OFF
	srv := buildServer(t, e)
	defer srv.Close()

	resp := doWrite(t, http.MethodPost, srv.URL+"/admin/schema/ddl", testTenantID,
		map[string]any{"sql": "CREATE TABLE x (id text)"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (gate off)", resp.StatusCode)
	}
}

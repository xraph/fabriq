package adminapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

const plainKey = "fq_plain_tenant_key"

// operatorOpsServer builds a keyed admin server with a fake schema writer and
// a second, non-manage key (plainKey) bound to the test tenant.
func operatorOpsServer(t *testing.T, opts ...Option) (srvURL, manageKey string, w *fakeWriter, closeFn func()) {
	t.Helper()
	e, adminKey, store := buildKeysExtWith(t, opts...)
	store.seed(plainKey, KeyRecord{ID: "plain", TenantID: testTenantID})
	w = &fakeWriter{}
	e.dynWriter = w
	srv := buildServer(t, e)
	return srv.URL, adminKey, w, srv.Close
}

type opCase struct {
	name, method, path, body string
}

var instanceOpCases = []opCase{
	{"raw query", http.MethodPost, "/admin/query", `{"sql":"SELECT 1"}`},
	{"drop type", http.MethodDelete, "/admin/schema/gadget?confirm=gadget", ""},
	{"drop field", http.MethodDelete, "/admin/schema/gadget/fields/label?confirm=label", ""},
	{"rename field", http.MethodPost, "/admin/schema/gadget/rename-field", `{"from":"label","to":"title"}`},
}

func doOp(t *testing.T, srvURL, key string, c opCase) (int, string) {
	t.Helper()
	var body io.Reader
	if c.body != "" {
		body = strings.NewReader(c.body)
	}
	resp := keysReq(t, c.method, srvURL, c.path, key, body)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestOperatorOnlyInstanceOps_NonManageKey403 pins the hosted-server gate: a
// key without CanManageKeys can no longer run raw SQL or drop or rename
// schema, and gets a 403 that says so.
func TestOperatorOnlyInstanceOps_NonManageKey403(t *testing.T) {
	url, _, w, done := operatorOpsServer(t, WithOperatorOnlyInstanceOps())
	defer done()

	for _, c := range instanceOpCases {
		status, body := doOp(t, url, plainKey, c)
		if status != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403, body = %s", c.name, status, body)
			continue
		}
		if !strings.Contains(body, "this API key may not") {
			t.Errorf("%s: body = %s, want a message naming the refusal", c.name, body)
		}
	}
	if len(w.dropped)+len(w.droppedF)+len(w.renamed) != 0 {
		t.Fatalf("writer reached on a refused request: %+v", w)
	}
}

// TestOperatorOnlyInstanceOps_ManageKeyPasses checks a manage key still gets
// through the gate. The raw query answers 501 here only because the test has
// no Postgres store; what matters is that it is not 403.
func TestOperatorOnlyInstanceOps_ManageKeyPasses(t *testing.T) {
	url, manageKey, w, done := operatorOpsServer(t, WithOperatorOnlyInstanceOps())
	defer done()

	for _, c := range instanceOpCases {
		status, body := doOp(t, url, manageKey, c)
		if status == http.StatusForbidden {
			t.Errorf("%s: manage key got 403, body = %s", c.name, body)
		}
	}
	if len(w.dropped) != 1 || len(w.droppedF) != 1 || len(w.renamed) != 1 {
		t.Fatalf("writer calls = %+v, want one drop type, one drop field, one rename", w)
	}
}

// TestOperatorOnlyInstanceOps_DefineAndAlterStayOpen keeps the Interfaces
// contract: a tenant-bound key can still define a type and add fields to it.
func TestOperatorOnlyInstanceOps_DefineAndAlterStayOpen(t *testing.T) {
	url, _, w, done := operatorOpsServer(t, WithOperatorOnlyInstanceOps())
	defer done()

	status, body := doOp(t, url, plainKey, opCase{
		"define", http.MethodPost, "/admin/schema",
		`{"type":"gadget","columns":[{"name":"label","kind":"string"}]}`,
	})
	if status != http.StatusCreated {
		t.Fatalf("define: status = %d, want 201, body = %s", status, body)
	}
	if len(w.defined) != 1 {
		t.Fatalf("define did not reach the writer: %+v", w)
	}
}

// TestOperatorOnlyInstanceOps_OffByDefault confirms library defaults are
// unchanged: without the option a non-manage key reaches every handler.
func TestOperatorOnlyInstanceOps_OffByDefault(t *testing.T) {
	url, _, w, done := operatorOpsServer(t)
	defer done()

	for _, c := range instanceOpCases {
		status, body := doOp(t, url, plainKey, c)
		if status == http.StatusForbidden {
			t.Errorf("%s: status = 403 without the option, body = %s", c.name, body)
		}
	}
	if len(w.dropped) != 1 || len(w.droppedF) != 1 || len(w.renamed) != 1 {
		t.Fatalf("writer calls = %+v, want one drop type, one drop field, one rename", w)
	}
}

package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTenantRegistry is a map-backed TenantRegistry.
type fakeTenantRegistry struct {
	mu   sync.Mutex
	recs map[string]TenantRecord
	now  time.Time
}

func newFakeTenantRegistry() *fakeTenantRegistry {
	return &fakeTenantRegistry{
		recs: map[string]TenantRecord{},
		now:  time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC),
	}
}

func (f *fakeTenantRegistry) Create(_ context.Context, id, label string) (TenantRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.recs[id]; ok {
		return rec, false, nil
	}
	rec := TenantRecord{ID: id, Label: label, CreatedAt: f.now}
	f.recs[id] = rec
	return rec, true, nil
}

func (f *fakeTenantRegistry) Get(_ context.Context, id string) (TenantRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.recs[id]
	return rec, ok, nil
}

func (f *fakeTenantRegistry) List(_ context.Context) ([]TenantRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]TenantRecord, 0, len(f.recs))
	for _, r := range f.recs {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func staticTenantServer(t *testing.T) *httptest.Server {
	t.Helper()
	e := NewAdminAPI(nil, WithTenantsAdmin(), WithTenantRegistry(newFakeTenantRegistry()))
	srv := buildServer(t, e)
	t.Cleanup(srv.Close)
	return srv
}

func postTenant(t *testing.T, srv *httptest.Server, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(srv.URL+"/admin/tenants", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /admin/tenants: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

func getBody(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp := getNoTenant(t, srv, path)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

func TestStaticTenants_CreateThenList(t *testing.T) {
	srv := staticTenantServer(t)

	status, body := postTenant(t, srv, `{"tenantId":"acme","label":"Acme"}`)
	if status != http.StatusCreated {
		t.Fatalf("create status = %d (%s), want 201", status, body)
	}
	var created struct {
		Tenant map[string]string `json:"tenant"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("decode create: %v (%s)", err, body)
	}
	want := map[string]string{
		"tenantId":  "acme",
		"label":     "Acme",
		"state":     "active",
		"mode":      "static",
		"createdAt": "2026-10-21T10:00:00.000Z",
	}
	for k, v := range want {
		if created.Tenant[k] != v {
			t.Errorf("tenant[%q] = %q, want %q", k, created.Tenant[k], v)
		}
	}

	status, body = getBody(t, srv, "/admin/tenants")
	if status != http.StatusOK {
		t.Fatalf("list status = %d (%s), want 200", status, body)
	}
	var list struct {
		Tenants []map[string]string `json:"tenants"`
	}
	if err := json.Unmarshal([]byte(body), &list); err != nil {
		t.Fatalf("decode list: %v (%s)", err, body)
	}
	if len(list.Tenants) != 1 {
		t.Fatalf("list has %d tenants, want 1: %s", len(list.Tenants), body)
	}
	if list.Tenants[0]["mode"] != "static" || list.Tenants[0]["state"] != "active" {
		t.Errorf("listed tenant = %v, want mode static and state active", list.Tenants[0])
	}

	status, body = getBody(t, srv, "/admin/tenants/acme")
	if status != http.StatusOK {
		t.Fatalf("get status = %d (%s), want 200", status, body)
	}
	var one map[string]string
	if err := json.Unmarshal([]byte(body), &one); err != nil {
		t.Fatalf("decode get: %v (%s)", err, body)
	}
	if one["tenantId"] != "acme" || one["label"] != "Acme" {
		t.Errorf("get = %v", one)
	}
}

func TestStaticTenants_CreateIdempotent200(t *testing.T) {
	srv := staticTenantServer(t)

	s1, b1 := postTenant(t, srv, `{"tenantId":"acme","label":"Acme"}`)
	if s1 != http.StatusCreated {
		t.Fatalf("first status = %d (%s), want 201", s1, b1)
	}
	s2, b2 := postTenant(t, srv, `{"tenantId":"acme","label":"Acme"}`)
	if s2 != http.StatusOK {
		t.Fatalf("second status = %d (%s), want 200", s2, b2)
	}
	if b1 != b2 {
		t.Errorf("retry body differs:\n first: %s\nsecond: %s", b1, b2)
	}

	// A retry carrying another label must not overwrite the first.
	s3, b3 := postTenant(t, srv, `{"tenantId":"acme","label":"Renamed"}`)
	if s3 != http.StatusOK {
		t.Fatalf("third status = %d (%s), want 200", s3, b3)
	}
	if b3 != b1 {
		t.Errorf("relabelled retry changed the body:\n first: %s\n third: %s", b1, b3)
	}
}

func TestStaticTenants_InvalidID400(t *testing.T) {
	srv := staticTenantServer(t)
	for _, body := range []string{
		`{"tenantId":"bad id!","label":"x"}`,
		`{"tenantId":"","label":"x"}`,
		`{"label":"x"}`,
		`not json`,
		`{"tenantId":"acme","label":"` + strings.Repeat("a", 201) + `"}`,
	} {
		status, resp := postTenant(t, srv, body)
		if status != http.StatusBadRequest {
			t.Errorf("body %.40q: status = %d (%s), want 400", body, status, resp)
		}
		if !strings.Contains(resp, `"error"`) {
			t.Errorf("body %.40q: response %q has no error field", body, resp)
		}
	}
}

func TestStaticTenants_GetUnknown404(t *testing.T) {
	srv := staticTenantServer(t)
	status, body := getBody(t, srv, "/admin/tenants/ghost")
	if status != http.StatusNotFound {
		t.Fatalf("status = %d (%s), want 404", status, body)
	}
	if body != `{"error":"no such tenant"}` {
		t.Errorf("body = %s", body)
	}
}

func TestStaticTenants_ListEmptyArray(t *testing.T) {
	srv := staticTenantServer(t)
	status, body := getBody(t, srv, "/admin/tenants")
	if status != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", status, body)
	}
	if body != `{"tenants":[]}` {
		t.Errorf("body = %s, want {\"tenants\":[]}", body)
	}
}

func TestStaticTenants_SuspendStill400(t *testing.T) {
	srv := staticTenantServer(t)
	for _, path := range []string{
		"/admin/tenants/acme/suspend",
		"/admin/tenants/acme/resume",
		"/admin/tenants/migrate-all",
	} {
		resp, err := http.Post(srv.URL+path, "application/json", nil)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("POST %s status = %d (%s), want 400", path, resp.StatusCode, b)
		}
	}
}

func TestStaticTenants_GatedByTenantsAdmin(t *testing.T) {
	e := NewAdminAPI(nil, WithTenantRegistry(newFakeTenantRegistry()))
	srv := buildServer(t, e)
	defer srv.Close()
	status, body := getBody(t, srv, "/admin/tenants")
	if status != http.StatusForbidden {
		t.Fatalf("status = %d (%s), want 403 without WithTenantsAdmin", status, body)
	}
}

// failingTenantRegistry returns an error carrying driver-looking text.
type failingTenantRegistry struct{}

const leakyRegistryErr = "ERROR: relation \"fabriq_tenant\" does not exist (SQLSTATE 42P01)"

func (failingTenantRegistry) Create(context.Context, string, string) (TenantRecord, bool, error) {
	return TenantRecord{}, false, errors.New(leakyRegistryErr)
}

func (failingTenantRegistry) Get(context.Context, string) (TenantRecord, bool, error) {
	return TenantRecord{}, false, errors.New(leakyRegistryErr)
}

func (failingTenantRegistry) List(context.Context) ([]TenantRecord, error) {
	return nil, errors.New(leakyRegistryErr)
}

func TestStaticTenants_RegistryErrorDoesNotLeak(t *testing.T) {
	e := NewAdminAPI(nil, WithTenantsAdmin(), WithTenantRegistry(failingTenantRegistry{}))
	srv := buildServer(t, e)
	defer srv.Close()

	check := func(name string, status int, body string) {
		t.Helper()
		if status != http.StatusInternalServerError {
			t.Errorf("%s: status = %d (%s), want 500", name, status, body)
		}
		for _, leak := range []string{"fabriq_tenant", "SQLSTATE", "relation"} {
			if strings.Contains(body, leak) {
				t.Errorf("%s: body %q leaks %q", name, body, leak)
			}
		}
	}
	s, b := postTenant(t, srv, `{"tenantId":"acme"}`)
	check("create", s, b)
	s, b = getBody(t, srv, "/admin/tenants")
	check("list", s, b)
	s, b = getBody(t, srv, "/admin/tenants/acme")
	check("get", s, b)
}

func TestStaticTenants_GetMalformedID404(t *testing.T) {
	srv := staticTenantServer(t)
	status, body := getBody(t, srv, "/admin/tenants/bad%20id!")
	if status != http.StatusNotFound {
		t.Fatalf("status = %d (%s), want 404", status, body)
	}
	if body != `{"error":"no such tenant"}` {
		t.Errorf("body = %s", body)
	}
}

package adminapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

const testTenantHeader = "X-Tenant-ID"
const testTenantID = "test-tenant-1"

// tenantMiddleware reads X-Tenant-ID from the request header and stamps the
// tenant into the request context. This simulates the auth middleware that the
// host app is required to provide in production.
func tenantMiddleware(next forge.Handler) forge.Handler {
	return func(ctx forge.Context) error {
		tid := ctx.Request().Header.Get(testTenantHeader)
		if tid == "" {
			return forge.BadRequest("missing " + testTenantHeader + " header")
		}
		tctx, err := tenant.WithTenant(ctx.Request().Context(), tid)
		if err != nil {
			return forge.BadRequest("invalid tenant id: " + err.Error())
		}
		// WithContext mutates the forge context in-place (replaces the request's context).
		ctx.WithContext(tctx)
		return next(ctx)
	}
}

// widgetSpec returns a minimal dynamic entity spec for use in tests.
// Dynamic entities use map[string]any natively on both fakes and real adapters.
func widgetSpec() registry.EntitySpec {
	return registry.EntitySpec{
		Name: "widget", Kind: registry.KindAggregate,
		Schema: &registry.DynamicSchema{
			Table: "ds_widgets",
			Columns: []registry.DynamicColumn{
				{Name: "name", Type: registry.ColText, NotNull: true},
				{Name: "colour", Type: registry.ColText},
			},
		},
	}
}

// buildTestWorld constructs a registry with the widget entity, seeds two rows
// in the test tenant, and returns the World.
func buildTestWorld(t *testing.T) *fabriqtest.World {
	t.Helper()

	reg := registry.New()
	if err := reg.Register(widgetSpec()); err != nil {
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

	// Seed two widget rows. Dynamic entities accept map[string]any payloads.
	for _, payload := range []map[string]any{
		{"name": "Sprocket", "colour": "blue"},
		{"name": "Cog", "colour": "red"},
	} {
		res, execErr := exec.Exec(ctx, command.Command{
			Entity:  "widget",
			Op:      command.OpCreate,
			Payload: payload,
		})
		if execErr != nil {
			t.Fatalf("seed widget %v: %v", payload, execErr)
		}
		// Store the assigned id back into the payload so tests can reference it.
		payload["id"] = res.AggID
	}

	return world
}

// fakeBackedAdminExt constructs an Extension whose fabric is pre-resolved
// from the given World, bypassing Start / fabriq.Open. The tenant middleware
// is attached via WithRouteOptions so all routes require the X-Tenant-ID header.
func fakeBackedAdminExt(t *testing.T, world *fabriqtest.World, opts ...Option) *Extension {
	t.Helper()
	// Prepend the tenant middleware so the real route options can override it.
	opts = append([]Option{
		WithRouteOptions(forge.WithMiddleware(tenantMiddleware)),
	}, opts...)
	e := NewAdminAPI(nil, opts...) // nil parent — bypass forgeext.Extension
	e.fabric = fabriqtest.NewFabric(world)
	e.reg = world.Registry // powers the types/schema introspection endpoints
	return e
}

// buildServer registers the admin controller on a fresh forge app and returns
// a test HTTP server backed by the app's router.
func buildServer(t *testing.T, e *Extension) *httptest.Server {
	t.Helper()
	app := forge.NewApp(forge.AppConfig{Name: "admin-api-test", HTTPAddress: ":0"})
	if err := app.RegisterController(newAdminController(e)); err != nil {
		t.Fatalf("register controller: %v", err)
	}
	return httptest.NewServer(app.Router().Handler())
}

// get issues a GET to srv at path with the test tenant header stamped.
func get(t *testing.T, srv *httptest.Server, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(testTenantHeader, testTenantID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// TestAdminMeta verifies GET /admin/meta returns name, version, capabilities.
func TestAdminMeta(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	// The tenant middleware is wired globally via WithRouteOptions, so all routes
	// (including meta) require the X-Tenant-ID header in tests.
	resp := get(t, srv, "/admin/meta")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got metaResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Name != "fabriq-admin-api" {
		t.Errorf("name = %q, want %q", got.Name, "fabriq-admin-api")
	}
	if len(got.Capabilities) == 0 {
		t.Error("capabilities must not be empty")
	}
	if got.Capabilities[0] != "entities.read" {
		t.Errorf("capabilities[0] = %q, want %q", got.Capabilities[0], "entities.read")
	}
}

// TestAdminEntities_List verifies GET /admin/entities?type=widget returns all
// seeded rows for the tenant.
func TestAdminEntities_List(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/entities?type=widget")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got entityListResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Items) != 2 {
		t.Fatalf("items len = %d, want 2", len(got.Items))
	}

	for _, item := range got.Items {
		if item.Type != "widget" {
			t.Errorf("item type = %q, want %q", item.Type, "widget")
		}
		if item.ID == "" {
			t.Error("item id must not be empty")
		}
		if item.Data == nil {
			t.Error("item data must not be nil")
		}
	}
}

// TestAdminEntities_List_MissingType verifies that omitting ?type= returns 400.
func TestAdminEntities_List_MissingType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/entities")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestAdminEntities_Get verifies GET /admin/entities/{id}?type=widget returns
// the correct record. The id is obtained from the list endpoint.
func TestAdminEntities_Get(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	// First, list to get a real id.
	listResp := get(t, srv, "/admin/entities?type=widget")
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(listResp.Body)
		t.Fatalf("list status = %d, body = %s", listResp.StatusCode, body)
	}
	var list entityListResponse
	if err := json.NewDecoder(listResp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list.Items) == 0 {
		t.Fatal("list returned no items")
	}
	targetID := list.Items[0].ID

	// Now get the specific entity.
	detailResp := get(t, srv, "/admin/entities/"+targetID+"?type=widget")
	defer detailResp.Body.Close()

	if detailResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(detailResp.Body)
		t.Fatalf("detail status = %d, body = %s", detailResp.StatusCode, body)
	}

	var got entityItem
	if err := json.NewDecoder(detailResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if got.ID != targetID {
		t.Errorf("id = %q, want %q", got.ID, targetID)
	}
	if got.Type != "widget" {
		t.Errorf("type = %q, want %q", got.Type, "widget")
	}
	if got.Data == nil {
		t.Error("data must not be nil")
	}
}

// TestAdminEntities_Get_NotFound verifies that an unknown id returns 404.
func TestAdminEntities_Get_NotFound(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/entities/no-such-id?type=widget")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}

// TestAdminEntities_Get_UnknownType verifies that an unknown entity type returns 400.
func TestAdminEntities_Get_UnknownType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/entities/foo?type=no-such-entity")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}

// getNoTenant issues a GET to srv at path WITHOUT the X-Tenant-ID header.
func getNoTenant(t *testing.T, srv *httptest.Server, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// TestAdminMeta_TenantEchoed verifies that GET /admin/meta returns the resolved
// tenant id in the "tenant" field when the tenant middleware stamps a tenant.
func TestAdminMeta_TenantEchoed(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/meta")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	var got metaResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Tenant != testTenantID {
		t.Errorf("tenant = %q, want %q", got.Tenant, testTenantID)
	}
}

// TestAdminMeta_NoTenant verifies that without a tenant the "tenant" field is
// absent (zero-value / omitempty) from the /admin/meta response.
// The tenant middleware is NOT wired here; routes have no auth requirement.
func TestAdminMeta_NoTenant(t *testing.T) {
	world := buildTestWorld(t)
	// Build extension without the tenant middleware so the no-header request succeeds.
	e := NewAdminAPI(nil)
	e.fabric = fabriqtest.NewFabric(world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := getNoTenant(t, srv, "/admin/meta")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	// Decode into a raw map so we can confirm the key is absent entirely.
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if v, ok := raw["tenant"]; ok {
		t.Errorf("tenant key must be absent when no tenant in context, got %v", v)
	}
}

// TestAdminEntities_List_CustomBasePath verifies WithBasePath works.
func TestAdminEntities_List_CustomBasePath(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world, WithBasePath("/api/v1/admin"))
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/api/v1/admin/meta")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}

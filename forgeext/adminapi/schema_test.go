package adminapi

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// TestAdminEntities_Types verifies GET /admin/entities/types includes the
// registered dynamic entity type names.
func TestAdminEntities_Types(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/entities/types")
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	defer resp.Body.Close()

	var got entityTypesResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !containsStr(got.Types, "widget") {
		t.Errorf("types = %v, want to include widget", got.Types)
	}
}

// TestAdminEntities_Types_Multiple verifies that multiple registered dynamic
// types are all enumerated.
func TestAdminEntities_Types_Multiple(t *testing.T) {
	world := buildPluginWorld(t) // registers admin_plugin_remote
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/entities/types")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	var got entityTypesResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !containsStr(got.Types, "admin_plugin_remote") {
		t.Errorf("types = %v, want to include admin_plugin_remote", got.Types)
	}
}

// TestAdminSchema verifies GET /admin/schema?type=widget returns the field
// descriptors for the dynamic entity.
func TestAdminSchema(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/schema?type=widget")
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	defer resp.Body.Close()

	var got schemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Type != "widget" {
		t.Errorf("type = %q, want widget", got.Type)
	}
	if len(got.Fields) != 2 {
		t.Fatalf("fields len = %d, want 2 (name, colour)", len(got.Fields))
	}

	byName := map[string]schemaField{}
	for _, f := range got.Fields {
		byName[f.Name] = f
	}
	name, ok := byName["name"]
	if !ok {
		t.Fatal("missing 'name' field")
	}
	if name.Kind != "string" {
		t.Errorf("name.kind = %q, want string", name.Kind)
	}
	if !name.Required {
		t.Error("name.required = false, want true (NotNull)")
	}
	colour, ok := byName["colour"]
	if !ok {
		t.Fatal("missing 'colour' field")
	}
	if colour.Required {
		t.Error("colour.required = true, want false (nullable)")
	}
}

// TestAdminSchema_UnknownType verifies GET /admin/schema?type=unknown returns 400.
func TestAdminSchema_UnknownType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/schema?type=no-such-entity")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestAdminSchema_MissingType verifies GET /admin/schema without ?type= returns 400.
func TestAdminSchema_MissingType(t *testing.T) {
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := get(t, srv, "/admin/schema")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// containsStr reports whether s is in xs.
func containsStr(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

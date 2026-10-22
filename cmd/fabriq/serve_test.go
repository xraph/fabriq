package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/forgeext/adminapi"
)

func TestLoadServeSettings_Defaults(t *testing.T) {
	got, err := loadServeSettings(envFrom(nil))
	if err != nil {
		t.Fatalf("loadServeSettings: %v", err)
	}
	want := serveSettings{
		HTTPAddr:            ":8081",
		ReconcileInterval:   5 * time.Minute,
		DomainPack:          "example",
		DynamicPersist:      true,
		DynamicSyncInterval: 15 * time.Second,
		AdminAPI:            false,
		AdminLoginUser:      "",
		AdminLoginPassword:  "",
		AllowRLSBypass:      false,
	}
	if got != want {
		t.Fatalf("defaults = %+v, want %+v", got, want)
	}
}

func TestLoadServeSettings_Overrides(t *testing.T) {
	got, err := loadServeSettings(envFrom(map[string]string{
		"FABRIQ_HTTP_ADDR":             "127.0.0.1:9090",
		"FABRIQ_RECONCILE_INTERVAL":    "0",
		"FABRIQ_DOMAIN_PACK":           "none",
		"FABRIQ_DYNAMIC_PERSIST":       "false",
		"FABRIQ_DYNAMIC_SYNC_INTERVAL": "2m",
		"FABRIQ_ADMIN_API":             "true",
		"FABRIQ_ADMIN_LOGIN_USER":      "ops",
		"FABRIQ_ADMIN_LOGIN_PASSWORD":  "correct horse",
		"FABRIQ_ALLOW_RLS_BYPASS":      "true",
	}))
	if err != nil {
		t.Fatalf("loadServeSettings: %v", err)
	}
	want := serveSettings{
		HTTPAddr:            "127.0.0.1:9090",
		ReconcileInterval:   0,
		DomainPack:          "none",
		DynamicPersist:      false,
		DynamicSyncInterval: 2 * time.Minute,
		AdminAPI:            true,
		AdminLoginUser:      "ops",
		AdminLoginPassword:  "correct horse",
		AllowRLSBypass:      true,
	}
	if got != want {
		t.Fatalf("overrides = %+v, want %+v", got, want)
	}
}

func TestLoadServeSettings_LoginPairIncomplete_Errors(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"only user":     {"FABRIQ_ADMIN_LOGIN_USER": "ops"},
		"only password": {"FABRIQ_ADMIN_LOGIN_PASSWORD": "secret"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadServeSettings(envFrom(env))
			if err == nil {
				t.Fatal("loadServeSettings accepted half a login pair")
			}
			for _, v := range []string{"FABRIQ_ADMIN_LOGIN_USER", "FABRIQ_ADMIN_LOGIN_PASSWORD"} {
				if !strings.Contains(err.Error(), v) {
					t.Errorf("err = %v, want it to name %s", err, v)
				}
			}
		})
	}
}

func TestLoadServeSettings_BadValues_Errors(t *testing.T) {
	for _, tc := range []struct{ key, val string }{
		{"FABRIQ_ADMIN_API", "maybe"},
		{"FABRIQ_DYNAMIC_SYNC_INTERVAL", "soon"},
		{"FABRIQ_DOMAIN_PACK", "x"},
		{"FABRIQ_DYNAMIC_PERSIST", "perhaps"},
		{"FABRIQ_ALLOW_RLS_BYPASS", "sometimes"},
		{"FABRIQ_RECONCILE_INTERVAL", "hourly"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			_, err := loadServeSettings(envFrom(map[string]string{tc.key: tc.val}))
			if err == nil {
				t.Fatalf("%s=%q accepted", tc.key, tc.val)
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("err = %v, want it to name %s", err, tc.key)
			}
		})
	}
}

// extensionNames lists the names of the extensions registered on app.
func extensionNames(app forge.App) []string {
	exts := app.Extensions()
	names := make([]string, 0, len(exts))
	for _, ext := range exts {
		names = append(names, ext.Name())
	}
	return names
}

// statusOf serves one GET through app's router without starting the app.
func statusOf(t *testing.T, app forge.App, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	app.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// testServeSettings returns the defaults with the overrides in env applied.
func testServeSettings(t *testing.T, env map[string]string) serveSettings {
	t.Helper()
	s, err := loadServeSettings(envFrom(env))
	if err != nil {
		t.Fatalf("loadServeSettings: %v", err)
	}
	return s
}

func TestBuildServeApp_AdminOff_NoAdminRoutes(t *testing.T) {
	s := testServeSettings(t, nil)
	app, err := buildServeApp(s, fabriq.Config{})
	if err != nil {
		t.Fatalf("buildServeApp: %v", err)
	}
	if got := extensionNames(app); !slices.Equal(got, []string{"fabriq"}) {
		t.Fatalf("extensions = %v, want [fabriq]", got)
	}
	if code := statusOf(t, app, "/_/livez"); code != http.StatusOK {
		t.Errorf("GET /_/livez = %d, want 200", code)
	}
	if code := statusOf(t, app, "/_/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("GET /_/readyz before Start = %d, want 503", code)
	}
}

func TestBuildServeApp_AdminOn_RegistersAdminExtension(t *testing.T) {
	s := testServeSettings(t, map[string]string{
		"FABRIQ_ADMIN_API":            "true",
		"FABRIQ_ADMIN_LOGIN_USER":     "ops",
		"FABRIQ_ADMIN_LOGIN_PASSWORD": "secret",
	})
	app, err := buildServeApp(s, fabriq.Config{})
	if err != nil {
		t.Fatalf("buildServeApp: %v", err)
	}
	if got := extensionNames(app); !slices.Equal(got, []string{"fabriq", "fabriq-admin-api"}) {
		t.Fatalf("extensions = %v, want [fabriq fabriq-admin-api]", got)
	}
	if code := statusOf(t, app, "/_/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("GET /_/readyz before Start = %d, want 503", code)
	}
}

func TestBuildServeApp_SetsDynamicConfig(t *testing.T) {
	s := testServeSettings(t, map[string]string{"FABRIQ_DYNAMIC_SYNC_INTERVAL": "40s"})
	app, err := buildServeApp(s, fabriq.Config{})
	if err != nil {
		t.Fatalf("buildServeApp: %v", err)
	}
	got := fabricExtension(t, app).Config().Dynamic
	if want := (fabriq.DynamicConfig{Persist: true, SyncInterval: 40 * time.Second}); got != want {
		t.Fatalf("Dynamic = %+v, want %+v", got, want)
	}
}

// Catalog mode refuses dynamic.persist, so the default FABRIQ_DYNAMIC_PERSIST
// must not turn a working catalog-mode deployment into a failing one.
func TestBuildServeApp_CatalogMode_NoPersist(t *testing.T) {
	s := testServeSettings(t, nil)
	cfg := fabriq.Config{Catalog: fabriq.CatalogConfig{DSN: "postgres://catalog", ClusterDSNs: map[string]string{"c1": "postgres://c1"}}}
	app, err := buildServeApp(s, cfg)
	if err != nil {
		t.Fatalf("buildServeApp: %v", err)
	}
	got := fabricExtension(t, app).Config()
	if got.Dynamic.Persist {
		t.Fatal("Dynamic.Persist = true in catalog mode, want false")
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("catalog config no longer validates: %v", err)
	}
}

func TestBuildServeApp_UnknownPack_Errors(t *testing.T) {
	s := testServeSettings(t, nil)
	s.DomainPack = "x"
	if _, err := buildServeApp(s, fabriq.Config{}); err == nil || !strings.Contains(err.Error(), "FABRIQ_DOMAIN_PACK") {
		t.Fatalf("err = %v, want an unknown FABRIQ_DOMAIN_PACK error", err)
	}
}

// fabricExtension returns the fabriq extension registered on app.
func fabricExtension(t *testing.T, app forge.App) *forgeext.Extension {
	t.Helper()
	for _, ext := range app.Extensions() {
		if fe, ok := ext.(*forgeext.Extension); ok {
			return fe
		}
	}
	t.Fatal("no fabriq extension registered")
	return nil
}

// adminExtension returns the admin API extension registered on app, or nil.
func adminExtension(app forge.App) *adminapi.Extension {
	for _, ext := range app.Extensions() {
		if ae, ok := ext.(*adminapi.Extension); ok {
			return ae
		}
	}
	return nil
}

// clearServeEnv blanks every variable setup reads, so the developer's shell
// cannot leak into a test.
func clearServeEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"FABRIQ_POSTGRES_DSN", "FABRIQ_REDIS_ADDR", "FABRIQ_HTTP_ADDR", "FABRIQ_RECONCILE_INTERVAL",
		"FABRIQ_DOMAIN_PACK", "FABRIQ_DYNAMIC_PERSIST", "FABRIQ_DYNAMIC_SYNC_INTERVAL",
		"FABRIQ_ADMIN_API", "FABRIQ_ADMIN_KEY", "FABRIQ_ADMIN_LOGIN_USER", "FABRIQ_ADMIN_LOGIN_PASSWORD",
		"FABRIQ_ALLOW_RLS_BYPASS",
	} {
		t.Setenv(k, "")
	}
}

// setup is resolved for every command, migrate included. With the admin API
// on and no DSN and no FABRIQ_ADMIN_KEY it must still build the app, and it
// must not start either extension: the admin API's Register (and so its
// required-auth DB check) runs only inside app.Start.
func TestSetup_AdminOn_NoDSN_DoesNoIO(t *testing.T) {
	clearServeEnv(t)
	t.Setenv("FABRIQ_ADMIN_API", "true")

	app, err := setup(nil)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if got := extensionNames(app); !slices.Equal(got, []string{"fabriq", "fabriq-admin-api"}) {
		t.Fatalf("extensions = %v, want [fabriq fabriq-admin-api]", got)
	}
	ctx := context.Background()
	if fe := fabricExtension(t, app); fe.Fabriq() != nil || fe.Ready(ctx) == nil {
		t.Fatal("setup opened the fabriq facade")
	}
	ae := adminExtension(app)
	if ae == nil {
		t.Fatal("no admin extension registered")
	}
	if ae.IsStarted() || ae.Ready(ctx) == nil {
		t.Fatal("setup started the admin API")
	}
}

func TestSetup_BadEnv_Errors(t *testing.T) {
	clearServeEnv(t)
	t.Setenv("FABRIQ_ADMIN_API", "maybe")
	if _, err := setup(nil); err == nil || !strings.Contains(err.Error(), "FABRIQ_ADMIN_API") {
		t.Fatalf("setup err = %v, want an error naming FABRIQ_ADMIN_API", err)
	}
}

// `fabriq migrate up` goes through setup too. With the admin API on and no
// FABRIQ_ADMIN_KEY it must fail only on the missing DSN: nothing may start the
// admin API or ask for its key.
func TestMigrateUp_AdminOn_NeverStartsAdminAPI(t *testing.T) {
	clearServeEnv(t)
	t.Setenv("FABRIQ_ADMIN_API", "true")

	origArgs := os.Args
	os.Args = []string{"fabriq", "migrate", "up"}
	t.Cleanup(func() { os.Args = origArgs })

	r := cli.NewAppRunner(setup)
	for _, opt := range runAppOptions() {
		opt(r)
	}
	err := r.Execute()
	if err == nil {
		t.Fatal("migrate up without a DSN succeeded")
	}
	if !strings.Contains(err.Error(), string(errMissingDSN)) {
		t.Fatalf("migrate up err = %v, want the missing-DSN error", err)
	}
	if strings.Contains(err.Error(), "FABRIQ_ADMIN_KEY") {
		t.Fatalf("migrate up asked for FABRIQ_ADMIN_KEY: %v", err)
	}
}

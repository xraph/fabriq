//go:build integration

package main

// End-to-end tests of the hosted `fabriq serve` lifecycle: the app setup
// builds, started against real containers and driven over HTTP the way
// Loomplane drives it. Each test starts the app with app.Start and serves
// app.Router() through httptest, so nothing listens on FABRIQ_HTTP_ADDR.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/client"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// newTestKey generates an admin key in the FABRIQ_ADMIN_KEY format: fq_
// followed by 40 random letters or digits.
func newTestKey(t *testing.T) string {
	t.Helper()
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 40)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			t.Fatalf("rand: %v", err)
		}
		b[i] = alphabet[n.Int64()]
	}
	return "fq_" + string(b)
}

// migrateUp applies every migration as the database owner, as
// `fabriq migrate up` does.
func migrateUp(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, dsn)
	if err != nil {
		t.Fatalf("open orchestrator: %v", err)
	}
	defer func() { _ = closeFn() }()
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

// appRoleDSN provisions the fabriq_app serving role, as
// `fabriq migrate app-role` does, and returns a DSN that connects as it.
func appRoleDSN(t *testing.T, superDSN string) string {
	t.Helper()
	const role, pass = "fabriq_app", "serve-e2e-password"
	if err := migrations.EnsureAppRole(context.Background(), superDSN, role, pass); err != nil {
		t.Fatalf("EnsureAppRole: %v", err)
	}
	u, err := url.Parse(superDSN)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	u.User = url.UserPassword(role, pass)
	return u.String()
}

// servedApp is a built serve app behind an httptest server.
type servedApp struct {
	app forge.App
	srv *httptest.Server
}

// newServedApp builds the serve app for s and cfg and serves its router. The
// app is not started.
func newServedApp(t *testing.T, s serveSettings, cfg fabriq.Config) *servedApp {
	t.Helper()
	app, err := buildServeApp(s, cfg)
	if err != nil {
		t.Fatalf("buildServeApp: %v", err)
	}
	srv := httptest.NewServer(app.Router().Handler())
	t.Cleanup(srv.Close)
	return &servedApp{app: app, srv: srv}
}

// start runs app.Start. forge does not stop the extensions that started before
// a failing one, so on failure the cleanup stops them itself; on success it
// stops the app (a no-op once the test has stopped it).
func (a *servedApp) start(t *testing.T) error {
	t.Helper()
	if err := a.app.Start(context.Background()); err != nil {
		t.Cleanup(func() {
			exts := a.app.Extensions()
			for i := len(exts) - 1; i >= 0; i-- {
				_ = exts[i].Stop(context.Background())
			}
		})
		return err
	}
	t.Cleanup(func() { _ = a.app.Stop(context.Background()) })
	return nil
}

// do sends one request with key as the bearer token (none when empty) and a
// JSON body (none when nil). It returns the status and the response body.
func (a *servedApp) do(t *testing.T, method, path, key string, body any) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, a.srv.URL+path, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := a.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s %s: %v", method, path, err)
	}
	return resp.StatusCode, out
}

// expect fails the test unless method path answers want.
func (a *servedApp) expect(t *testing.T, want int, method, path, key string, body any) []byte {
	t.Helper()
	got, out := a.do(t, method, path, key, body)
	if got != want {
		t.Fatalf("%s %s = %d, want %d; body = %s", method, path, got, want, out)
	}
	return out
}

// connect opens a Go client for tenant with key against the served app.
func (a *servedApp) connect(t *testing.T, key, tenant string) *client.Client {
	t.Helper()
	hostport := strings.TrimPrefix(a.srv.URL, "http://")
	c, err := client.Connect(context.Background(), "fabriq://"+key+"@"+hostport+"/"+tenant+"?tls=false")
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	return c
}

func TestServe_HostedLifecycle(t *testing.T) {
	ctx := context.Background()

	// 1. Postgres and Redis; migrate as the superuser; provision fabriq_app.
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	migrateUp(t, superDSN)
	appDSN := appRoleDSN(t, superDSN)

	// 2. A generated bootstrap key; admin API on, no demo pack, serving as
	// the app role.
	bootstrap := newTestKey(t)
	t.Setenv("FABRIQ_ADMIN_KEY", bootstrap)
	s := testServeSettings(t, map[string]string{"FABRIQ_ADMIN_API": "true", "FABRIQ_DOMAIN_PACK": "none"})
	cfg := fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
	}

	// 3. Not ready before Start, ready after.
	a := newServedApp(t, s, cfg)
	a.expect(t, http.StatusServiceUnavailable, http.MethodGet, "/_/readyz", "", nil)
	if err := a.start(t); err != nil {
		t.Fatalf("Start: %v", err)
	}
	a.expect(t, http.StatusOK, http.MethodGet, "/_/readyz", "", nil)

	// With no login pair set there is no password login at all, never the
	// admin/admin default.
	if code, out := a.do(t, http.MethodPost, "/admin/login", "",
		map[string]any{"username": "admin", "password": "admin"}); code < 400 {
		t.Fatalf("POST /admin/login admin/admin = %d, want a refusal; body = %s", code, out)
	}

	// 4. The bootstrap key registers a tenant without X-Tenant-ID; a retry is 200.
	a.expect(t, http.StatusCreated, http.MethodPost, "/admin/tenants", bootstrap, map[string]any{"tenantId": "acme"})
	a.expect(t, http.StatusOK, http.MethodPost, "/admin/tenants", bootstrap, map[string]any{"tenantId": "acme"})

	// 5. A key bound to the registered tenant; an unregistered tenant is 404.
	var issued struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	}
	out := a.expect(t, http.StatusCreated, http.MethodPost, "/admin/keys", bootstrap,
		map[string]any{"label": "svc", "tenantId": "acme"})
	if err := json.Unmarshal(out, &issued); err != nil {
		t.Fatalf("decode issued key: %v; body = %s", err, out)
	}
	if issued.ID == "" || !strings.HasPrefix(issued.Key, "fq_") {
		t.Fatalf("issued key = %+v, want an id and an fq_ key", issued)
	}
	a.expect(t, http.StatusNotFound, http.MethodPost, "/admin/keys", bootstrap,
		map[string]any{"label": "svc", "tenantId": "ghost"})

	// 6. The tenant key defines a type and writes a row through the Go client.
	c := a.connect(t, issued.Key, "acme")
	if _, err := c.CreateEntityType(ctx, client.CreateEntityTypeInput{
		Type:    "orders",
		Columns: []client.SchemaColumnInput{{Name: "sku", Kind: "string"}},
	}); err != nil {
		t.Fatalf("CreateEntityType: %v", err)
	}
	rec, err := c.CreateEntity(ctx, client.EntityWriteInput{Type: "orders", Data: map[string]any{"sku": "A1"}})
	if err != nil {
		t.Fatalf("CreateEntity: %v", err)
	}
	if rec.ID == "" {
		t.Fatal("CreateEntity returned no id")
	}

	// 7. A tenant key cannot manage tenants.
	a.expect(t, http.StatusForbidden, http.MethodGet, "/admin/tenants", issued.Key, nil)

	// 8. Restart: a fresh app over a new registry reloads the persisted type
	// and the row is still there.
	if err := a.app.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	b := newServedApp(t, s, cfg)
	if err := b.start(t); err != nil {
		t.Fatalf("restart: %v", err)
	}
	b.expect(t, http.StatusOK, http.MethodGet, "/_/readyz", "", nil)
	c = b.connect(t, issued.Key, "acme")
	types, err := c.ListEntityTypes(ctx)
	if err != nil {
		t.Fatalf("ListEntityTypes: %v", err)
	}
	if !slices.Contains(types, "orders") {
		t.Fatalf("types after restart = %v, want orders", types)
	}
	got, err := c.GetEntity(ctx, rec.ID, "orders")
	if err != nil {
		t.Fatalf("GetEntity after restart: %v", err)
	}
	if got.Data["sku"] != "A1" {
		t.Fatalf("sku after restart = %v, want A1", got.Data["sku"])
	}

	// 9. Revoking the tenant key locks it out on the next request.
	b.expect(t, http.StatusOK, http.MethodDelete, "/admin/keys/"+issued.ID, bootstrap, nil)
	b.expect(t, http.StatusUnauthorized, http.MethodGet, "/admin/meta", issued.Key, nil)
}

// hostedContainers starts Postgres and Redis and migrates Postgres. It returns
// the superuser DSN and the Redis address.
func hostedContainers(t *testing.T) (string, string) {
	t.Helper()
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	migrateUp(t, superDSN)
	return superDSN, redisAddr
}

// 10. The admin API refuses to start without FABRIQ_ADMIN_KEY.
func TestServe_AdminWithoutKey_FailsStart(t *testing.T) {
	superDSN, redisAddr := hostedContainers(t)
	appDSN := appRoleDSN(t, superDSN)
	t.Setenv("FABRIQ_ADMIN_KEY", "")

	s := testServeSettings(t, map[string]string{"FABRIQ_ADMIN_API": "true", "FABRIQ_DOMAIN_PACK": "none"})
	a := newServedApp(t, s, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
	})
	err := a.start(t)
	if err == nil {
		t.Fatal("Start without FABRIQ_ADMIN_KEY succeeded")
	}
	if !strings.Contains(err.Error(), "FABRIQ_ADMIN_KEY") {
		t.Fatalf("Start err = %v, want it to name FABRIQ_ADMIN_KEY", err)
	}
	a.expect(t, http.StatusServiceUnavailable, http.MethodGet, "/_/readyz", "", nil)
}

// 11. An unmigrated database fails Start with a pointer to `fabriq migrate up`.
func TestServe_UnmigratedDatabase_FailsStart(t *testing.T) {
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	t.Setenv("FABRIQ_ADMIN_KEY", newTestKey(t))

	s := testServeSettings(t, map[string]string{
		"FABRIQ_ADMIN_API":        "true",
		"FABRIQ_DOMAIN_PACK":      "none",
		"FABRIQ_ALLOW_RLS_BYPASS": "true",
	})
	a := newServedApp(t, s, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: superDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
	})
	err := a.start(t)
	if err == nil {
		t.Fatal("Start on an unmigrated database succeeded")
	}
	if !strings.Contains(err.Error(), "fabriq migrate up") {
		t.Fatalf("Start err = %v, want it to mention %q", err, "fabriq migrate up")
	}
}

// 12. With the admin API on, a superuser DSN is refused unless
// FABRIQ_ALLOW_RLS_BYPASS=true.
func TestServe_SuperuserRefused(t *testing.T) {
	superDSN, redisAddr := hostedContainers(t)
	t.Setenv("FABRIQ_ADMIN_KEY", newTestKey(t))
	cfg := fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: superDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
	}
	env := map[string]string{"FABRIQ_ADMIN_API": "true", "FABRIQ_DOMAIN_PACK": "none"}

	refused := newServedApp(t, testServeSettings(t, env), cfg)
	err := refused.start(t)
	if err == nil {
		t.Fatal("Start as a superuser with the admin API on succeeded")
	}
	if !strings.Contains(err.Error(), "bypasses row-level security") {
		t.Fatalf("Start err = %v, want it to mention %q", err, "bypasses row-level security")
	}

	env["FABRIQ_ALLOW_RLS_BYPASS"] = "true"
	allowed := newServedApp(t, testServeSettings(t, env), cfg)
	if err := allowed.start(t); err != nil {
		t.Fatalf("Start with FABRIQ_ALLOW_RLS_BYPASS=true: %v", err)
	}
	allowed.expect(t, http.StatusOK, http.MethodGet, "/_/readyz", "", nil)
}

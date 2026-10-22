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
	"time"

	goredis "github.com/redis/go-redis/v9"
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
	return serveApp(t, app)
}

// serveApp serves app's router through httptest. The app is not started.
func serveApp(t *testing.T, app forge.App) *servedApp {
	t.Helper()
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

	// 7. A tenant key cannot manage tenants, run raw SQL (which would read
	// the instance tables RLS does not fence) or drop a shared type.
	a.expect(t, http.StatusForbidden, http.MethodGet, "/admin/tenants", issued.Key, nil)
	a.expect(t, http.StatusForbidden, http.MethodPost, "/admin/query", issued.Key,
		map[string]any{"sql": "SELECT id, key_hash FROM fabriq_api_key"})
	a.expect(t, http.StatusForbidden, http.MethodDelete, "/admin/schema/orders?confirm=orders", issued.Key, nil)

	// 7a. The bootstrap key reads migration status with no X-Tenant-ID while
	// the server runs as fabriq_app, and sees every fabriq migration applied.
	out = a.expect(t, http.StatusOK, http.MethodGet, "/admin/migrations", bootstrap, nil)
	var status struct {
		Groups []struct {
			Name    string            `json:"name"`
			Applied []json.RawMessage `json:"applied"`
			Pending []json.RawMessage `json:"pending"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(out, &status); err != nil {
		t.Fatalf("decode migration status: %v; body = %s", err, out)
	}
	wantApplied := len(migrations.Group().Migrations())
	foundGroup := false
	for _, g := range status.Groups {
		if g.Name != migrations.GroupName {
			continue
		}
		foundGroup = true
		if len(g.Applied) != wantApplied || len(g.Pending) != 0 {
			t.Fatalf("fabriq group applied = %d pending = %d, want %d and 0", len(g.Applied), len(g.Pending), wantApplied)
		}
	}
	if !foundGroup {
		t.Fatalf("migration status has no %q group; body = %s", migrations.GroupName, out)
	}

	// 7b. One multi-tenant key without canManageKeys serves several tenants:
	// the DSN's tenant by default, another per request through WithTenant.
	var multi struct {
		Key string `json:"key"`
	}
	out = a.expect(t, http.StatusCreated, http.MethodPost, "/admin/keys", bootstrap, map[string]any{"label": "svc-multi"})
	if err := json.Unmarshal(out, &multi); err != nil || multi.Key == "" {
		t.Fatalf("decode multi-tenant key: %v; body = %s", err, out)
	}
	mc := a.connect(t, multi.Key, "acme")
	if _, err := mc.CreateEntity(ctx, client.EntityWriteInput{Type: "orders", Data: map[string]any{"sku": "M1"}}); err != nil {
		t.Fatalf("CreateEntity with the multi-tenant key: %v", err)
	}
	acmeRows, err := mc.ListEntities(ctx, client.ListEntitiesParams{Type: "orders"})
	if err != nil {
		t.Fatalf("ListEntities(acme): %v", err)
	}
	if len(acmeRows.Items) != 2 {
		t.Fatalf("acme rows = %d, want 2 (A1 and M1)", len(acmeRows.Items))
	}
	betaRows, err := mc.WithTenant("beta").ListEntities(ctx, client.ListEntitiesParams{Type: "orders"})
	if err != nil {
		t.Fatalf("ListEntities(beta): %v", err)
	}
	if len(betaRows.Items) != 0 {
		t.Fatalf("beta sees %d of acme's rows, want none: %+v", len(betaRows.Items), betaRows.Items)
	}
	a.expect(t, http.StatusForbidden, http.MethodGet, "/admin/tenants", multi.Key, nil)
	a.expect(t, http.StatusBadRequest, http.MethodGet, "/admin/entities?type=orders", multi.Key, nil)

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

// TestServe_RedisPassword drives serve through setup, so the Redis password
// travels the production path: FABRIQ_REDIS_PASSWORD, forgeext.LoadConfig,
// the Redis adapter. The right password serves; a wrong one never starts and
// never echoes the password; a password rotated under a running server turns
// readyz to 503 with a reason naming Redis.
func TestServe_RedisPassword(t *testing.T) {
	ctx := context.Background()
	const password = "serve-redis-password"
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedisWithPassword(t, password)
	migrateUp(t, superDSN)
	appDSN := appRoleDSN(t, superDSN)

	clearStoreEnv(t)
	t.Setenv("FABRIQ_POSTGRES_DSN", appDSN)
	t.Setenv("FABRIQ_REDIS_ADDR", redisAddr)
	t.Setenv("FABRIQ_DOMAIN_PACK", "none")
	setupApp := func() *servedApp {
		t.Helper()
		app, err := setup(nil)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		return serveApp(t, app)
	}

	const (
		wrong   = "not-the-redis-password"
		rotated = "rotated-redis-password"
	)
	// redact hides the fixture passwords in failure output, so a test that
	// guards against a leak never prints one itself.
	redact := strings.NewReplacer(password, "[REDACTED]", wrong, "[REDACTED]", rotated, "[REDACTED]").Replace
	t.Setenv("FABRIQ_REDIS_PASSWORD", wrong)
	bad := setupApp()
	err := bad.start(t)
	if err == nil {
		t.Fatal("Start with a wrong Redis password succeeded")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "redis") || strings.Contains(err.Error(), wrong) {
		t.Fatalf("Start err = %q, want it to name Redis and not echo the password", redact(err.Error()))
	}
	bad.expect(t, http.StatusServiceUnavailable, http.MethodGet, "/_/readyz", "", nil)

	t.Setenv("FABRIQ_REDIS_PASSWORD", password)
	good := setupApp()
	if err := good.start(t); err != nil {
		t.Fatalf("Start with FABRIQ_REDIS_PASSWORD: %s", redact(err.Error()))
	}
	good.expect(t, http.StatusOK, http.MethodGet, "/_/readyz", "", nil)

	// Rotate the password and drop every connection, so the server's pool
	// has to log in again with the password it was started with.
	rc := goredis.NewClient(&goredis.Options{Addr: redisAddr, Password: password})
	defer func() { _ = rc.Close() }()
	if err := rc.ConfigSet(ctx, "requirepass", rotated).Err(); err != nil {
		t.Fatalf("CONFIG SET requirepass: %v", err)
	}
	if err := rc.Do(ctx, "CLIENT", "KILL", "TYPE", "normal").Err(); err != nil {
		t.Fatalf("CLIENT KILL: %v", err)
	}
	var reason string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		code, body := good.do(t, http.MethodGet, "/_/readyz", "", nil)
		if code == http.StatusServiceUnavailable {
			var resp struct {
				Reason string `json:"reason"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatalf("decode readyz: %v; body = %s", err, redact(string(body)))
			}
			reason = resp.Reason
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !strings.Contains(strings.ToLower(reason), "redis") {
		t.Fatalf("readyz after rotation: reason = %q, want 503 with a reason naming Redis", redact(reason))
	}
	if redact(reason) != reason {
		t.Fatalf("readyz reason %q echoes a password", redact(reason))
	}
}

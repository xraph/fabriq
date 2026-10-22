//go:build integration

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestRebuild_AllTenantsStaticWithPasswords runs `fabriq rebuild` the way a
// hosted one-shot job does: every engine address and the Redis password come
// from env, --all-tenants finds the static-mode tenants (acme has a row,
// quiet is only registered), and --drop-old leaves them live.
func TestRebuild_AllTenantsStaticWithPasswords(t *testing.T) {
	ctx := context.Background()
	const redisPassword = "rebuild-redis-password"
	redisAddr := fabriqtest.StartRedisWithPassword(t, redisPassword)
	superDSN := fabriqtest.StartPostgres(t)
	falkorAddr := fabriqtest.StartFalkorDB(t)
	migrateUp(t, superDSN)
	fabriqtest.ApplyDDL(t, superDSN, []string{`INSERT INTO fabriq_tenant (id) VALUES ('acme'), ('quiet')`})

	// Define a persisted graph-projected type and write one acme row. This
	// opens as the owner because DefineDynamic runs CREATE TABLE.
	cfg := fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: superDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr, Password: redisPassword},
		Dynamic:  fabriq.DynamicConfig{Persist: true},
	}
	f, _, err := fabriq.Open(ctx, registry.New(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := f.DefineDynamic(ctx, registry.EntitySpec{
		Name:      "orders",
		Kind:      registry.KindAggregate,
		GraphNode: "Order",
		Schema: &registry.DynamicSchema{
			Table:   "ds_orders",
			Columns: []registry.DynamicColumn{{Name: "sku", Type: registry.ColText}},
		},
	}); err != nil {
		_ = f.Close()
		t.Fatalf("DefineDynamic: %v", err)
	}
	actx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Exec(actx, command.Command{
		Entity: "orders", Op: command.OpCreate, Payload: map[string]any{"sku": "A1"},
	}); err != nil {
		_ = f.Close()
		t.Fatalf("Exec(orders): %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	clearStoreEnv(t)
	t.Setenv("FABRIQ_REDIS_ADDR", redisAddr)
	t.Setenv("FABRIQ_FALKORDB_ADDR", falkorAddr)
	t.Setenv("FABRIQ_DOMAIN_PACK", "none")

	// A wrong Redis password is a runtime failure that never echoes it.
	const wrong = "not-the-redis-password"
	t.Setenv("FABRIQ_REDIS_PASSWORD", wrong)
	code, _, err := runRebuild(t, "--dsn", superDSN, "--projection", "graph", "--all-tenants", "--drop-old")
	if code != 1 {
		t.Fatalf("wrong password: exit code = %d (err %v), want 1", code, err)
	}
	if strings.Contains(err.Error(), wrong) {
		t.Fatalf("wrong password: err = %q, must not contain the password",
			strings.ReplaceAll(err.Error(), wrong, "[REDACTED]"))
	}

	t.Setenv("FABRIQ_REDIS_PASSWORD", redisPassword)
	code, out, err := runRebuild(t, "--dsn", superDSN, "--projection", "graph", "--all-tenants", "--drop-old")
	if code != 0 {
		t.Fatalf("exit code = %d (err %v), want 0; output:\n%s", code, err, out)
	}
	for _, want := range []string{"tenant acme: dropped", "tenant quiet: dropped", "status=live"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output lacks %q:\n%s", want, out)
		}
	}

	// The rebuilt live graph holds acme's one order.
	cfg.FalkorDB = fabriq.FalkorDBConfig{Addr: falkorAddr}
	g, _, err := fabriq.Open(ctx, registry.New(), cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	var nodes []map[string]any
	if err := g.Graph().Query(actx, `MATCH (n:Order) RETURN n.id AS id`, nil, &nodes); err != nil {
		t.Fatalf("graph query: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("Order nodes = %v, want exactly one", nodes)
	}
}

// TestRebuild_AllTenantsNone pins the empty case: a migrated database with no
// tenants prints "no tenants to rebuild" and exits 0.
func TestRebuild_AllTenantsNone(t *testing.T) {
	superDSN := fabriqtest.StartPostgres(t)
	falkorAddr := fabriqtest.StartFalkorDB(t)
	migrateUp(t, superDSN)

	clearStoreEnv(t)
	t.Setenv("FABRIQ_DOMAIN_PACK", "none")
	code, out, err := runRebuild(t, "--dsn", superDSN, "--falkordb", falkorAddr, "--projection", "graph", "--all-tenants")
	if code != 0 {
		t.Fatalf("exit code = %d (err %v), want 0; output:\n%s", code, err, out)
	}
	if !strings.Contains(out, "no tenants to rebuild") {
		t.Fatalf("output lacks %q:\n%s", "no tenants to rebuild", out)
	}
}

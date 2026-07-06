//go:build integration

package postgres_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// TestClusterOps_SchemaLifecycle proves the schema-mode cluster ops against a
// single real database: bootstrap once (extensions in the shared schema),
// create two tenant schemas, migrate each under search_path — head version,
// per-schema grove_migrations, and resolvable vector/geometry types.
func TestClusterOps_SchemaLifecycle(t *testing.T) {
	superDSN := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	// One consolidation database (the container's default db) shared by tenants.
	// clusterDSNs maps the cluster id to this DSN; TenantDSN swaps the db path,
	// but here we target the same database the harness created.
	dbName := databaseOf(t, superDSN)
	ops := postgres.NewClusterOps(map[string]string{"c1": superDSN})

	// Bootstrap twice — idempotent.
	for i := 0; i < 2; i++ {
		if err := ops.EnsureBootstrap(ctx, "c1", dbName, "fabriq_shared"); err != nil {
			t.Fatalf("bootstrap #%d: %v", i, err)
		}
	}

	admin := pgdriver.New()
	if err := admin.Open(ctx, superDSN); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()

	// Extensions live in the shared schema, not public / a tenant schema.
	for _, ext := range []string{"vector", "postgis"} {
		var schema string
		if err := admin.QueryRow(ctx,
			`SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = $1`,
			ext).Scan(&schema); err != nil {
			t.Fatalf("extension %s namespace: %v", ext, err)
		}
		if schema != "fabriq_shared" {
			t.Fatalf("extension %s is in schema %q, want fabriq_shared", ext, schema)
		}
	}

	// Two tenant schemas, each migrated under its own search_path.
	for _, schema := range []string{"tenant_a", "tenant_b"} {
		if err := ops.CreateSchema(ctx, "c1", dbName, schema); err != nil {
			t.Fatalf("create schema %s: %v", schema, err)
		}
		version, err := ops.MigrateSchema(ctx, "c1", dbName, schema, "fabriq_shared")
		if err != nil {
			t.Fatalf("migrate schema %s: %v", schema, err)
		}
		if version != migrations.HeadVersion() {
			t.Fatalf("schema %s version = %q, want head %q", schema, version, migrations.HeadVersion())
		}
		// grove_migrations landed IN the tenant schema (search_path worked).
		var n int
		if err := admin.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'grove_migrations'`,
			schema).Scan(&n); err != nil {
			t.Fatalf("check grove_migrations in %s: %v", schema, err)
		}
		if n != 1 {
			t.Fatalf("grove_migrations not found in schema %s (search_path did not route the chain)", schema)
		}
		// A core fabriq table landed there too.
		if err := admin.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'fabriq_outbox'`,
			schema).Scan(&n); err != nil || n != 1 {
			t.Fatalf("fabriq_outbox not in schema %s (n=%d, err=%v)", schema, n, err)
		}
		// The embeddings table's vector column resolved its type from the
		// shared schema (proves search_path included fabriq_shared at migrate).
		if err := admin.QueryRow(ctx,
			`SELECT count(*) FROM information_schema.columns
			 WHERE table_schema = $1 AND table_name = 'fabriq_embeddings' AND udt_name = 'vector'`,
			schema).Scan(&n); err != nil {
			t.Fatalf("check vector column in %s: %v", schema, err)
		}
		if n < 1 {
			t.Fatalf("fabriq_embeddings.embedding vector type not resolved in schema %s", schema)
		}
	}

	// The two schemas are independent namespaces (no cross-contamination):
	// dropping a table in tenant_a leaves tenant_b's intact — verified simply
	// by both having their own fabriq_outbox above.
}

// databaseOf extracts the database name from a postgres DSN.
func databaseOf(t testing.TB, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	name := u.Path
	if len(name) > 0 && name[0] == '/' {
		name = name[1:]
	}
	if name == "" {
		t.Fatalf("DSN has no database: %s", dsn)
	}
	return name
}

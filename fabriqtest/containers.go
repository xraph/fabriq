package fabriqtest

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xraph/grove/drivers/pgdriver"
)

// Container images for the integration harness. timescaledb-ha bundles
// TimescaleDB and pgvector, matching the production datastore contract.
const (
	PostgresImage = "timescale/timescaledb-ha:pg16-all"
	RedisImage    = "redis:7-alpine"
)

// StartPostgres launches a Postgres+Timescale+pgvector container and
// returns its DSN. The container terminates with the test.
func StartPostgres(t testing.TB) string {
	t.Helper()
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("fabriq"),
		tcpostgres.WithUsername("fabriq"),
		tcpostgres.WithPassword("fabriq"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		t.Fatalf("fabriqtest: start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(pg); err != nil {
			t.Logf("fabriqtest: terminate postgres: %v", err)
		}
	})

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("fabriqtest: postgres connection string: %v", err)
	}
	return dsn
}

// CreateAppRole provisions a NON-superuser application role on a database
// previously started with StartPostgres and returns a DSN for it.
//
// This mirrors production: migrations run as the schema owner; the
// application connects as a restricted role so RLS actually applies
// (Postgres row security NEVER constrains superusers). Call it AFTER
// running migrations.
func CreateAppRole(t testing.TB, superDSN string) string {
	t.Helper()
	ctx := context.Background()

	db := pgdriver.New()
	if err := db.Open(ctx, superDSN); err != nil {
		t.Fatalf("fabriqtest: open superuser conn: %v", err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`DO $$ BEGIN
			IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'fabriq_app') THEN
				CREATE ROLE fabriq_app LOGIN PASSWORD 'fabriq_app' NOSUPERUSER NOBYPASSRLS;
			END IF;
		END $$`,
		`GRANT USAGE ON SCHEMA public TO fabriq_app`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fabriq_app`,
		`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fabriq_app`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fabriq_app`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO fabriq_app`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("fabriqtest: provision app role: %v\n%s", err, s)
		}
	}

	u, err := url.Parse(superDSN)
	if err != nil {
		t.Fatalf("fabriqtest: parse DSN: %v", err)
	}
	u.User = url.UserPassword("fabriq_app", "fabriq_app")
	return u.String()
}

// StartRedis launches a Redis container and returns its address
// (host:port). The container terminates with the test.
func StartRedis(t testing.TB) string {
	t.Helper()
	ctx := context.Background()

	r, err := tcredis.Run(ctx, RedisImage)
	if err != nil {
		t.Fatalf("fabriqtest: start redis container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(r); err != nil {
			t.Logf("fabriqtest: terminate redis: %v", err)
		}
	})

	ep, err := r.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("fabriqtest: redis endpoint: %v", err)
	}
	return ep
}

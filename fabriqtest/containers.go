package fabriqtest

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xraph/grove/drivers/pgdriver"
)

// Container images for the integration harness. timescaledb-ha bundles
// TimescaleDB and pgvector, matching the production datastore contract.
const (
	PostgresImage = "timescale/timescaledb-ha:pg16-all"
	// PostgresPlainImage is the vanilla upstream Postgres image used for the
	// primary+standby replication harness (StartPrimaryStandby). It backs the
	// catalog control database — a single plain table with no pgvector/timescale
	// needs — and, unlike timescaledb-ha, has no HA entrypoint that fights
	// pg_basebackup-based standby setup.
	PostgresPlainImage = "postgres:16-alpine"
	RedisImage         = "redis:7-alpine"
	FalkorDBImage      = "falkordb/falkordb:v4.2.2" // pinned: multi-label + SET n:Label
	ElasticsearchImage = "elasticsearch:9.4.1"
	// ClickHouseImage pins a server new enough that lightweight DELETE is GA and
	// enabled by default (>= 23.3).
	ClickHouseImage = "clickhouse/clickhouse-server:24.3"
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
		if termErr := testcontainers.TerminateContainer(pg); termErr != nil {
			t.Logf("fabriqtest: terminate postgres: %v", termErr)
		}
	})

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("fabriqtest: postgres connection string: %v", err)
	}
	return dsn
}

// StartClickHouse launches a ClickHouse container and returns its
// clickhouse:// DSN. The container terminates with the test.
func StartClickHouse(t testing.TB) string {
	t.Helper()
	ctx := context.Background()

	ch, err := tcclickhouse.Run(ctx, ClickHouseImage,
		tcclickhouse.WithUsername("fabriq"),
		tcclickhouse.WithPassword("fabriq"),
		tcclickhouse.WithDatabase("fabriq"),
		// ClickHouse logs "Ready for connections" to its internal server log
		// file, NOT container stdout (the entrypoint script's own stdout lines
		// stop at "create database" — verified against 24.3), so wait.ForLog
		// never matches. Poll the HTTP interface instead, mirroring the
		// module's own default wait strategy.
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/").WithPort("8123/tcp").
				WithStatusCodeMatcher(func(status int) bool { return status == 200 }).
				WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		t.Fatalf("fabriqtest: start clickhouse container: %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(ch); termErr != nil {
			t.Logf("fabriqtest: terminate clickhouse: %v", termErr)
		}
	})

	dsn, err := ch.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("fabriqtest: clickhouse connection string: %v", err)
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

// ApplyDDL executes the given statements against dsn as the connecting role
// (typically the superuser/owner DSN). It is the seam tests use to create
// application-defined materialization targets (e.g. domain.PagesDDL()) that are
// no longer part of fabriq's shipped migration chain. Apply it BEFORE
// CreateAppRole so the app role inherits grants on the new tables.
func ApplyDDL(t testing.TB, dsn string, stmts []string) {
	t.Helper()
	ctx := context.Background()

	db := pgdriver.New()
	if err := db.Open(ctx, dsn); err != nil {
		t.Fatalf("fabriqtest: open conn for DDL: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("fabriqtest: apply DDL: %v\n%s", err, s)
		}
	}
}

// StartFalkorDB launches a FalkorDB container and returns its address
// (host:port). The container terminates with the test.
func StartFalkorDB(t testing.TB) string {
	t.Helper()
	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        FalkorDBImage,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("fabriqtest: start falkordb container: %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(c); termErr != nil {
			t.Logf("fabriqtest: terminate falkordb: %v", termErr)
		}
	})

	ep, err := c.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("fabriqtest: falkordb endpoint: %v", err)
	}
	return ep
}

// StartElasticsearch launches a single-node Elasticsearch container
// (security off, HTTP only) and returns its base URL. The container
// terminates with the test.
func StartElasticsearch(t testing.TB) string {
	t.Helper()
	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        ElasticsearchImage,
			ExposedPorts: []string{"9200/tcp"},
			Env: map[string]string{
				"discovery.type":         "single-node",
				"xpack.security.enabled": "false",
				"ES_JAVA_OPTS":           "-Xms512m -Xmx512m",
			},
			// Wait for real write-readiness, not just a live HTTP port: the
			// cluster-health endpoint with wait_for_status=yellow blocks until
			// the node has formed a cluster and its shards can accept writes.
			// Plain "/" answers as soon as the HTTP layer is up — before the
			// cluster is ready — which on a loaded host surfaces later as bulk
			// writes rejected with 429/503 ("unavailable").
			WaitingFor: wait.ForHTTP("/_cluster/health?wait_for_status=yellow&timeout=60s").
				WithPort("9200/tcp").
				WithStatusCodeMatcher(func(status int) bool { return status == 200 }).
				WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("fabriqtest: start elasticsearch container: %v", err)
	}
	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(c); termErr != nil {
			t.Logf("fabriqtest: terminate elasticsearch: %v", termErr)
		}
	})

	ep, err := c.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("fabriqtest: elasticsearch endpoint: %v", err)
	}
	return "http://" + ep
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
		if termErr := testcontainers.TerminateContainer(r); termErr != nil {
			t.Logf("fabriqtest: terminate redis: %v", termErr)
		}
	})

	ep, err := r.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("fabriqtest: redis endpoint: %v", err)
	}
	return ep
}

// QueryStrings runs a single-column query against dsn and returns the rows
// as strings — the seam integration tests use for schema-shape assertions
// (information_schema / pg_catalog checks).
func QueryStrings(t testing.TB, dsn, sql string, args ...any) []string {
	t.Helper()
	ctx := context.Background()

	db := pgdriver.New()
	if err := db.Open(ctx, dsn); err != nil {
		t.Fatalf("fabriqtest: open conn for query: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		t.Fatalf("fabriqtest: query: %v\n%s", err, sql)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("fabriqtest: scan: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("fabriqtest: rows: %v", err)
	}
	return out
}

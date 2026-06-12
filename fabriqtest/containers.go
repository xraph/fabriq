package fabriqtest

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
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

//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/livequery"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func openFsLiveTest(t *testing.T) (*fabriq.Fabriq, *fabriq.Stores) {
	t.Helper()
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
		Storage: fabriq.StorageConfig{
			StorageDriver: "file://" + t.TempDir(),
			DefaultBucket: "primary",
			EnableCas:     true,
		},
		Subscriptions: fabriq.SubscriptionsConfig{
			ConflationWindow: 30 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("fabriq.Open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	// Start the leader-elected relay: outbox -> Redis feed, exactly as
	// fabriq-worker runs it. Without this, no live deltas arrive.
	relay := postgres.NewRelay(stores.Postgres, reg, stores.Redis,
		postgres.WithRelayPollInterval(100*time.Millisecond))
	elector := postgres.NewElector(stores.Postgres, 1002, postgres.WithElectorRetry(100*time.Millisecond))
	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	go func() { _ = elector.Run(runCtx, relay.Run) }()

	return f, stores
}

func TestFsWatchChildren(t *testing.T) {
	ctx := context.Background()
	f, _ := openFsLiveTest(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	dir, err := f.CreateFolder(tctx, "", "live")
	if err != nil {
		t.Fatalf("CreateFolder root: %v", err)
	}
	if _, err := f.CreateFolder(tctx, dir.ID, "first"); err != nil {
		t.Fatalf("CreateFolder first: %v", err)
	}

	snap, deltas, handle, err := f.WatchChildren(tctx, dir.ID, 100)
	if err != nil {
		t.Fatalf("WatchChildren: %v", err)
	}
	defer handle.Close()
	if len(snap.Rows) != 1 {
		t.Fatalf("initial snapshot rows = %d, want 1", len(snap.Rows))
	}

	// A new child arrives → OpEnter delta.
	if _, err := f.CreateFile(tctx, dir.ID, "second.txt", bytes.NewReader([]byte("x")), fabriq.CreateFileOpts{}); err != nil {
		t.Fatalf("CreateFile second.txt: %v", err)
	}

	select {
	case d := <-deltas:
		if d.Op != livequery.OpEnter {
			t.Fatalf("delta op = %v, want OpEnter", d.Op)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no live delta for new child within 10s")
	}
}

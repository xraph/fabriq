//go:build integration

package fabriq_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// e2e boots the full phase-3 plane: Postgres + Redis containers, migrated
// schema, app-role connection, Open()'d facade, and a leader-elected
// relay — the same wiring fabriq-worker and api-example use.
func e2e(t *testing.T) (*fabriq.Fabriq, *fabriq.Stores, *registry.Registry) {
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
		Subscriptions: fabriq.SubscriptionsConfig{
			ConflationWindow: 30 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	// Leader-elected relay, exactly as fabriq-worker runs it.
	relay := postgres.NewRelay(stores.Postgres, reg, stores.Redis,
		postgres.WithRelayPollInterval(100*time.Millisecond))
	elector := postgres.NewElector(stores.Postgres, 1001, postgres.WithElectorRetry(100*time.Millisecond))
	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	go func() { _ = elector.Run(runCtx, relay.Run) }()

	return f, stores, reg
}

func TestE2E_CommandToSubscriptionDelta(t *testing.T) {
	f, _, _ := e2e(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Fetch-then-subscribe: subscribe to the tenant scope first...
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	deltas, err := f.Subscribe(subCtx, query.SubscribeScope{Entity: "asset", Scope: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // pump attach

	// ...then write through the command plane.
	site, err := f.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "Plant"}})
	if err != nil {
		t.Fatal(err)
	}
	asset, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Pump 7", Kind: "pump", SiteID: site.AggID}})
	if err != nil {
		t.Fatal(err)
	}

	// The delta arrives end-to-end: command -> outbox -> relay -> redis
	// stream -> hub pump -> conflation -> subscriber. The tenant channel
	// carries every entity's events, so the site.created may arrive first.
	assetSeen := false
	firstDeadline := time.After(10 * time.Second)
	for !assetSeen {
		select {
		case d := <-deltas:
			if d.Aggregate != "asset" {
				continue
			}
			if d.AggID != asset.AggID || d.Version != 1 || d.Type != "asset.created" {
				t.Fatalf("asset delta = %+v", d)
			}
			if d.StreamID == "" {
				t.Fatal("delta must carry the stream id for Last-Event-ID resume")
			}
			assetSeen = true
		case <-firstDeadline:
			t.Fatal("no asset delta received end-to-end")
		}
	}

	// Conflation: a burst of updates to one aggregate within the window
	// collapses; the last version survives.
	for v := 0; v < 5; v++ {
		if _, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpUpdate, AggID: asset.AggID,
			Payload: &domain.Asset{Name: "Pump 7", Kind: "pump", SiteID: site.AggID}}); err != nil {
			t.Fatal(err)
		}
	}
	var last query.Delta
	gotAny := false
	timeout := time.After(10 * time.Second)
drain:
	for {
		select {
		case d := <-deltas:
			gotAny = true
			last = d
			if d.Version == 6 {
				break drain
			}
		case <-timeout:
			break drain
		}
	}
	if !gotAny || last.Version != 6 {
		t.Fatalf("conflated stream never reached v6: last=%+v", last)
	}
}

func TestE2E_CatchUpAfterDisconnect(t *testing.T) {
	f, _, _ := e2e(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	site, err := f.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "P"}})
	if err != nil {
		t.Fatal(err)
	}

	scope := query.SubscribeScope{Entity: "site", Scope: "id", ID: site.AggID}

	// Wait until the relay has published, then read the channel from the
	// beginning — the "client missed everything" case.
	var missed []query.Delta
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		missed, err = f.CatchUp(ctx, scope, "0", 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(missed) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(missed) != 1 || missed[0].Version != 1 {
		t.Fatalf("catch-up = %+v", missed)
	}

	// Update while "disconnected"; resuming from the last seen id returns
	// exactly the missed delta.
	if _, err := f.Exec(ctx, command.Command{Entity: "site", Op: command.OpUpdate, AggID: site.AggID,
		Payload: &domain.Site{Name: "P2"}}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resumed, err := f.CatchUp(ctx, scope, missed[0].StreamID, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(resumed) == 1 && resumed[0].Version == 2 {
			return // exactly the missed update, nothing replayed
		}
		if len(resumed) > 1 {
			t.Fatalf("resume replayed too much: %+v", resumed)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("resume never saw the missed update")
}

func TestE2E_CrossTenantSubscriptionSeesNothing(t *testing.T) {
	f, _, _ := e2e(t)
	acme, _ := tenant.WithTenant(context.Background(), "acme")
	rival, _ := tenant.WithTenant(context.Background(), "rival")

	subCtx, cancel := context.WithCancel(rival)
	defer cancel()
	rivalDeltas, err := f.Subscribe(subCtx, query.SubscribeScope{Entity: "asset", Scope: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	if _, err := f.Exec(acme, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Secret Pump"}}); err != nil {
		t.Fatal(err)
	}

	select {
	case d := <-rivalDeltas:
		t.Fatalf("rival tenant received acme's delta: %+v", d)
	case <-time.After(3 * time.Second):
		// silence is correct: channels are tenant-prefixed by derivation
	}
}

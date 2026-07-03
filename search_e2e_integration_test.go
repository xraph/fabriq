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

// TestE2E_SearchProjection: command -> outbox -> relay -> stream -> search
// engine -> Elasticsearch -> Search() finds it; then a blue-green search
// rebuild swaps aliases and serving continues.
func TestE2E_SearchProjection(t *testing.T) {
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	esURL := fabriqtest.StartElasticsearch(t)

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

	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres:      fabriq.PostgresConfig{DSN: appDSN},
		Redis:         fabriq.RedisConfig{Addr: redisAddr},
		Elasticsearch: fabriq.ElasticsearchConfig{Addrs: []string{esURL}},
		Projections:   fabriq.ProjectionsConfig{Search: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)
	relay := postgres.NewRelay(stores.Postgres, reg, stores.Redis, postgres.WithRelayPollInterval(100*time.Millisecond))
	elector := postgres.NewElector(stores.Postgres, 1001, postgres.WithElectorRetry(100*time.Millisecond))
	go func() { _ = elector.Run(runCtx, relay.Run) }()
	engine, err := stores.SearchEngine(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = engine.Run(runCtx, "search-e2e") }()

	tctx, _ := tenant.WithTenant(ctx, "acme")

	asset, err := f.Exec(tctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Centrifugal Pump 7", Kind: "pump", Serial: "SN-777"}})
	if err != nil {
		t.Fatal(err)
	}

	waitCtx, cancel := context.WithTimeout(tctx, 60*time.Second)
	defer cancel()
	if err := f.WaitForProjection(waitCtx, "search", "asset", asset.AggID, 1); err != nil {
		t.Fatalf("WaitForProjection(search): %v", err)
	}

	var hits []map[string]any
	if err := f.Search().Search(tctx, query.SearchQuery{Entity: "asset", Query: "centrifugal"}, &hits); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0]["id"] != asset.AggID || hits[0]["serial"] != "SN-777" {
		t.Fatalf("hits = %v", hits)
	}

	// Cross-tenant search sees nothing.
	rival, _ := tenant.WithTenant(ctx, "rival")
	var rivalHits []map[string]any
	if err := f.Search().Search(rival, query.SearchQuery{Entity: "asset", Query: "centrifugal"}, &rivalHits); err != nil {
		t.Fatal(err)
	}
	if len(rivalHits) != 0 {
		t.Fatalf("tenant leak: %v", rivalHits)
	}

	// Blue-green search rebuild: replay from Postgres, alias swap, serve on.
	rebuilder, err := stores.SearchRebuilder(reg)
	if err != nil {
		t.Fatal(err)
	}
	oldTarget, newTarget, err := rebuilder.Rebuild(ctx, "acme")
	if err != nil {
		t.Fatalf("search rebuild: %v", err)
	}
	if newTarget != "v2" {
		t.Fatalf("new search target = %q", newTarget)
	}

	hits = nil
	if err := f.Search().Search(tctx, query.SearchQuery{Entity: "asset", Query: "pump"}, &hits); err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("post-rebuild search = %v", hits)
	}

	if oldTarget == "" {
		oldTarget = "v1"
	}
	if err := rebuilder.Finalize(ctx, "acme", oldTarget); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	hits = nil
	if err := f.Search().Search(tctx, query.SearchQuery{Entity: "asset", Query: "pump"}, &hits); err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 {
		t.Fatalf("search broke after finalize: %v", hits)
	}
}

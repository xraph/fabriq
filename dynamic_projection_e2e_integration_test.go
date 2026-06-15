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

// widgetSpec is the dynamic entity under test. Only scalar columns so the
// built-in GraphApplier can serialise every prop as a Cypher literal.
var widgetSpec = registry.EntitySpec{
	Name: "widget",
	Kind: registry.KindAggregate,
	Schema: &registry.DynamicSchema{
		Table: "ds_widgets",
		Columns: []registry.DynamicColumn{
			{Name: "name", Type: registry.ColText, NotNull: true},
			{Name: "kind", Type: registry.ColText},
		},
	},
	GraphNode: "Widget",
	Search:    registry.SearchSpec{Index: "widgets", Fields: []string{"name", "kind"}},
	Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
}

// ensureWidgets creates the dynamic ds_widgets table as the schema owner
// (superuser), mirroring how Phase 3/4 integration tests call EnsureDynamic
// before CreateAppRole so DEFAULT PRIVILEGES cover the new table.
func ensureWidgets(t *testing.T, superDSN string, reg *registry.Registry) {
	t.Helper()
	ctx := context.Background()

	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	defer func() { _ = owner.Close() }()

	ent, ok := reg.Get("widget")
	if !ok {
		t.Fatal("entity 'widget' not found in registry")
	}
	if err := owner.EnsureDynamic(ctx, ent); err != nil {
		t.Fatalf("EnsureDynamic(widget): %v", err)
	}
}

// TestDynamicProjection_Graph proves that a DYNAMIC entity whose spec carries
// GraphNode projects to FalkorDB through the full
// command → outbox → relay → projection-engine pipeline, identically to a
// static entity.
func TestDynamicProjection_Graph(t *testing.T) {
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	falkorAddr := fabriqtest.StartFalkorDB(t)

	// Build a registry with the domain pack (needed for migrations) plus the
	// test dynamic entity.
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	reg.MustRegister(widgetSpec)

	// Migrate as schema owner.
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()

	// Create ds_widgets BEFORE CreateAppRole so DEFAULT PRIVILEGES apply.
	ensureWidgets(t, superDSN, reg)

	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres:      fabriq.PostgresConfig{DSN: appDSN},
		Redis:         fabriq.RedisConfig{Addr: redisAddr},
		FalkorDB:      fabriq.FalkorDBConfig{Addr: falkorAddr},
		Projections:   fabriq.ProjectionsConfig{Graph: true},
		Subscriptions: fabriq.SubscriptionsConfig{ConflationWindow: 30 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)

	relay := postgres.NewRelay(stores.Postgres, reg, stores.Redis,
		postgres.WithRelayPollInterval(100*time.Millisecond))
	elector := postgres.NewElector(stores.Postgres, 1001,
		postgres.WithElectorRetry(100*time.Millisecond))
	go func() { _ = elector.Run(runCtx, relay.Run) }()

	engine, err := stores.GraphEngine(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = engine.Run(runCtx, "graph-dynamic-e2e") }()

	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Write a dynamic widget through the command plane.
	// Use OpCreate (auto-generates AggID); OpUpsert requires an explicit AggID.
	res, err := f.Exec(tctx, command.Command{
		Entity:  "widget",
		Op:      command.OpCreate,
		Payload: map[string]any{"name": "Sprocket", "kind": "gear"},
	})
	if err != nil {
		t.Fatalf("Exec(widget): %v", err)
	}
	widgetID := res.AggID

	// Wait for the graph projection to catch up (version 1 of the widget).
	waitCtx, cancel := context.WithTimeout(tctx, 30*time.Second)
	defer cancel()
	if err := f.WaitForProjection(waitCtx, "graph", "widget", widgetID, 1); err != nil {
		t.Fatalf("WaitForProjection(graph, widget): %v", err)
	}

	// Assert the node landed in FalkorDB with the correct name.
	var names []string
	if err := f.Graph().Query(tctx,
		`MATCH (n:Widget {id: $id}) RETURN n.name`,
		map[string]any{"id": widgetID}, &names); err != nil {
		t.Fatalf("graph query (name): %v", err)
	}
	if len(names) != 1 || names[0] != "Sprocket" {
		t.Fatalf("Widget name in graph = %v, want [Sprocket]", names)
	}

	// Assert both props are present on the node.
	var rows []map[string]any
	if err := f.Graph().Query(tctx,
		`MATCH (n:Widget {id: $id}) RETURN n.name AS name, n.kind AS kind`,
		map[string]any{"id": widgetID}, &rows); err != nil {
		t.Fatalf("graph query (props): %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 Widget node, got %d", len(rows))
	}
	if rows[0]["name"] != "Sprocket" {
		t.Errorf("node.name = %v, want Sprocket", rows[0]["name"])
	}
	if rows[0]["kind"] != "gear" {
		t.Errorf("node.kind = %v, want gear", rows[0]["kind"])
	}
}

// TestDynamicProjection_Search proves that the SAME dynamic entity projects
// to Elasticsearch through the same pipeline when Projections.Search is set.
func TestDynamicProjection_Search(t *testing.T) {
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	esURL := fabriqtest.StartElasticsearch(t)

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	reg.MustRegister(widgetSpec)

	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = closeFn()

	ensureWidgets(t, superDSN, reg)

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

	relay := postgres.NewRelay(stores.Postgres, reg, stores.Redis,
		postgres.WithRelayPollInterval(100*time.Millisecond))
	elector := postgres.NewElector(stores.Postgres, 1001,
		postgres.WithElectorRetry(100*time.Millisecond))
	go func() { _ = elector.Run(runCtx, relay.Run) }()

	engine, err := stores.SearchEngine(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = engine.Run(runCtx, "search-dynamic-e2e") }()

	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}

	res, err := f.Exec(tctx, command.Command{
		Entity:  "widget",
		Op:      command.OpCreate,
		Payload: map[string]any{"name": "Sprocket", "kind": "gear"},
	})
	if err != nil {
		t.Fatalf("Exec(widget): %v", err)
	}
	widgetID := res.AggID

	waitCtx, cancel := context.WithTimeout(tctx, 60*time.Second)
	defer cancel()
	if err := f.WaitForProjection(waitCtx, "search", "widget", widgetID, 1); err != nil {
		t.Fatalf("WaitForProjection(search, widget): %v", err)
	}

	var hits []map[string]any
	if err := f.Search().Search(tctx, query.SearchQuery{Entity: "widget", Query: "Sprocket"}, &hits); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("expected 1 search hit, got %d: %v", len(hits), hits)
	}
	if hits[0]["id"] != widgetID {
		t.Errorf("hit id = %v, want %v", hits[0]["id"], widgetID)
	}
	if hits[0]["name"] != "Sprocket" {
		t.Errorf("hit name = %v, want Sprocket", hits[0]["name"])
	}
}

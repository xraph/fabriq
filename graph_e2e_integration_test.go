//go:build integration

package fabriq_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// graphE2E boots the COMPLETE phase-4 plane: Postgres + Redis + FalkorDB,
// migrated schema, app role, facade, leader-elected relay AND the graph
// projection engine — the full fabriq-worker wiring.
func graphE2E(t *testing.T) (*fabriq.Fabriq, *fabriq.Stores, *registry.Registry) {
	t.Helper()
	ctx := context.Background()

	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	falkorAddr := fabriqtest.StartFalkorDB(t)

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

	relay := postgres.NewRelay(stores.Postgres, reg, stores.Redis, postgres.WithRelayPollInterval(100*time.Millisecond))
	elector := postgres.NewElector(stores.Postgres, 1001, postgres.WithElectorRetry(100*time.Millisecond))
	go func() { _ = elector.Run(runCtx, relay.Run) }()

	engine, err := stores.GraphEngine(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = engine.Run(runCtx, "graph-e2e") }()

	return f, stores, reg
}

func TestE2E_GraphProjection(t *testing.T) {
	f, _, _ := graphE2E(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	site, err := f.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate,
		Payload: &domain.Site{Name: "Plant A", Code: "PA"}})
	if err != nil {
		t.Fatal(err)
	}
	pump, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Pump 7", Kind: "pump", SiteID: site.AggID}})
	if err != nil {
		t.Fatal(err)
	}
	valve, err := f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
		Payload: &domain.Asset{Name: "Valve 2", Kind: "valve", SiteID: site.AggID, ParentID: pump.AggID}})
	if err != nil {
		t.Fatal(err)
	}

	// Read-your-writes through the projection plane.
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := f.WaitForProjection(waitCtx, "graph", "asset", valve.AggID, 1); err != nil {
		t.Fatalf("WaitForProjection: %v", err)
	}

	// Raw openCypher through the live graph.
	var names []string
	err = f.Graph().Query(ctx,
		`MATCH (a:Asset)-[:LOCATED_AT]->(s:Site {id: $site}) RETURN a.name ORDER BY a.name`,
		map[string]any{"site": site.AggID}, &names)
	if err != nil {
		t.Fatalf("Graph query: %v", err)
	}
	if len(names) != 2 || names[0] != "Pump 7" || names[1] != "Valve 2" {
		t.Fatalf("graph traversal = %v", names)
	}

	// The composed op: traverse in the graph, hydrate from Postgres in ONE
	// batched query.
	var assets []*domain.Asset
	err = f.Graph().TraverseAndHydrate(ctx,
		`MATCH (a:Asset)-[:LOCATED_AT]->(:Site {id: $site}) RETURN a.id ORDER BY a.id`,
		map[string]any{"site": site.AggID}, &assets)
	if err != nil {
		t.Fatalf("TraverseAndHydrate: %v", err)
	}
	if len(assets) != 2 {
		t.Fatalf("hydrated %d assets, want 2", len(assets))
	}
	for _, a := range assets {
		if a.TenantID != "acme" || a.SiteID != site.AggID {
			t.Fatalf("hydrated row wrong: %+v", a)
		}
	}

	// Hierarchy edge too.
	var children []string
	err = f.Graph().Query(ctx,
		`MATCH (c:Asset)-[:CHILD_OF]->(p:Asset {id: $p}) RETURN c.id`,
		map[string]any{"p": pump.AggID}, &children)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0] != valve.AggID {
		t.Fatalf("CHILD_OF = %v", children)
	}
}

// TestE2E_RebuildProducesIdenticalGraph is the spec's rebuild guarantee:
// blue-green rebuild from Postgres yields a graph identical to the one
// built event-by-event, and readers follow the flipped pointer.
func TestE2E_RebuildProducesIdenticalGraph(t *testing.T) {
	f, stores, reg := graphE2E(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	// Build a small but edge-rich world through the command plane.
	site, _ := f.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "S"}})
	var lastAsset command.Result
	parent := ""
	for i := 0; i < 5; i++ {
		lastAsset, err = f.Exec(ctx, command.Command{Entity: "asset", Op: command.OpCreate,
			Payload: &domain.Asset{Name: fmt.Sprintf("A%d", i), SiteID: site.AggID, ParentID: parent}})
		if err != nil {
			t.Fatal(err)
		}
		parent = lastAsset.AggID
	}
	tag, err := f.Exec(ctx, command.Command{Entity: "tag", Op: command.OpCreate,
		Payload: &domain.Tag{Name: "temp", Unit: "C", AssetID: lastAsset.AggID}})
	if err != nil {
		t.Fatal(err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := f.WaitForProjection(waitCtx, "graph", "tag", tag.AggID, 1); err != nil {
		t.Fatal(err)
	}

	liveDump := dumpGraph(t, f, ctx)

	// Blue-green rebuild from Postgres.
	rebuilder, err := stores.GraphRebuilder(reg)
	if err != nil {
		t.Fatal(err)
	}
	oldTarget, newTarget, err := rebuilder.Rebuild(context.Background(), "acme")
	if err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if newTarget != "tenant_acme_v2" {
		t.Fatalf("new target = %q", newTarget)
	}

	// Readers follow the flipped pointer (resolver TTL is 2s).
	time.Sleep(2500 * time.Millisecond)
	rebuiltDump := dumpGraph(t, f, ctx)
	if liveDump != rebuiltDump {
		t.Fatalf("rebuilt graph differs from event-built graph:\n--- live ---\n%s\n--- rebuilt ---\n%s", liveDump, rebuiltDump)
	}

	// Finalize drops the old (initial unversioned) target.
	if oldTarget == "" {
		oldTarget = "tenant_acme"
	}
	if err := rebuilder.Finalize(context.Background(), "acme", oldTarget); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	// Queries still served after the drop.
	if got := dumpGraph(t, f, ctx); got != rebuiltDump {
		t.Fatal("graph changed after finalize")
	}
}

// dumpGraph renders a canonical text form of the tenant's live graph:
// all nodes (id@version) and all edges (from-rel->to), sorted.
func dumpGraph(t *testing.T, f *fabriq.Fabriq, ctx context.Context) string {
	t.Helper()
	var nodes []map[string]any
	if err := f.Graph().Query(ctx,
		`MATCH (n) RETURN n.id AS id, n.version AS version ORDER BY n.id`, nil, &nodes); err != nil {
		t.Fatalf("dump nodes: %v", err)
	}
	var edges []map[string]any
	if err := f.Graph().Query(ctx,
		`MATCH (a)-[r]->(b) RETURN a.id AS f, type(r) AS rel, b.id AS t`, nil, &edges); err != nil {
		t.Fatalf("dump edges: %v", err)
	}
	lines := make([]string, 0, len(nodes)+len(edges))
	for _, n := range nodes {
		lines = append(lines, fmt.Sprintf("N %v@%v", n["id"], n["version"]))
	}
	for _, e := range edges {
		lines = append(lines, fmt.Sprintf("E %v-%v->%v", e["f"], e["rel"], e["t"]))
	}
	sort.Strings(lines)
	out := ""
	for _, l := range lines {
		out += l + "\n"
	}
	return out
}

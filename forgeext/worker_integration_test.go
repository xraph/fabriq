//go:build integration

package forgeext_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/migrations"
)

// TestWorker_RunsAndProjects brings up the full plane via Extension with
// RunWorker=true, executes a command, and asserts the graph projection applies.
func TestWorker_RunsAndProjects(t *testing.T) {
	ctx := context.Background()

	// Boot infrastructure.
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	falkorAddr := fabriqtest.StartFalkorDB(t)

	// Run migrations as superuser.
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, superDSN)
	if err != nil {
		t.Fatalf("open orchestrator: %v", err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = closeFn()

	// Create restricted app role so RLS applies.
	appDSN := fabriqtest.CreateAppRole(t, superDSN)

	// Build registry with demo domain.
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatalf("domain.RegisterAll: %v", err)
	}

	// Create the extension with worker enabled.
	ext := forgeext.New(reg,
		forgeext.WithConfig(fabriq.Config{
			Postgres: fabriq.PostgresConfig{DSN: appDSN},
			Redis:    fabriq.RedisConfig{Addr: redisAddr},
			FalkorDB: fabriq.FalkorDBConfig{Addr: falkorAddr},
			Projections: fabriq.ProjectionsConfig{
				Graph: true,
			},
			Subscriptions: fabriq.SubscriptionsConfig{
				ConflationWindow: 30 * time.Millisecond,
			},
		}),
		forgeext.WithWorker(true),
	)

	// Build a forge app (do NOT call app.Run() — it blocks on signal).
	app := forge.NewApp(forge.AppConfig{
		Name:        "fabriq-worker-test",
		HTTPAddress: ":0",
	})

	// Drive the lifecycle.
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := ext.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = ext.Shutdown(shutdownCtx)
		_ = ext.Stop(context.Background())
	})

	// Run spawns goroutines and returns immediately.
	if err := ext.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Exercise the facade.
	f := ext.Fabriq()
	if f == nil {
		t.Fatal("ext.Fabriq() returned nil after Start")
	}

	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("tenant.WithTenant: %v", err)
	}

	// Create a site.
	siteRes, err := f.Exec(tctx, command.Command{
		Entity:  "site",
		Op:      command.OpCreate,
		Payload: &domain.Site{Name: "Plant A"},
	})
	if err != nil {
		t.Fatalf("Exec(OpCreate site): %v", err)
	}

	// Create an asset linked to the site.
	assetRes, err := f.Exec(tctx, command.Command{
		Entity:  "asset",
		Op:      command.OpCreate,
		Payload: &domain.Asset{Name: "Pump 7", Kind: "pump", SiteID: siteRes.AggID},
	})
	if err != nil {
		t.Fatalf("Exec(OpCreate asset): %v", err)
	}

	// Wait for the graph projection to catch up.
	waitCtx, cancelWait := context.WithTimeout(tctx, 30*time.Second)
	defer cancelWait()
	if err := f.WaitForProjection(waitCtx, "graph", "asset", assetRes.AggID, 1); err != nil {
		t.Fatalf("WaitForProjection(graph, asset): %v", err)
	}

	// Query the graph: the asset must appear as a node linked to the site.
	var assetNames []string
	if err := f.Graph().Query(tctx,
		`MATCH (a:Asset)-[:LOCATED_AT]->(s:Site {id: $site}) RETURN a.name`,
		map[string]any{"site": siteRes.AggID},
		&assetNames,
	); err != nil {
		t.Fatalf("Graph().Query: %v", err)
	}
	if len(assetNames) != 1 || assetNames[0] != "Pump 7" {
		t.Fatalf("graph query returned %v; want [\"Pump 7\"]", assetNames)
	}
	t.Logf("graph projection confirmed: asset=%q projected under site=%s", assetNames[0], siteRes.AggID)
}

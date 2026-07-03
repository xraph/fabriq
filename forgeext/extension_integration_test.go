//go:build integration

package forgeext_test

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/vessel"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/migrations"
)

func TestExtension_Lifecycle_DI(t *testing.T) {
	ctx := context.Background()

	// Boot infrastructure.
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

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
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)

	// Build registry with demo domain.
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatalf("domain.RegisterAll: %v", err)
	}

	// Build a forge app (do NOT call app.Run() — it blocks on signal).
	app := forge.NewApp(forge.AppConfig{
		Name:        "fabriq-test",
		HTTPAddress: ":0",
	})

	// Create the extension.
	ext := forgeext.New(reg, forgeext.WithConfig(fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
		Subscriptions: fabriq.SubscriptionsConfig{
			ConflationWindow: 30 * time.Millisecond,
		},
	}))

	// Drive the lifecycle directly.
	if err := ext.Register(app); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := ext.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ext.Stop(context.Background()) })

	// Resolve the facade from DI by alias "fabriq".
	f, err := vessel.InjectNamed[*fabriq.Fabriq](app.Container(), "fabriq")
	if err != nil {
		t.Fatalf("InjectNamed[*fabriq.Fabriq](\"fabriq\"): %v", err)
	}
	if f == nil {
		t.Fatal("InjectNamed returned nil facade")
	}

	// The DI-resolved instance must be the same as the one held by the extension.
	if f != ext.Fabriq() {
		t.Fatal("DI-resolved facade differs from ext.Fabriq()")
	}

	// Exercise the facade through DI: create a site in tenant "acme".
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("tenant.WithTenant: %v", err)
	}

	res, err := f.Exec(tctx, command.Command{
		Entity:  "site",
		Op:      command.OpCreate,
		Payload: &domain.Site{Name: "Plant A"},
	})
	if err != nil {
		t.Fatalf("Exec(OpCreate site): %v", err)
	}
	if res.AggID == "" {
		t.Fatal("Exec returned empty AggID")
	}

	var s domain.Site
	if err := f.Relational().Get(tctx, "site", res.AggID, &s); err != nil {
		t.Fatalf("Relational().Get(site): %v", err)
	}
	if s.Name != "Plant A" {
		t.Fatalf("expected Name %q, got %q", "Plant A", s.Name)
	}

	// Health check must pass after Start.
	if err := ext.Health(ctx); err != nil {
		t.Fatalf("Health after Start: %v", err)
	}
}

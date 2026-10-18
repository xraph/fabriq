//go:build integration

package forgeext_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

// A started extension serves two endpoints: fabriq's own series on GET
// /metrics (fabriq_ names) and, on forge's /_/metrics, grove's
// db_client_operation_duration_seconds for the databases fabriq dialed.
func TestExtension_TelemetryEndpoints(t *testing.T) {
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
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

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	app := forge.New(forge.WithAppName("fabriq-telemetry-it"), forge.WithAppLogger(forge.NewNoopLogger()))
	ext := forgeext.New(reg, forgeext.WithConfig(fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
	}))
	if err := ext.Register(app); err != nil {
		t.Fatal(err)
	}
	if err := ext.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ext.Stop(context.Background()) })

	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ext.Fabriq().Exec(tctx, command.Command{Entity: "site", Op: command.OpCreate, Payload: &domain.Site{Name: "Plant A"}}); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	app.Router().ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rr.Body)
	if rr.Code != http.StatusOK || !strings.Contains(string(body), `fabriq_command_duration_seconds_count{op="create",result="ok"} 1`) {
		t.Fatalf("GET /metrics = %d, lacks the command histogram:\n%s", rr.Code, body)
	}

	p, ok := app.Metrics().(interface{ PrometheusHandler() http.Handler })
	if !ok {
		t.Fatalf("app metrics %T have no Prometheus handler", app.Metrics())
	}
	rr = httptest.NewRecorder()
	p.PrometheusHandler().ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_/metrics", nil))
	forgeBody, _ := io.ReadAll(rr.Body)
	found := false
	for _, line := range strings.Split(string(forgeBody), "\n") {
		if strings.HasPrefix(line, "db_client_operation_duration_seconds_count{") && strings.Contains(line, `db_system_name="postgresql"`) {
			found = true
		}
		if strings.HasPrefix(line, "fabriq_") || strings.HasPrefix(line, "forge_fabriq_") {
			t.Fatalf("fabriq's series leaked onto forge's registry: %q", line)
		}
	}
	if !found {
		t.Fatalf("/_/metrics lacks grove's postgresql series:\n%s", forgeBody)
	}
}

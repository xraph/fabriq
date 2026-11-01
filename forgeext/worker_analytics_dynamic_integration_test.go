//go:build integration

package forgeext_test

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/adapters/pganalytics"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/migrations"
)

func TestHostedAnalyticsDynamicLifecycle(t *testing.T) {
	ctx := context.Background()
	sourceDSN := fabriqtest.StartPostgres(t)
	sinkDSN := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)
	orch, closeMigrations, err := migrations.OpenOrchestrator(ctx, sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := closeMigrations(); err != nil {
		t.Fatal(err)
	}
	if err := migrations.EnsureAppRole(ctx, sourceDSN, "analytics_app", "worker-role-secret"); err != nil {
		t.Fatal(err)
	}
	sourceURL, err := url.Parse(sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	sourceURL.User = url.UserPassword("analytics_app", "worker-role-secret")
	cfg := fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: sourceURL.String()}, Redis: fabriq.RedisConfig{Addr: redisAddr}, Analytics: fabriq.AnalyticsConfig{DSN: sinkDSN, HashSalt: "stable-secret"}, Dynamic: fabriq.DynamicConfig{Persist: true}}
	start := func() (*forgeext.Extension, *registry.Registry) {
		reg := registry.New()
		ext := forgeext.New(reg, forgeext.WithConfig(cfg), forgeext.WithWorker(true), forgeext.WithRequireRLS(true))
		app := forge.NewApp(forge.AppConfig{Name: "analytics-dynamic-test", HTTPAddress: ":0"})
		if err := ext.Register(app); err != nil {
			t.Fatal(err)
		}
		if err := ext.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := ext.Run(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = ext.Shutdown(stopCtx)
			_ = ext.Stop(context.Background())
		})
		return ext, reg
	}
	ext, reg := start()
	if len(reg.All()) != 0 {
		t.Fatal("registry must start empty")
	}
	spec := registry.EntitySpec{Name: "hosted_widget", Kind: registry.KindAggregate, Schema: &registry.DynamicSchema{Table: "ds_hosted_widgets", Columns: []registry.DynamicColumn{{Name: "name", Type: registry.ColText}, {Name: "secret", Type: registry.ColText}, {Name: "private", Type: registry.ColText}}}, Analytics: &registry.AnalyticsSpec{IncludeAll: true, Hash: []string{"secret"}, Exclude: []string{"private"}}}
	if err := ext.Fabriq().DefineDynamic(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if _, err := ext.Fabriq().Exec(ctx, command.Command{Entity: spec.Name, Op: command.OpCreate, Payload: map[string]any{"name": "denied"}}); err == nil {
		t.Fatal("tenantless write accepted")
	}
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	result, err := ext.Fabriq().Exec(tctx, command.Command{Entity: spec.Name, Op: command.OpCreate, Payload: map[string]any{"name": "Widget", "secret": "sensitive", "private": "hidden"}})
	if err != nil {
		t.Fatal(err)
	}
	waitVersion := func(ext *forgeext.Extension, want int64) {
		deadline := time.Now().Add(15 * time.Second)
		for {
			got, err := ext.Stores().Analytics.Watermark(ctx, "acme", spec.Name, result.AggID)
			if err != nil {
				t.Fatal(err)
			}
			if got == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("late dynamic definition did not materialize: got version %d want %d", got, want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitVersion(ext, 1)
	otherCtx, err := tenant.WithTenant(ctx, "globex")
	if err != nil {
		t.Fatal(err)
	}
	var otherRow map[string]any
	if err := ext.Fabriq().Relational().Get(otherCtx, spec.Name, result.AggID, &otherRow); err == nil {
		t.Fatal("another tenant read the source row")
	}
	rows, _, _, err := ext.Stores().Analytics.(*pganalytics.Sink).QueryReadOnly(ctx, `SELECT payload::text AS payload FROM fabriq_analytics_facts WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`, "acme", spec.Name, result.AggID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("facts: %v %v", rows, err)
	}
	payload, ok := rows[0]["payload"].(string)
	if !ok || strings.Contains(payload, "sensitive") || strings.Contains(payload, "hidden") || strings.Contains(payload, "private") || !strings.Contains(payload, "Widget") {
		t.Fatalf("redaction failed: %v", rows)
	}
	var firstPayload map[string]any
	if err := json.Unmarshal([]byte(payload), &firstPayload); err != nil {
		t.Fatal(err)
	}
	rows, _, _, err = ext.Stores().Analytics.(*pganalytics.Sink).QueryReadOnly(ctx, `SELECT count(*) AS n FROM fabriq_analytics_events WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`, "acme", spec.Name, result.AggID)
	if err != nil || len(rows) != 1 || rows[0]["n"] != int64(1) {
		t.Fatalf("create event missing: %v %v", rows, err)
	}
	redacted, err := ext.Stores().Analytics.LagByTenant(ctx)
	if err != nil || len(redacted) != 1 {
		t.Fatalf("lag: %v %v", redacted, err)
	}
	if err := ext.Health(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ext.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := ext.Shutdown(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := ext.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, reloaded := start()
	ent, ok := reloaded.Get(spec.Name)
	if !ok || ent.Spec.Analytics == nil || len(ent.Spec.Analytics.Exclude) != 1 || len(ent.Spec.Analytics.Hash) != 1 {
		t.Fatal("persisted analytics policy lost on restart")
	}
	if _, err := reopened.Fabriq().Exec(tctx, command.Command{Entity: spec.Name, Op: command.OpUpdate, AggID: result.AggID, Payload: map[string]any{"name": "Updated", "secret": "sensitive", "private": "hidden"}}); err != nil {
		t.Fatal(err)
	}
	waitVersion(reopened, 2)
	rows, _, _, err = reopened.Stores().Analytics.(*pganalytics.Sink).QueryReadOnly(ctx, `SELECT payload::text AS payload FROM fabriq_analytics_facts WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`, "acme", spec.Name, result.AggID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("updated facts: %v %v", rows, err)
	}
	var secondPayload map[string]any
	if err := json.Unmarshal([]byte(rows[0]["payload"].(string)), &secondPayload); err != nil {
		t.Fatal(err)
	}
	if secondPayload["secret"] != firstPayload["secret"] || secondPayload["private"] != nil || secondPayload["name"] != "Updated" {
		t.Fatalf("restart changed hash or redaction: %v", secondPayload)
	}
	rows, _, _, err = reopened.Stores().Analytics.(*pganalytics.Sink).QueryReadOnly(ctx, `SELECT count(*) AS n FROM fabriq_analytics_events WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`, "acme", spec.Name, result.AggID)
	if err != nil || len(rows) != 1 || rows[0]["n"] != int64(2) {
		t.Fatalf("update event missing: %v %v", rows, err)
	}
	if err := reopened.Stores().Analytics.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Ready(ctx); err == nil {
		t.Fatal("analytics outage hidden from readiness")
	}
	if err := reopened.Health(ctx); err == nil {
		t.Fatal("analytics outage hidden from health")
	}
}

//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xraph/forge/cli"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/pganalytics"
	"github.com/xraph/fabriq/core/analytics"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func runAnalyticsJob(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	c := cli.New(cli.Config{Name: "fabriq", Output: &out, ErrorHandler: func(error) {}})
	if err := c.AddCommand(analyticsCommand()); err != nil {
		t.Fatal(err)
	}
	err := c.Run(append([]string{"fabriq", "analytics"}, args...))
	return out.String(), err
}

func TestHostedAnalyticsOperatorJobs(t *testing.T) {
	ctx := context.Background()
	sourceDSN := fabriqtest.StartPostgres(t)
	sinkDSN := fabriqtest.StartPostgres(t)
	migrateUp(t, sourceDSN)
	sourceDSN = appRoleDSN(t, sourceDSN)
	cfg := fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: sourceDSN}, Analytics: fabriq.AnalyticsConfig{DSN: sinkDSN, HashSalt: "stable-secret"}, Dynamic: fabriq.DynamicConfig{Persist: true}}
	reg := registry.New()
	f, stores, err := fabriq.Open(ctx, reg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	spec := registry.EntitySpec{Name: "job_widget", Kind: registry.KindAggregate, Schema: &registry.DynamicSchema{Table: "ds_job_widgets", Columns: []registry.DynamicColumn{{Name: "name", Type: registry.ColText}, {Name: "secret", Type: registry.ColText}, {Name: "private", Type: registry.ColText}}}, Analytics: &registry.AnalyticsSpec{IncludeAll: true, Hash: []string{"secret"}, Exclude: []string{"private"}}}
	if err := f.DefineDynamic(ctx, spec); err != nil {
		t.Fatal(err)
	}
	noSalt := cfg
	noSalt.Analytics.HashSalt = ""
	if opened, _, err := fabriq.Open(ctx, registry.New(), noSalt); err == nil {
		_ = opened.Close()
		t.Fatal("persisted hash policies must reject missing salt at open")
	}
	tctx, err := tenant.WithTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.Exec(tctx, command.Command{Entity: spec.Name, Op: command.OpCreate, Payload: map[string]any{"name": "Widget", "secret": "sensitive", "private": "hidden"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("FABRIQ_POSTGRES_DSN", sourceDSN)
	t.Setenv("FABRIQ_ANALYTICS_DSN", sinkDSN)
	t.Setenv("FABRIQ_ANALYTICS_HASH_SALT", "stable-secret")
	t.Setenv("FABRIQ_DOMAIN_PACK", "none")
	t.Setenv("FABRIQ_DYNAMIC_PERSIST", "true")
	run := func(args ...string) string {
		out, err := runAnalyticsJob(t, args...)
		if err != nil {
			t.Fatalf("job %v failed: %v (%s)", args, err, out)
		}
		return out
	}
	payload := func() map[string]any {
		rows, _, _, err := stores.Analytics.(*pganalytics.Sink).QueryReadOnly(ctx, `SELECT payload::text AS payload FROM fabriq_analytics_facts WHERE tenant_id=$1 AND aggregate=$2 AND agg_id=$3`, "acme", spec.Name, result.AggID)
		if err != nil || len(rows) != 1 {
			t.Fatalf("fact rows=%v err=%v", rows, err)
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(rows[0]["payload"].(string)), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	run("backfill", "--all-tenants")
	first := payload()
	expected, err := analytics.Redact(json.RawMessage(`{"secret":"sensitive"}`), &registry.AnalyticsSpec{Hash: []string{"secret"}}, "stable-secret")
	if err != nil {
		t.Fatal(err)
	}
	var expectedMap map[string]any
	if err := json.Unmarshal(expected, &expectedMap); err != nil {
		t.Fatal(err)
	}
	if first["secret"] != expectedMap["secret"] || first["private"] != nil || first["name"] != "Widget" {
		t.Fatalf("operator redaction/salt mismatch: %v", first)
	}
	if _, err := runAnalyticsJob(t, "reproject", "--tenant", "acme"); err == nil || !strings.Contains(err.Error(), "Hash") {
		t.Fatalf("unsafe reproject must refuse: %v", err)
	}
	if payload()["secret"] != first["secret"] {
		t.Fatal("refused reproject mutated stored hash")
	}
	// Erasing and replaying current source state must reproduce the same salt hash.
	run("purge", "--tenant", "acme", "--yes")
	run("backfill", "--tenant", "acme")
	if payload()["secret"] != first["secret"] {
		t.Fatal("source replay hash changed across job reopen")
	}
	run("purge", "--tenant", "acme", "--yes")
	run("reconcile", "--tenant", "acme")
	if payload()["secret"] != first["secret"] {
		t.Fatal("reconcile used another salt")
	}
	v, err := stores.Analytics.Watermark(ctx, "acme", spec.Name, result.AggID)
	if err != nil || v != 1 {
		t.Fatalf("watermark %d %v", v, err)
	}
	run("prune-events", "--older-than", "1ns")
	// Tightening to a hashless allow-list can safely remove the stored hash.
	spec.Analytics = &registry.AnalyticsSpec{Include: []string{"name"}}
	if err := f.AlterDynamic(ctx, spec); err != nil {
		t.Fatal(err)
	}
	run("reproject", "--tenant", "acme")
	if got := payload(); got["name"] != "Widget" || got["secret"] != nil {
		t.Fatalf("hashless reproject did not narrow: %v", got)
	}
	if _, err := runAnalyticsJob(t, "purge", "--tenant", "acme"); err == nil {
		t.Fatal("purge confirmation bypassed")
	}
	if _, err := stores.Postgres.SaveDynamicSpec(ctx, spec.Name, []byte(`{"format":99,"name":"job_widget"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := runAnalyticsJob(t, "backfill", "--tenant", "acme"); err == nil {
		t.Fatal("job silently skipped invalid persisted definition")
	}
	t.Setenv("FABRIQ_ANALYTICS_DSN", "quack://remote/analytics")
	if _, err := runAnalyticsJob(t, "purge", "--tenant", "acme", "--yes"); err == nil || !strings.Contains(err.Error(), "prepared") {
		t.Fatalf("purge bypassed remote compatibility refusal: %v", err)
	}
}

//go:build integration

package forgeext_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
)

func TestAnalyticsCatalogHealth(t *testing.T) {
	ctx := context.Background()
	sourceDSN := fabriqtest.StartPostgres(t)
	sinkDSN := fabriqtest.StartPostgres(t)
	ext := forgeext.New(registry.New(), forgeext.WithConfig(fabriq.Config{Catalog: fabriq.CatalogConfig{DSN: sourceDSN, ClusterDSNs: map[string]string{"primary": sourceDSN}, AllowSuperuser: true}, Analytics: fabriq.AnalyticsConfig{DSN: sinkDSN}}))
	if err := ext.Health(ctx); err == nil {
		t.Fatal("unstarted extension reported healthy")
	}
	if err := ext.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ext.Stop(context.Background()) })
	if ext.Stores().Postgres != nil {
		t.Fatal("catalog regression must have no primary Postgres adapter")
	}
	if err := ext.Health(ctx); err != nil {
		t.Fatalf("valid catalog health: %v", err)
	}
	if err := ext.Ready(ctx); err != nil {
		t.Fatalf("valid catalog readiness: %v", err)
	}
	if err := ext.Stores().Analytics.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ext.Health(ctx); err == nil || !strings.Contains(err.Error(), "analytics") {
		t.Fatalf("catalog sink outage hidden: %v", err)
	}
	if err := ext.Ready(ctx); err == nil || !strings.Contains(err.Error(), "analytics") {
		t.Fatalf("catalog sink readiness outage hidden: %v", err)
	}
	ext.Stores().Analytics = nil
	if err := ext.Stores().Catalog.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ext.Health(ctx); err == nil || !strings.Contains(err.Error(), "catalog") {
		t.Fatalf("catalog source outage hidden: %v", err)
	}
	if err := ext.Ready(ctx); err == nil || !strings.Contains(err.Error(), "catalog") {
		t.Fatalf("catalog source readiness outage hidden: %v", err)
	}
}

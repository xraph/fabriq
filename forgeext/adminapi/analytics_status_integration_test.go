//go:build integration

package adminapi

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/migrations"
)

func TestAnalyticsStatusReportsOutage(t *testing.T) {
	ctx := context.Background()
	sourceDSN := fabriqtest.StartPostgres(t)
	sinkDSN := fabriqtest.StartPostgres(t)
	orch, closeFn, err := migrations.OpenOrchestrator(ctx, sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := closeFn(); err != nil {
		t.Fatal(err)
	}
	parent := forgeext.New(registry.New(), forgeext.WithConfig(fabriq.Config{Postgres: fabriq.PostgresConfig{DSN: sourceDSN}, Analytics: fabriq.AnalyticsConfig{DSN: sinkDSN}}))
	if err := parent.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Stop(context.Background()) })
	ext := NewAdminAPI(parent, WithAnalyticsRead())
	srv := buildServer(t, ext)
	defer srv.Close()
	assertStatus := func(want int) {
		resp := getNoTenant(t, srv, "/admin/analytics/status")
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != want {
			t.Fatalf("status=%d want %d body=%s", resp.StatusCode, want, body)
		}
		if strings.Contains(string(body), sinkDSN) {
			t.Fatal("raw connection details leaked")
		}
	}
	assertStatus(http.StatusOK)
	if err := parent.Stores().Analytics.Close(); err != nil {
		t.Fatal(err)
	}
	assertStatus(http.StatusInternalServerError)
	parent.Stores().Analytics = nil
	if err := parent.Stores().Postgres.Close(); err != nil {
		t.Fatal(err)
	}
	assertStatus(http.StatusInternalServerError)
}

//go:build integration

package migrations_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestCheckExtensions_TimescaleImage(t *testing.T) {
	dsn := fabriqtest.StartPostgres(t)

	rep, err := migrations.CheckExtensions(context.Background(), dsn, migrations.DefaultRequiredExtensions)
	if err != nil {
		t.Fatalf("CheckExtensions: %v", err)
	}
	if !rep.OK {
		t.Fatalf("report not OK: %+v", rep)
	}
	if len(rep.Extensions) != 3 {
		t.Fatalf("got %d extensions, want 3: %+v", len(rep.Extensions), rep.Extensions)
	}
	byName := map[string]migrations.ExtensionStatus{}
	for _, e := range rep.Extensions {
		byName[e.Name] = e
	}
	for _, name := range []string{"vector", "postgis"} {
		if e, ok := byName[name]; !ok || !e.Available || !e.OK {
			t.Fatalf("%s = %+v, want available and OK", name, e)
		}
	}
	if e := byName["timescaledb"]; !e.Available || !e.Preloaded || !e.OK {
		t.Fatalf("timescaledb = %+v, want available, preloaded and OK", e)
	}
	if len(rep.Problems) != 0 {
		t.Fatalf("problems = %v, want none", rep.Problems)
	}
}

func TestCheckExtensions_UnknownExtension(t *testing.T) {
	dsn := fabriqtest.StartPostgres(t)

	rep, err := migrations.CheckExtensions(context.Background(), dsn, []string{"vector", "nope_ext"})
	if err != nil {
		t.Fatalf("CheckExtensions: %v", err)
	}
	if rep.OK {
		t.Fatalf("report OK with an unknown extension: %+v", rep)
	}
	if len(rep.Extensions) != 2 {
		t.Fatalf("got %d extensions, want 2", len(rep.Extensions))
	}
	if !rep.Extensions[0].OK || rep.Extensions[1].OK || rep.Extensions[1].Available {
		t.Fatalf("extensions = %+v", rep.Extensions)
	}
	if len(rep.Problems) != 1 || rep.Problems[0] != "nope_ext: not available" {
		t.Fatalf("problems = %v", rep.Problems)
	}
}

func TestCheckExtensions_BadDSN(t *testing.T) {
	_, err := migrations.CheckExtensions(context.Background(), "postgres://nobody:pw@127.0.0.1:1/none?sslmode=disable&connect_timeout=2", migrations.DefaultRequiredExtensions)
	if err == nil {
		t.Fatal("expected a connection error")
	}
	if strings.Contains(err.Error(), "pw@") {
		t.Fatalf("error leaks the DSN credentials: %v", err)
	}
}

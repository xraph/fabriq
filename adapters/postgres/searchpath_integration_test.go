//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/pathctx"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestSearchPath_RoutesToSchema_FailsClosed proves the Task 2 mechanism on
// real Postgres: with a tenant schema on ctx the per-tx SET LOCAL search_path
// resolves a bare table name to that tenant's schema; without one (shared
// path only) the same bare name is relation-not-found — the mode fails
// CLOSED rather than crossing tenants.
//
// This exercises stampSearchPath directly through TenantTxRaw (the sanctioned
// raw tenant-tx seam, which runs inside inTenantTx). It deliberately uses a
// bare "probe" table in each schema rather than the full migration chain: the
// claim under test is search_path routing of bare identifiers, which grove
// emits everywhere. End-to-end provisioning + RLS is proven in the
// SchemaClusterOps and sweeper integration tests.
func TestSearchPath_RoutesToSchema_FailsClosed(t *testing.T) {
	superDSN := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	reg := registry.New()
	a, err := postgres.Open(ctx, superDSN, reg, postgres.WithSharedSchema("fabriq_shared"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	// Two tenant schemas + a shared schema, each with a bare-named probe table.
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS fabriq_shared`,
		`CREATE SCHEMA IF NOT EXISTS tenant_a`,
		`CREATE SCHEMA IF NOT EXISTS tenant_b`,
		`CREATE TABLE tenant_a.probe (v text)`,
		`CREATE TABLE tenant_b.probe (v text)`,
	} {
		if _, err := a.Driver().Exec(ctx, stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}

	// A write stamped for tenant_a must land in tenant_a.probe (bare name
	// routed by search_path), and nowhere else.
	actx := pathctx.MustWithSearchPath(mustTenant(t, "a"), "tenant_a")
	if err := a.TenantTxRaw(actx, func(tx *pgdriver.PgTx) error {
		_, e := tx.NewRaw(`INSERT INTO probe (v) VALUES ('x')`).Exec(actx)
		return e
	}); err != nil {
		t.Fatalf("stamped insert: %v", err)
	}

	if got := probeCount(t, a, "tenant_a"); got != 1 {
		t.Fatalf("tenant_a.probe count = %d, want 1", got)
	}
	if got := probeCount(t, a, "tenant_b"); got != 0 {
		t.Fatalf("tenant_b.probe count = %d, want 0 (leak!)", got)
	}

	// A tenant-stamped tx with NO schema on ctx does not stamp search_path,
	// so the bare "probe" resolves against the connection default (public) —
	// where no such table exists. Fails CLOSED (relation not found), never
	// cross-tenant.
	nctx := mustTenant(t, "a")
	err = a.TenantTxRaw(nctx, func(tx *pgdriver.PgTx) error {
		_, e := tx.NewRaw(`INSERT INTO probe (v) VALUES ('leak')`).Exec(nctx)
		return e
	})
	if err == nil {
		t.Fatal("unstamped write to a bare tenant table should fail closed, got nil error")
	}
}

func mustTenant(t testing.TB, id string) context.Context {
	t.Helper()
	ctx, err := tenant.WithTenant(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func probeCount(t testing.TB, a *postgres.Adapter, schema string) int {
	t.Helper()
	var n int
	// Explicit schema qualification here — the test verifies routing, so it
	// must read each schema unambiguously, not via search_path.
	if err := a.Driver().QueryRow(context.Background(),
		`SELECT count(*) FROM `+schema+`.probe`).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", schema, err)
	}
	return n
}

// BenchmarkStamp_SearchPathOverhead measures the added cost of the per-tx
// search_path stamp (one extra set_config round-trip) versus a tenant+scope
// stamp alone. Target: < 5% delta amortized in the tx.
func BenchmarkStamp_SearchPathOverhead(b *testing.B) {
	superDSN := fabriqtest.StartPostgres(b)
	ctx := context.Background()
	reg := registry.New()
	a, err := postgres.Open(ctx, superDSN, reg, postgres.WithSharedSchema("fabriq_shared"))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer func() { _ = a.Close() }()
	for _, stmt := range []string{
		`CREATE SCHEMA IF NOT EXISTS fabriq_shared`,
		`CREATE SCHEMA IF NOT EXISTS tenant_a`,
		`CREATE TABLE tenant_a.probe (v text)`,
	} {
		if _, err := a.Driver().Exec(ctx, stmt); err != nil {
			b.Fatalf("setup: %v", err)
		}
	}
	withPath := pathctx.MustWithSearchPath(mustTenantB(b, "a"), "tenant_a")
	noPath := mustTenantB(b, "a")

	run := func(b *testing.B, c context.Context, table string) {
		for i := 0; i < b.N; i++ {
			_ = a.TenantTxRaw(c, func(tx *pgdriver.PgTx) error {
				_, e := tx.NewRaw(`SELECT count(*) FROM ` + table).Exec(c)
				return e
			})
		}
	}
	b.Run("with_search_path", func(b *testing.B) { run(b, withPath, "probe") })
	b.Run("tenant_scope_only", func(b *testing.B) { run(b, noPath, "tenant_a.probe") })
}

func mustTenantB(b testing.TB, id string) context.Context {
	b.Helper()
	ctx, err := tenant.WithTenant(context.Background(), id)
	if err != nil {
		b.Fatal(err)
	}
	return ctx
}

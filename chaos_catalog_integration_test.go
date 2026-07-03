//go:build integration

package fabriq_test

// Chaos-style contracts for catalog mode (spec P6, failure-mode table):
// boot assertions fail fast; a dead tenant database trips only its own
// breaker (others serve on, the sweeper isolates it, recovery is
// automatic); a catalog outage keeps cached routes serving and is never
// negative-cached (recovery is immediate).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestCatalogMode_BootAssertions(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	reg := cmRegistry(t)

	// Superuser serving credentials are refused (RLS does not bind them).
	_, _, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn}},
	})
	if err == nil || !strings.Contains(err.Error(), "superuser") {
		t.Fatalf("superuser cluster creds must be refused at boot, got %v", err)
	}

	// Every cluster must dial at Open — a dead cluster fails the boot,
	// not the first tenant request routed to it.
	_, _, err = fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN: dsn,
			ClusterDSNs: map[string]string{
				"c1": dsn,
				"c2": "postgres://ghost:ghost@127.0.0.1:1/none?sslmode=disable&connect_timeout=1",
			},
			AllowSuperuser: true,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "does not dial") {
		t.Fatalf("unreachable cluster must fail the boot, got %v", err)
	}
}

// killDatabase makes a database undialable (terminate + rename away) and
// returns the restore func. Run from a DSN pointing at a DIFFERENT database.
func killDatabase(t *testing.T, adminDSN, name string) (restore func()) {
	t.Helper()
	terminate := func() {
		fabriqtest.QueryStrings(t, adminDSN,
			`SELECT COALESCE(pg_terminate_backend(pid)::text, '') FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, name)
	}
	terminate()
	fabriqtest.ApplyDDL(t, adminDSN, []string{
		`ALTER DATABASE ` + name + ` RENAME TO ` + name + `_dark`})
	return func() {
		fabriqtest.ApplyDDL(t, adminDSN, []string{
			`ALTER DATABASE ` + name + `_dark RENAME TO ` + name})
	}
}

func TestChaos_TenantDBDownOnlyTripsItsOwnBreaker(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	for _, tid := range []string{"acme", "globex"} {
		if _, err := p.Provision(ctx, tid, "c1"); err != nil {
			t.Fatal(err)
		}
		tdsn, _ := ops.TenantDSN("c1", "fabriq_"+tid)
		fabriqtest.ApplyDDL(t, tdsn, cmDDL())
	}
	_ = cat.Close()

	reg := cmRegistry(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn}, AllowSuperuser: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	// acme is served (its shard dials); globex dies before first touch.
	acmeCtx, _ := tenant.WithTenant(ctx, "acme")
	if _, err := f.Exec(acmeCtx, command.Command{
		Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "pre-chaos"},
	}); err != nil {
		t.Fatal(err)
	}
	acmeDSN, _ := ops.TenantDSN("c1", "fabriq_acme")
	restore := killDatabase(t, acmeDSN, "fabriq_globex")
	restored := false
	defer func() {
		if !restored {
			restore()
		}
	}()

	// The dead tenant fails typed; the immediate retry hits the OPEN
	// breaker (fast-fail, no dial storm). Both are CodeUnavailable.
	globexCtx, _ := tenant.WithTenant(ctx, "globex")
	widget := command.Command{Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "x"}}
	if _, err := f.Exec(globexCtx, widget); fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("dead tenant DB: err = %v, want CodeUnavailable", err)
	}
	if _, err := f.Exec(globexCtx, widget); fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("breaker-open retry: err = %v, want CodeUnavailable", err)
	}

	// The sweeper isolates the dead tenant: acme sweeps, globex errors,
	// the pass never aborts.
	var sweepErrs []string
	eng := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		MinVersion: migrations.HeadVersion(),
		OnError:    func(tid string, _ error) { sweepErrs = append(sweepErrs, tid) },
	})
	stats := eng.Pass(ctx)
	if stats.Errors != 1 || len(sweepErrs) != 1 || sweepErrs[0] != "globex" {
		t.Fatalf("pass stats = %+v errs = %v, want exactly globex failing", stats, sweepErrs)
	}

	// The healthy tenant is untouched by its neighbor's outage.
	if _, err := f.Exec(acmeCtx, command.Command{
		Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "mid-chaos"},
	}); err != nil {
		t.Fatalf("healthy tenant affected by neighbor outage: %v", err)
	}

	// Recovery is automatic once the database is back and the dial
	// breaker's backoff lapses.
	restore()
	restored = true
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := f.Exec(globexCtx, widget)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant never recovered: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	eng.Wake("globex")
	if stats := eng.Pass(ctx); stats.Errors != 0 {
		t.Fatalf("post-recovery sweep still failing: %+v", stats)
	}
}

func TestChaos_CatalogOutageServesCachedRoutes(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	for _, tid := range []string{"acme", "beta"} {
		if _, err := p.Provision(ctx, tid, "c1"); err != nil {
			t.Fatal(err)
		}
		tdsn, _ := ops.TenantDSN("c1", "fabriq_"+tid)
		fabriqtest.ApplyDDL(t, tdsn, cmDDL())
	}
	_ = cat.Close()

	reg := cmRegistry(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn},
			CacheTTL:       time.Minute, // outage window < TTL: cached routes must carry
			AllowSuperuser: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	// acme's route is cached (and its shard dialed); beta stays uncached.
	acmeCtx, _ := tenant.WithTenant(ctx, "acme")
	widget := command.Command{Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "w"}}
	if _, err := f.Exec(acmeCtx, widget); err != nil {
		t.Fatal(err)
	}

	// The control database goes dark ("fabriq" is the catalog DB; tenant
	// databases are unaffected).
	acmeDSN, _ := ops.TenantDSN("c1", "fabriq_acme")
	restore := killDatabase(t, acmeDSN, "fabriq")
	restored := false
	defer func() {
		if !restored {
			restore()
		}
	}()

	// Cached routes keep serving through the outage.
	if _, err := f.Exec(acmeCtx, widget); err != nil {
		t.Fatalf("cached route must serve through a catalog outage: %v", err)
	}

	// Uncached tenants fail while the catalog is dark…
	betaCtx, _ := tenant.WithTenant(ctx, "beta")
	if _, err := f.Exec(betaCtx, widget); err == nil {
		t.Fatal("uncached tenant must fail during a catalog outage")
	}

	// The sweeper pauses (a scan finds nothing) instead of crashing.
	eng := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		MinVersion: migrations.HeadVersion(),
	})
	if stats := eng.Pass(ctx); stats.Scanned != 0 {
		t.Fatalf("outage pass = %+v, want a paused (empty) scan", stats)
	}

	// …and recovery is IMMEDIATE: transport failures are never
	// negative-cached, so beta routes on the very next request.
	restore()
	restored = true
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := f.Exec(betaCtx, widget)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("catalog recovery not picked up: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if stats := eng.Pass(ctx); stats.Scanned == 0 || stats.Errors != 0 {
		t.Fatalf("post-recovery sweep = %+v", stats)
	}
}

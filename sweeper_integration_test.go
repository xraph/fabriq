//go:build integration

package fabriq_test

// Catalog-mode sweeper end-to-end (spec Phase 5): one sweep engine works
// TWO tenants' dedicated databases — outbox rows relay into the shared
// Redis stream, quiet documents materialize into their entity rows, and
// the write path's wake nudge reaches a subscriber.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xraph/grove/crdt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/sweep"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

func TestSweeper_TwoTenantDBs_RelaysAndMaterializes(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	redisAddr := fabriqtest.StartRedis(t)

	// Provision two tenants (P4) with the app DDL applied.
	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	tenantDSNs := map[string]string{}
	for _, tid := range []string{"acme", "globex"} {
		if _, err := p.Provision(ctx, tid, "c1"); err != nil {
			t.Fatalf("provision %s: %v", tid, err)
		}
		tenantDSNs[tid], _ = ops.TenantDSN("c1", "fabriq_"+tid)
		fabriqtest.ApplyDDL(t, tenantDSNs[tid], cmDDL())
	}
	_ = cat.Close()

	reg := cmRegistry(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn}, AllowSuperuser: true},
		Redis:   fabriq.RedisConfig{Addr: redisAddr},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	// The wake nudge: the write path must publish the tenant on commit.
	wakes := make(chan string, 8)
	ready := make(chan struct{})
	subCtx, subCancel := context.WithCancel(ctx)
	t.Cleanup(subCancel)
	go func() {
		_ = stores.Redis.SubscribeWakes(subCtx, func(tid string) { wakes <- tid }, ready)
	}()
	<-ready

	// Writes in BOTH tenants: a command (outbox row) and, for acme, a CRDT
	// document update (materialization work).
	for _, tid := range []string{"acme", "globex"} {
		tctx, _ := tenant.WithTenant(ctx, tid)
		if _, err := f.Exec(tctx, command.Command{
			Entity: "cmwidget", Op: command.OpCreate,
			Payload: &cmWidget{Name: "hello-" + tid},
		}); err != nil {
			t.Fatalf("exec %s: %v", tid, err)
		}
	}
	select {
	case tid := <-wakes:
		if tid != "acme" && tid != "globex" {
			t.Fatalf("wake for unexpected tenant %q", tid)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no wake nudge observed after a committed command")
	}

	acmeCtx, _ := tenant.WithTenant(ctx, "acme")
	docID := "cmnote/01SWEEPERTEST0000000000001"
	update, _ := json.Marshal([]crdt.ChangeRecord{{
		Table: "cm_notes", PK: docID, Field: "title", CRDTType: crdt.TypeLWW,
		HLC: crdt.HLC{Timestamp: 1, NodeID: "n1"}, NodeID: "n1",
		Value: json.RawMessage(`"swept title"`),
	}})
	if err := f.Document().ApplyUpdate(acmeCtx, docID, update); err != nil {
		t.Fatalf("ApplyUpdate: %v", err)
	}

	// One engine pass, built exactly the way the forgeext worker builds it.
	eng := sweep.New(stores.Catalog, stores.TenantSweeper(), sweep.Config{
		MinVersion: migrations.HeadVersion(),
		OnError:    func(tid string, err error) { t.Errorf("sweep %s: %v", tid, err) },
	})
	stats := eng.Pass(ctx)
	if stats.Swept != 2 || stats.Busy != 2 || stats.Errors != 0 {
		t.Fatalf("pass 1 stats = %+v, want both tenants swept busy", stats)
	}

	// Both tenants' outboxes relayed into the shared Redis stream with
	// recorded stream ids, physically per database. (acme may hold ONE new
	// unpublished row: the pass relays before it materializes, so the
	// materialization event it just enqueued rides the next pass.)
	for tid, tdsn := range tenantDSNs {
		streamIDs := fabriqtest.QueryStrings(t, tdsn,
			`SELECT stream_id FROM fabriq_outbox WHERE stream_id IS NOT NULL AND stream_id <> ''`)
		if len(streamIDs) == 0 {
			t.Fatalf("%s: no stream ids recorded — envelopes never reached Redis", tid)
		}
	}
	unrelayed := fabriqtest.QueryStrings(t, tenantDSNs["globex"],
		`SELECT id FROM fabriq_outbox WHERE published_at IS NULL`)
	if len(unrelayed) != 0 {
		t.Fatalf("globex: %d outbox rows left unrelayed", len(unrelayed))
	}

	// The quiet document materialized into its entity row in acme's OWN
	// database (QuietWindow 0: quiet immediately).
	titles := fabriqtest.QueryStrings(t, tenantDSNs["acme"],
		`SELECT title FROM cm_notes`)
	if len(titles) != 1 || titles[0] != "swept title" {
		t.Fatalf("materialized cm_notes rows = %v", titles)
	}

	// Materialization enqueued a cmnote.updated event; the next pass
	// relays it (acme reported busy, so it is due immediately).
	stats = eng.Pass(ctx)
	if stats.Errors != 0 {
		t.Fatalf("pass 2 stats = %+v", stats)
	}
	unpublished := fabriqtest.QueryStrings(t, tenantDSNs["acme"],
		`SELECT id FROM fabriq_outbox WHERE published_at IS NULL`)
	if len(unpublished) != 0 {
		t.Fatalf("materialization event left unrelayed: %v", unpublished)
	}

	// Idle accounting: with everything drained, the next pass finds no
	// work and both tenants report idle (they will back off).
	stats = eng.Pass(ctx)
	if stats.Busy != 0 || stats.Errors != 0 {
		t.Fatalf("pass 3 stats = %+v, want fully idle", stats)
	}
}

//go:build integration

package fabriq_test

// Catalog serving mode end-to-end: provision two tenants onto dedicated
// databases, then serve BOTH through ONE fabriq facade — commands, reads,
// physical isolation, and the per-tenant CRDT document plane.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xraph/grove/crdt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestCatalogMode_ServesTenantsFromDedicatedDatabases(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t) // control DB + cluster maintenance DSN

	// Provision two tenants (the P4 machinery).
	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	for _, tid := range []string{"acme", "globex"} {
		if _, err := p.Provision(ctx, tid, "c1"); err != nil {
			t.Fatalf("provision %s: %v", tid, err)
		}
		tenantDSN, derr := ops.TenantDSN("c1", "fabriq_"+tid)
		if derr != nil {
			t.Fatal(derr)
		}
		fabriqtest.ApplyDDL(t, tenantDSN, cmDDL())
	}
	_ = cat.Close()

	// One facade serves both tenants.
	reg := cmRegistry(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN:            dsn,
			ClusterDSNs:    map[string]string{"c1": dsn},
			AllowSuperuser: true, // testcontainers hand out superuser creds
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	ids := map[string]string{}
	for _, tid := range []string{"acme", "globex"} {
		tctx, _ := tenant.WithTenant(ctx, tid)
		res, execErr := f.Exec(tctx, command.Command{
			Entity: "cmwidget", Op: command.OpCreate,
			Payload: &cmWidget{Name: "hello-" + tid},
		})
		if execErr != nil {
			t.Fatalf("exec %s: %v", tid, execErr)
		}
		ids[tid] = res.AggID

		var got cmWidget
		if err := f.Relational().Get(tctx, "cmwidget", res.AggID, &got); err != nil {
			t.Fatalf("read-back %s: %v", tid, err)
		}
		if got.Name != "hello-"+tid {
			t.Fatalf("%s read-back = %+v", tid, got)
		}
	}

	// Physical isolation: each row lives ONLY in its tenant's database.
	for _, tid := range []string{"acme", "globex"} {
		tenantDSN, _ := ops.TenantDSN("c1", "fabriq_"+tid)
		rows := fabriqtest.QueryStrings(t, tenantDSN, `SELECT name FROM cm_widgets`)
		if len(rows) != 1 || rows[0] != "hello-"+tid {
			t.Fatalf("%s database rows = %v", tid, rows)
		}
	}

	// Cross-tenant reads miss (routing, not RLS, is already the boundary).
	acmeCtx, _ := tenant.WithTenant(ctx, "acme")
	var leak cmWidget
	if err := f.Relational().Get(acmeCtx, "cmwidget", ids["globex"], &leak); err == nil {
		t.Fatal("tenant acme must not see globex's row")
	}

	// Unknown tenants are typed 404s, not 500s.
	ghostCtx, _ := tenant.WithTenant(ctx, "ghost")
	if _, err := f.Exec(ghostCtx, command.Command{
		Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "x"},
	}); err == nil {
		t.Fatal("unknown tenant must be rejected")
	}
}

func TestCatalogMode_DocumentPlanePerTenantDB(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatal(err)
	}
	_ = cat.Close()
	tenantDSN, _ := ops.TenantDSN("c1", "fabriq_acme")
	fabriqtest.ApplyDDL(t, tenantDSN, cmDDL())

	reg := cmRegistry(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn}, AllowSuperuser: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	tctx, _ := tenant.WithTenant(ctx, "acme")
	docID := "cmnote/01CATALOGMODE0000000000001"
	update, _ := json.Marshal([]crdt.ChangeRecord{{
		Table: "cm_notes", PK: docID, Field: "title", CRDTType: crdt.TypeLWW,
		HLC: crdt.HLC{Timestamp: 1, NodeID: "n1"}, NodeID: "n1",
		Value: json.RawMessage(`"hello doc"`),
	}})
	if err := f.Document().ApplyUpdate(tctx, docID, update); err != nil {
		t.Fatalf("ApplyUpdate: %v", err)
	}
	raw, err := f.Document().Sync(tctx, docID, nil)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	var payload struct {
		Seq     int64             `json:"seq"`
		Updates []json.RawMessage `json:"updates"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Seq != 1 || len(payload.Updates) != 1 {
		t.Fatalf("sync payload = %+v", payload)
	}

	// The CRDT log physically lives in the tenant's own database.
	rows := fabriqtest.QueryStrings(t, tenantDSN,
		`SELECT doc_id FROM fabriq_crdt_updates`)
	if len(rows) != 1 || rows[0] != docID {
		t.Fatalf("tenant-db crdt log = %v", rows)
	}
}

// TestCatalogMode_WithReplicaDSN_Routes proves the wiring: configuring
// ReplicaDSNs builds the Failover routing path and the facade still serves
// tenants normally. Using the same DB as its own "replica" only proves the
// path builds and routes; true standby fallback behaviour is validated by
// the chaos suite (Task 7).
func TestCatalogMode_WithReplicaDSN_Routes(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)

	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	ops := postgres.NewClusterOps(map[string]string{"c1": dsn})
	p := provision.New(cat, ops)
	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatal(err)
	}
	tenantDSN, _ := ops.TenantDSN("c1", "fabriq_acme")
	fabriqtest.ApplyDDL(t, tenantDSN, cmDDL())
	_ = cat.Close()

	// Same DB doubles as its own "replica" here: this only proves the wiring
	// builds the Failover path and routes; true standby behaviour is Task 7.
	reg := cmRegistry(t)
	f, stores, err := fabriq.Open(ctx, reg, fabriq.Config{
		Catalog: fabriq.CatalogConfig{
			DSN: dsn, ClusterDSNs: map[string]string{"c1": dsn},
			ReplicaDSNs: []string{dsn}, AllowSuperuser: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stores.Close() })

	acmeCtx, _ := tenant.WithTenant(ctx, "acme")
	if _, err := f.Exec(acmeCtx, command.Command{
		Entity: "cmwidget", Op: command.OpCreate, Payload: &cmWidget{Name: "w"},
	}); err != nil {
		t.Fatalf("replica-configured routing failed: %v", err)
	}
}

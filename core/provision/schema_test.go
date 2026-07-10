package provision_test

import (
	"context"
	"sync"
	"testing"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/fabriqtest"
)

// fakeSchemaOps records schema-mode physical operations and injects failures.
type fakeSchemaOps struct {
	mu        sync.Mutex
	bootstrap map[string]int // "cluster/db" -> count
	schemas   map[string]int // "cluster/db/schema" -> count
	migrated  map[string]int // "cluster/db/schema" -> count
	version   string
	failOn    map[string]error // "bootstrap"/"createSchema"/"migrate"
}

func newFakeSchemaOps() *fakeSchemaOps {
	return &fakeSchemaOps{
		bootstrap: map[string]int{}, schemas: map[string]int{}, migrated: map[string]int{},
		version: "v9", failOn: map[string]error{},
	}
}

func (f *fakeSchemaOps) EnsureBootstrap(_ context.Context, clusterID, database, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failOn["bootstrap"]; err != nil {
		return err
	}
	f.bootstrap[clusterID+"/"+database]++
	return nil
}

func (f *fakeSchemaOps) CreateSchema(_ context.Context, clusterID, database, schema string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failOn["createSchema"]; err != nil {
		return err
	}
	f.schemas[clusterID+"/"+database+"/"+schema]++
	return nil
}

func (f *fakeSchemaOps) MigrateSchema(_ context.Context, clusterID, database, schema, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failOn["migrate"]; err != nil {
		return "", err
	}
	f.migrated[clusterID+"/"+database+"/"+schema]++
	return f.version, nil
}

func TestSchemaProvision_HappyPath_States(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeSchemaOps()
	p := provision.NewSchemaProvisioner(cat, ops, "fabriq_shared")

	e, err := p.Provision(context.Background(), "acme", "c1", "pool_a")
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	if e.State != catalog.StateActive || e.Database != "pool_a" || e.Schema != "tenant_acme" || e.Version != "v9" {
		t.Fatalf("unexpected entry: %+v", e)
	}
	if ops.bootstrap["c1/pool_a"] != 1 || ops.schemas["c1/pool_a/tenant_acme"] != 1 || ops.migrated["c1/pool_a/tenant_acme"] != 1 {
		t.Fatalf("ops mis-called: %+v", ops)
	}
}

func TestSchemaProvision_BootstrapPerCall_ButIdempotent(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeSchemaOps()
	p := provision.NewSchemaProvisioner(cat, ops, "fabriq_shared")
	ctx := context.Background()

	if _, err := p.Provision(ctx, "acme", "c1", "pool_a"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Provision(ctx, "beta", "c1", "pool_a"); err != nil {
		t.Fatal(err)
	}
	// Both tenants share pool_a: bootstrap invoked once per provision (the op
	// itself is idempotent), and each gets its own schema.
	if ops.bootstrap["c1/pool_a"] != 2 {
		t.Fatalf("bootstrap count = %d, want 2 (once per provision, idempotent op)", ops.bootstrap["c1/pool_a"])
	}
	if ops.schemas["c1/pool_a/tenant_acme"] != 1 || ops.schemas["c1/pool_a/tenant_beta"] != 1 {
		t.Fatalf("per-tenant schemas not created: %+v", ops.schemas)
	}
}

func TestSchemaProvision_SecondRunIsNoop(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeSchemaOps()
	p := provision.NewSchemaProvisioner(cat, ops, "fabriq_shared")
	ctx := context.Background()

	if _, err := p.Provision(ctx, "acme", "c1", "pool_a"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Provision(ctx, "acme", "c1", "pool_a"); err != nil {
		t.Fatal(err)
	}
	if ops.migrated["c1/pool_a/tenant_acme"] != 1 {
		t.Fatalf("second run re-migrated: %+v", ops.migrated)
	}
}

func TestSchemaProvision_ResumesFromMigrating(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeSchemaOps()
	// Seed a half-provisioned entry stuck in migrating.
	if _, err := cat.Put(context.Background(), catalog.Entry{
		TenantID: "acme", ClusterID: "c1", Database: "pool_a", Schema: "tenant_acme", State: catalog.StateMigrating,
	}); err != nil {
		t.Fatal(err)
	}
	p := provision.NewSchemaProvisioner(cat, ops, "fabriq_shared")
	e, err := p.Provision(context.Background(), "acme", "c1", "pool_a")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if e.State != catalog.StateActive {
		t.Fatalf("did not converge to active: %s", e.State)
	}
}

func TestSchemaProvision_WrongPlacementRejected(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeSchemaOps()
	p := provision.NewSchemaProvisioner(cat, ops, "fabriq_shared")
	ctx := context.Background()
	if _, err := p.Provision(ctx, "acme", "c1", "pool_a"); err != nil {
		t.Fatal(err)
	}
	// Re-provision onto a different database must be rejected (moves are separate).
	if _, err := p.Provision(ctx, "acme", "c1", "pool_b"); fabriqerr.CodeOf(err) != fabriqerr.CodeConstraintViolation {
		t.Fatalf("err = %v, want CodeConstraintViolation", err)
	}
}

func TestSchemaProvision_StepFailureFlagsFailed(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeSchemaOps()
	ops.failOn["migrate"] = fabriqerr.New(fabriqerr.CodeInternal, "boom")
	p := provision.NewSchemaProvisioner(cat, ops, "fabriq_shared")
	ctx := context.Background()
	if _, err := p.Provision(ctx, "acme", "c1", "pool_a"); err == nil {
		t.Fatal("expected migrate failure")
	}
	got, _ := cat.Get(ctx, "acme")
	if got.State != catalog.StateFailed {
		t.Fatalf("state = %s, want failed (listable)", got.State)
	}
}

func TestSchemaProvision_MigrateAll_RecordsVersions(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeSchemaOps()
	p := provision.NewSchemaProvisioner(cat, ops, "fabriq_shared")
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if _, err := p.Provision(ctx, id, "c1", "pool_a"); err != nil {
			t.Fatal(err)
		}
	}
	ops.version = "v10"
	rep, err := p.MigrateAll(ctx, provision.MigrateAllOpts{Batch: 2})
	if err != nil {
		t.Fatalf("migrate-all: %v", err)
	}
	if rep.Migrated != 3 {
		t.Fatalf("migrated = %d, want 3", rep.Migrated)
	}
	got, _ := cat.Get(ctx, "b")
	if got.Version != "v10" {
		t.Fatalf("version not recorded: %q", got.Version)
	}
}

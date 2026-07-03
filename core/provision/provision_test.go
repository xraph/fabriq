package provision_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/provision"
	"github.com/xraph/fabriq/fabriqtest"
)

// fakeOps records physical operations and injects failures.
type fakeOps struct {
	mu       sync.Mutex
	created  map[string]int // "cluster/db" -> count
	migrated map[string]int
	version  string
	failOn   map[string]error // "create"/"migrate" -> err (also per-db "migrate:db")
}

func newFakeOps() *fakeOps {
	return &fakeOps{created: map[string]int{}, migrated: map[string]int{}, version: "v1", failOn: map[string]error{}}
}

func (f *fakeOps) CreateDatabase(_ context.Context, clusterID, database string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failOn["create"]; err != nil {
		return err
	}
	f.created[clusterID+"/"+database]++
	return nil
}

func (f *fakeOps) Migrate(_ context.Context, clusterID, database string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failOn["migrate:"+database]; err != nil {
		return "", err
	}
	if err := f.failOn["migrate"]; err != nil {
		return "", err
	}
	f.migrated[clusterID+"/"+database]++
	return f.version, nil
}

func TestProvision_HappyPath(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeOps()
	p := provision.New(cat, ops)

	entry, err := p.Provision(context.Background(), "acme", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if entry.State != catalog.StateActive || entry.Version != "v1" ||
		entry.Database != "fabriq_acme" || entry.ClusterID != "c1" {
		t.Fatalf("entry = %+v", entry)
	}
	if ops.created["c1/fabriq_acme"] != 1 || ops.migrated["c1/fabriq_acme"] != 1 {
		t.Fatalf("ops = %+v / %+v", ops.created, ops.migrated)
	}
}

func TestProvision_SecondRunIsNoop(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeOps()
	p := provision.New(cat, ops)
	ctx := context.Background()

	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatal(err)
	}
	if ops.created["c1/fabriq_acme"] != 1 || ops.migrated["c1/fabriq_acme"] != 1 {
		t.Fatalf("active tenant must not re-run physical steps: %+v %+v", ops.created, ops.migrated)
	}
}

func TestProvision_ResumesFromEveryState(t *testing.T) {
	for _, stuck := range []catalog.State{
		catalog.StatePending, catalog.StateCreating, catalog.StateMigrating, catalog.StateFailed,
	} {
		cat := fabriqtest.NewFakeCatalog()
		ops := newFakeOps()
		p := provision.New(cat, ops)
		ctx := context.Background()

		// Simulate a crash: a catalog row parked in `stuck`.
		if _, err := cat.Put(ctx, catalog.Entry{
			TenantID: "acme", ClusterID: "c1", Database: "fabriq_acme", State: stuck,
		}); err != nil {
			t.Fatal(err)
		}

		entry, err := p.Provision(ctx, "acme", "c1")
		if err != nil {
			t.Fatalf("resume from %s: %v", stuck, err)
		}
		if entry.State != catalog.StateActive {
			t.Fatalf("resume from %s: state = %s", stuck, entry.State)
		}
		if ops.created["c1/fabriq_acme"] != 1 || ops.migrated["c1/fabriq_acme"] != 1 {
			t.Fatalf("resume from %s must run both idempotent steps exactly once: %+v %+v",
				stuck, ops.created, ops.migrated)
		}
	}
}

func TestProvision_StepFailureFlagsFailed(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeOps()
	ops.failOn["migrate"] = errors.New("chain exploded")
	p := provision.New(cat, ops)
	ctx := context.Background()

	_, err := p.Provision(ctx, "acme", "c1")
	if fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("err = %v", err)
	}
	got, gerr := cat.Get(ctx, "acme")
	if gerr != nil || got.State != catalog.StateFailed {
		t.Fatalf("entry after failure = %+v (%v)", got, gerr)
	}
	// Clearing the fault and re-running converges.
	delete(ops.failOn, "migrate")
	entry, err := p.Provision(ctx, "acme", "c1")
	if err != nil || entry.State != catalog.StateActive {
		t.Fatalf("recovery: %+v (%v)", entry, err)
	}
}

func TestProvision_WrongClusterRejected(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	p := provision.New(cat, newFakeOps())
	ctx := context.Background()
	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Provision(ctx, "acme", "c2"); fabriqerr.CodeOf(err) != fabriqerr.CodeConstraintViolation {
		t.Fatalf("err = %v, want CodeConstraintViolation", err)
	}
}

func TestProvision_ConcurrentSingleWinner(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeOps()
	p := provision.New(cat, ops)
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = p.Provision(ctx, "acme", "c1")
		}(i)
	}
	wg.Wait()

	// At least one provisioner converges the tenant; losers see CAS
	// conflicts (or the already-exists create race), never corruption.
	got, err := cat.Get(ctx, "acme")
	if err != nil || got.State != catalog.StateActive {
		t.Fatalf("final entry = %+v (%v)", got, err)
	}
	winners := 0
	for _, e := range errs {
		if e == nil {
			winners++
			continue
		}
		switch fabriqerr.CodeOf(e) {
		case fabriqerr.CodeVersionConflict, fabriqerr.CodeAlreadyExists:
		default:
			t.Fatalf("unexpected concurrent error: %v", e)
		}
	}
	if winners == 0 {
		t.Fatal("no provisioner won")
	}
}

func TestSuspendResume_Lifecycle(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	p := provision.New(cat, newFakeOps())
	ctx := context.Background()
	if _, err := p.Provision(ctx, "acme", "c1"); err != nil {
		t.Fatal(err)
	}
	e, err := p.Suspend(ctx, "acme")
	if err != nil || e.State != catalog.StateSuspended {
		t.Fatalf("suspend: %+v (%v)", e, err)
	}
	// Suspended tenants cannot be re-provisioned (explicit resume only).
	if _, err := p.Provision(ctx, "acme", "c1"); fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("provision suspended: %v", err)
	}
	e, err = p.Resume(ctx, "acme")
	if err != nil || e.State != catalog.StateActive {
		t.Fatalf("resume: %+v (%v)", e, err)
	}
	if _, err := p.Resume(ctx, "acme"); fabriqerr.CodeOf(err) != fabriqerr.CodeConstraintViolation {
		t.Fatalf("resume active: %v", err)
	}
}

func seedFleet(t *testing.T, cat catalog.Catalog, p *provision.Provisioner, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := p.Provision(context.Background(), fmt.Sprintf("t-%03d", i), "c1"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMigrateAll_RollsFleetAndRecordsVersions(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeOps()
	p := provision.New(cat, ops)
	seedFleet(t, cat, p, 20)

	ops.mu.Lock()
	ops.version = "v2"
	ops.mu.Unlock()

	report, err := p.MigrateAll(context.Background(), provision.MigrateAllOpts{Batch: 4})
	if err != nil {
		t.Fatal(err)
	}
	if report.Migrated != 20 || report.Failed != 0 {
		t.Fatalf("report = %+v", report)
	}
	got, _ := cat.Get(context.Background(), "t-007")
	if got.Version != "v2" {
		t.Fatalf("version not recorded: %+v", got)
	}
}

func TestMigrateAll_SkipsAlreadyCurrent(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeOps()
	p := provision.New(cat, ops)
	seedFleet(t, cat, p, 5)

	report, err := p.MigrateAll(context.Background(),
		provision.MigrateAllOpts{TargetVersion: "v1"}) // fleet is already at v1
	if err != nil {
		t.Fatal(err)
	}
	if report.Skipped != 5 || report.Migrated != 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestMigrateAll_StopsAtFailureBudget(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeOps()
	p := provision.New(cat, ops)
	seedFleet(t, cat, p, 30)
	ops.mu.Lock()
	ops.failOn["migrate"] = errors.New("boom")
	ops.mu.Unlock()

	report, err := p.MigrateAll(context.Background(),
		provision.MigrateAllOpts{Batch: 1, MaxFailures: 3})
	if fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("err = %v, want failure-budget stop", err)
	}
	if report.Failed < 3 || report.Failed > 4 {
		t.Fatalf("failed = %d, want ~MaxFailures", report.Failed)
	}
	if report.Migrated != 0 {
		t.Fatalf("migrated = %d under total failure", report.Migrated)
	}
}

func TestMigrateAll_SkipsNonActive(t *testing.T) {
	cat := fabriqtest.NewFakeCatalog()
	ops := newFakeOps()
	p := provision.New(cat, ops)
	seedFleet(t, cat, p, 3)
	if _, err := p.Suspend(context.Background(), "t-001"); err != nil {
		t.Fatal(err)
	}

	report, err := p.MigrateAll(context.Background(), provision.MigrateAllOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Migrated != 2 {
		t.Fatalf("migrated = %d, want 2 (suspended tenant skipped)", report.Migrated)
	}
}

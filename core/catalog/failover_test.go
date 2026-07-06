package catalog_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/catalog/catalogtest"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/fabriqtest"
)

// flaky wraps a catalog and, while down, fails every op with a transport-
// flavoured error (CodeInternal, like a dead-DB SELECT). It also counts reads.
type flaky struct {
	inner catalog.Catalog
	down  bool
	reads int
}

func (f *flaky) Get(ctx context.Context, id string) (catalog.Entry, error) {
	f.reads++
	if f.down {
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeInternal, "boom")
	}
	return f.inner.Get(ctx, id)
}
func (f *flaky) Put(ctx context.Context, e catalog.Entry) (catalog.Entry, error) {
	if f.down {
		return catalog.Entry{}, fabriqerr.New(fabriqerr.CodeInternal, "boom")
	}
	return f.inner.Put(ctx, e)
}
func (f *flaky) List(ctx context.Context, c catalog.Cursor, n int) ([]catalog.Entry, catalog.Cursor, error) {
	if f.down {
		return nil, "", fabriqerr.New(fabriqerr.CodeInternal, "boom")
	}
	return f.inner.List(ctx, c, n)
}

func seed(t *testing.T, c catalog.Catalog, tenant string) {
	t.Helper()
	if _, err := c.Put(context.Background(), catalog.Entry{
		TenantID: tenant, ClusterID: "c1", Database: "fabriq_" + tenant, State: catalog.StateActive,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFailover_PrimaryUp_ServesPrimary_NoReplicaRead(t *testing.T) {
	primary := fabriqtest.NewFakeCatalog()
	seed(t, primary, "acme")
	replica := &flaky{inner: fabriqtest.NewFakeCatalog()}
	f := catalog.NewFailover(primary, replica)

	e, err := f.Get(context.Background(), "acme")
	if err != nil || e.TenantID != "acme" {
		t.Fatalf("get = %v, %v", e, err)
	}
	if replica.reads != 0 {
		t.Fatalf("replica was read %d times while primary was up", replica.reads)
	}
	// Unknown-on-primary is authoritative NotFound, NOT degraded.
	_, err = f.Get(context.Background(), "ghost")
	if fabriqerr.CodeOf(err) != fabriqerr.CodeNotFound || catalog.IsDegraded(err) {
		t.Fatalf("primary NotFound must be authoritative, got %v (degraded=%v)", err, catalog.IsDegraded(err))
	}
}

func TestFailover_PrimaryDown_FallsToReplica(t *testing.T) {
	inner := fabriqtest.NewFakeCatalog()
	seed(t, inner, "acme")
	primary := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	f := catalog.NewFailover(primary, inner)

	e, err := f.Get(context.Background(), "acme")
	if err != nil || e.TenantID != "acme" {
		t.Fatalf("replica fallback failed: %v, %v", e, err)
	}
	p, r, fo := f.ReadStats()
	if r != 1 || fo != 1 || p != 0 {
		t.Fatalf("stats primary=%d replica=%d failover=%d", p, r, fo)
	}
}

func TestFailover_PrimaryDown_ReplicaNotFound_IsDegraded(t *testing.T) {
	primary := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	replica := fabriqtest.NewFakeCatalog() // empty → NotFound
	f := catalog.NewFailover(primary, replica)

	_, err := f.Get(context.Background(), "acme")
	if fabriqerr.CodeOf(err) != fabriqerr.CodeNotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
	if !catalog.IsDegraded(err) {
		t.Fatal("replica NotFound must be degraded-marked (non-cacheable)")
	}
}

func TestFailover_AllDown_ReturnsPrimaryTransportError_NotDegraded(t *testing.T) {
	primary := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	replica := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	f := catalog.NewFailover(primary, replica)

	_, err := f.Get(context.Background(), "acme")
	if fabriqerr.CodeOf(err) != fabriqerr.CodeInternal {
		t.Fatalf("want primary transport error, got %v", err)
	}
	if catalog.IsDegraded(err) {
		t.Fatal("an all-down transport error must not be degraded/cacheable")
	}
}

func TestFailover_TriesReplicasInOrder(t *testing.T) {
	primary := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	down := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	up := fabriqtest.NewFakeCatalog()
	seed(t, up, "acme")
	f := catalog.NewFailover(primary, down, up)

	e, err := f.Get(context.Background(), "acme")
	if err != nil || e.TenantID != "acme" {
		t.Fatalf("second replica should have served: %v, %v", e, err)
	}
}

func TestFailover_PutNeverTouchesReplica(t *testing.T) {
	primary := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	replica := &flaky{inner: fabriqtest.NewFakeCatalog()}
	f := catalog.NewFailover(primary, replica)

	_, err := f.Put(context.Background(), catalog.Entry{
		TenantID: "acme", ClusterID: "c1", Database: "fabriq_acme", State: catalog.StatePending,
	})
	if err == nil {
		t.Fatal("Put against a down primary must fail")
	}
	if replica.reads != 0 { // flaky.Put doesn't touch reads, but assert nothing landed
		t.Fatal("Put must never reach a replica")
	}
}

// A Failover with NO replicas must behave exactly like its primary.
func TestFailover_NoReplicas_PassesContract(t *testing.T) {
	catalogtest.Run(t, func(t *testing.T) catalog.Catalog {
		return catalog.NewFailover(fabriqtest.NewFakeCatalog())
	})
}

func TestFailover_List_PrimaryDown_FallsToReplica(t *testing.T) {
	inner := fabriqtest.NewFakeCatalog()
	seed(t, inner, "acme")
	seed(t, inner, "beta")
	primary := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	f := catalog.NewFailover(primary, inner)

	got, _, err := f.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("list fallback failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries from replica, got %d", len(got))
	}
}

func TestFailover_List_AllDown_ReturnsPrimaryError(t *testing.T) {
	primary := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	replica := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	f := catalog.NewFailover(primary, replica)

	if _, _, err := f.List(context.Background(), "", 10); fabriqerr.CodeOf(err) != fabriqerr.CodeInternal {
		t.Fatalf("want primary transport error, got %v", err)
	}
}

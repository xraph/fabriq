package catalog_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/fabriqtest"
)

// A positive read served from a replica (primary unreachable) must carry the
// FromReplica provenance flag, so the routing directory can treat any answer it
// derives from that entry (version gate, not-active) as non-cacheable.
func TestFailover_PrimaryDown_ReplicaEntryMarkedFromReplica(t *testing.T) {
	inner := fabriqtest.NewFakeCatalog()
	seed(t, inner, "acme")
	primary := &flaky{inner: fabriqtest.NewFakeCatalog(), down: true}
	f := catalog.NewFailover(primary, inner)

	e, err := f.Get(context.Background(), "acme")
	if err != nil {
		t.Fatalf("replica fallback failed: %v", err)
	}
	if !e.FromReplica {
		t.Fatal("a replica-served read must be marked FromReplica")
	}
}

// A read served by the primary is authoritative and must NOT be flagged.
func TestFailover_PrimaryUp_EntryNotFromReplica(t *testing.T) {
	primary := fabriqtest.NewFakeCatalog()
	seed(t, primary, "acme")
	f := catalog.NewFailover(primary, fabriqtest.NewFakeCatalog())

	e, err := f.Get(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if e.FromReplica {
		t.Fatal("a primary-served read must not be marked FromReplica")
	}
}

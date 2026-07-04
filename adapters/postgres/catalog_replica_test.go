package postgres

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/fabriqerr"
)

func TestCatalogReplicaStore_PutRefused(t *testing.T) {
	// A replica store constructed without dialing (readOnly true) refuses Put
	// defensively — a hot standby is read-only; writing to it is a bug.
	s := &CatalogStore{readOnly: true}
	_, err := s.Put(context.Background(), catalog.Entry{
		TenantID: "acme", ClusterID: "c1", Database: "fabriq_acme", State: catalog.StatePending,
	})
	if fabriqerr.CodeOf(err) != fabriqerr.CodeUnavailable {
		t.Fatalf("read-only Put: err = %v, want CodeUnavailable", err)
	}
}

package catalog

import (
	"testing"

	"github.com/xraph/fabriq/core/fabriqerr"
)

func TestValidateEntry_AllowsEmptySchema(t *testing.T) {
	e := Entry{TenantID: "acme", ClusterID: "c1", Database: "pool_a", State: StateActive}
	if err := ValidateEntry(e); err != nil {
		t.Fatalf("empty schema should be valid (database mode): %v", err)
	}
}

func TestValidateEntry_AllowsValidSchema(t *testing.T) {
	e := Entry{TenantID: "acme", ClusterID: "c1", Database: "pool_a", State: StateActive, Schema: "tenant_acme"}
	if err := ValidateEntry(e); err != nil {
		t.Fatalf("valid schema rejected: %v", err)
	}
}

func TestValidateEntry_RejectsMalformedSchema(t *testing.T) {
	for _, bad := range []string{"Bad-Schema", "public", "tenant_ACME", "drop table"} {
		e := Entry{TenantID: "acme", ClusterID: "c1", Database: "pool_a", State: StateActive, Schema: bad}
		err := ValidateEntry(e)
		if err == nil {
			t.Fatalf("expected malformed schema %q to be rejected", bad)
		}
		if fabriqerr.CodeOf(err) != fabriqerr.CodeInvalidInput {
			t.Fatalf("schema %q: want CodeInvalidInput, got %v", bad, fabriqerr.CodeOf(err))
		}
	}
}

func TestEntry_ShardIDUnaffectedBySchema(t *testing.T) {
	e := Entry{ClusterID: "c1", Database: "pool_a", Schema: "tenant_acme"}
	if got := e.ShardID(); got != "c1/pool_a" {
		t.Fatalf("ShardID = %q, want c1/pool_a (schema must not change routing key)", got)
	}
}

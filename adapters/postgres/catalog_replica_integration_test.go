//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

// A replica store must NOT create the catalog table. Point it at a fresh DB
// and assert the table is absent after open (a real standby would reject the
// CREATE; here we prove we never issue it).
//
// Note: to_regclass(...) returns SQL NULL when the relation does not exist,
// and fabriqtest.QueryStrings scans into a non-nullable Go string — scanning
// a NULL directly would fail the scan rather than yield "". So the query
// casts the null-check itself to text (`(... IS NULL)::text`), which is
// always non-null ("true"/"false").
func TestCatalogReplicaStore_SkipsEnsureSchema(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)

	s, err := OpenCatalogReplica(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	rows := fabriqtest.QueryStrings(t, dsn,
		`SELECT (to_regclass('public.fabriq_tenant_catalog') IS NULL)::text`)
	if len(rows) != 1 || rows[0] != "true" {
		t.Fatalf("replica open must not create the catalog table, got %q", rows)
	}
}

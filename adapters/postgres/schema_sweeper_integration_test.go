//go:build integration

package postgres_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/pathctx"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
)

// TestSweeper_TwoSchemasOneDB_MaterializesPerSchema proves the schema-mode
// document sweeper: two tenants share ONE consolidation database (isolated by
// schema), each gets a CRDT page, and one maintenance pass per schema
// materializes each tenant's page into ITS OWN schema — the cross-tenant scan
// (crdtDocsRef) is schema-scoped and the per-doc materialization routes via
// search_path. Running the two passes concurrently proves the per-schema
// advisory locks do not serialize tenants sharing a database.
func TestSweeper_TwoSchemasOneDB_MaterializesPerSchema(t *testing.T) {
	superDSN := fabriqtest.StartPostgres(t)
	ctx := context.Background()

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}

	db := databaseOf(t, superDSN)
	ops := postgres.NewClusterOps(map[string]string{"c1": superDSN})
	if err := ops.EnsureBootstrap(ctx, "c1", db, "fabriq_shared"); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	schemas := map[string]string{"acme": "tenant_acme", "beta": "tenant_beta"}
	for _, schema := range schemas {
		if err := ops.CreateSchema(ctx, "c1", db, schema); err != nil {
			t.Fatalf("create schema %s: %v", schema, err)
		}
		if _, err := ops.MigrateSchema(ctx, "c1", db, schema, "fabriq_shared"); err != nil {
			t.Fatalf("migrate schema %s: %v", schema, err)
		}
		// The demo "pages" materialization target is not in the shipped chain.
		applyDDLUnderSchema(t, superDSN, schema, "fabriq_shared", domain.PagesDDL())
	}

	// One adapter serving the consolidation database; search_path per tx.
	a, err := postgres.Open(ctx, superDSN, reg, postgres.WithSharedSchema("fabriq_shared"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = a.Close() }()
	docs := a.Documents()
	maint := postgres.NewMaintenance(a, reg, nil, docs) // pub=nil: materialize only

	// Write one page per tenant, into that tenant's schema.
	docIDs := map[string]string{}
	for tid, schema := range schemas {
		sctx := pathctx.MustWithSearchPath(mustTenant(t, tid), schema)
		docID := "page/" + event.NewID()
		docIDs[tid] = docID
		up := crdtLWWUpdate(t, "pages", docID, "title", tid+"-title", 100, "n1")
		if err := docs.ApplyUpdate(sctx, docID, up); err != nil {
			t.Fatalf("ApplyUpdate %s: %v", tid, err)
		}
	}

	// Backdate updated_at past the 2s quiet window so MaterializeQuiet fires now.
	admin := pgdriver.New()
	if err := admin.Open(ctx, superDSN); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	for _, schema := range schemas {
		if _, err := admin.Exec(ctx,
			`UPDATE `+schema+`.fabriq_crdt_docs SET updated_at = now() - interval '1 hour'`); err != nil {
			t.Fatalf("backdate %s: %v", schema, err)
		}
	}

	// Sweep both tenants CONCURRENTLY (distinct per-schema advisory locks must
	// not serialize them).
	type res struct {
		tid string
		err error
	}
	done := make(chan res, len(schemas))
	for tid, schema := range schemas {
		go func(tid, schema string) {
			sctx := pathctx.MustWithSearchPath(mustTenant(t, tid), schema)
			_, err := maint.Sweep(sctx, true)
			done <- res{tid, err}
		}(tid, schema)
	}
	for range schemas {
		if r := <-done; r.err != nil {
			t.Fatalf("sweep %s: %v", r.tid, r.err)
		}
	}

	// Each tenant's page materialized in ITS schema, and nowhere else.
	for _, schema := range schemas {
		var n int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM `+schema+`.pages`).Scan(&n); err != nil {
			t.Fatalf("count %s.pages: %v", schema, err)
		}
		if n != 1 {
			t.Fatalf("%s.pages has %d rows, want 1 (materialization did not route to the schema)", schema, n)
		}
	}
	// Cross-check: acme's doc id does not appear in beta's schema.
	var leak int
	if err := admin.QueryRow(ctx,
		`SELECT count(*) FROM tenant_beta.fabriq_crdt_docs WHERE doc_id = $1`, docIDs["acme"]).Scan(&leak); err != nil {
		t.Fatal(err)
	}
	if leak != 0 {
		t.Fatalf("acme doc leaked into beta schema (%d)", leak)
	}
}

// applyDDLUnderSchema runs DDL against a connection whose search_path is the
// given schema (so bare CREATE TABLE lands there).
func applyDDLUnderSchema(t testing.TB, superDSN, schema, shared string, stmts []string) {
	t.Helper()
	u, err := url.Parse(superDSN)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("options", "-c search_path="+schema+","+shared)
	u.RawQuery = q.Encode()

	db := pgdriver.New()
	if err := db.Open(context.Background(), u.String()); err != nil {
		t.Fatalf("open under schema %s: %v", schema, err)
	}
	defer func() { _ = db.Close() }()
	for _, s := range stmts {
		if _, err := db.Exec(context.Background(), s); err != nil {
			t.Fatalf("apply DDL under %s: %v\n%s", schema, err, s)
		}
	}
}

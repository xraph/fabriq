//go:build integration

package postgres_test

// The Postgres control store must pass the exact contract the fake passes
// (core/catalog/catalogtest) — drift between them is a failing test.

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/catalog/catalogtest"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestCatalogStore_Contract(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	store, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	catalogtest.Run(t, func(t *testing.T) catalog.Catalog {
		// Fresh catalog per contract case: same container, truncated table.
		fabriqtest.ApplyDDL(t, dsn, []string{`TRUNCATE fabriq_tenant_catalog`})
		return store
	})
}

// BenchmarkCatalog_List10k is the sweeper's full-scan cost: page-walking
// 10k entries. Spec target: < 50 ms against local Postgres.
func BenchmarkCatalog_List10k(b *testing.B) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(b)
	store, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	const n = 10_000
	stmts := make([]string, 0, 11)
	for batch := 0; batch < 10; batch++ {
		stmt := "INSERT INTO fabriq_tenant_catalog (tenant_id, cluster_id, db_name, state, version, updated_at) VALUES "
		for i := 0; i < n/10; i++ {
			id := fmt.Sprintf("t-%05d", batch*(n/10)+i)
			if i > 0 {
				stmt += ","
			}
			stmt += fmt.Sprintf("('%s','c1','fabriq_%s','active','1',clock_timestamp())", id, id)
		}
		stmts = append(stmts, stmt)
	}
	fabriqtest.ApplyDDL(b, dsn, stmts)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		total := 0
		cursor := catalog.Cursor("")
		for {
			page, next, err := store.List(ctx, cursor, 500)
			if err != nil {
				b.Fatal(err)
			}
			total += len(page)
			if next == "" {
				break
			}
			cursor = next
		}
		if total != n {
			b.Fatalf("listed %d, want %d", total, n)
		}
	}
}

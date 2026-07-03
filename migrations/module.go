// Package migrations is fabriq's DDL authority: grove Go-code migrations
// for every fabriq-owned table. The registry never generates DDL; the
// registry-conformance integration test keeps the two in sync.
//
// Run them with `fabriq migrate up|down|status` (which wraps grove's
// orchestrator under its advisory migration lock) — never at app startup.
// Expand/contract discipline is documented in docs/MIGRATIONS.md.
package migrations

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/grove/migrate"

	// Register the Postgres migration executor factory.
	_ "github.com/xraph/grove/drivers/pgdriver/pgmigrate"
)

// GroupName identifies fabriq's migration group; host applications that
// embed fabriq alongside their own grove migration groups can depend on it
// (migrate.DependsOn(migrations.GroupName)).
const GroupName = "fabriq"

var (
	groupOnce sync.Once
	group     *migrate.Group
)

// Group returns fabriq's migration group with all migrations registered,
// in version order.
func Group() *migrate.Group {
	groupOnce.Do(func() {
		group = migrate.NewGroup(GroupName)
		group.MustRegister(
			migration0001Outbox,
			migration0002ProjectionState,
			migration0003SiteAssetTag,
			migration0004RLSPolicies,
			migration0005Timescale,
			migration0006PGVector,
			migration0007CRDTUpdates,
			migration0008CRDTDocs,
			migration0009Link,
			migration0010LiveSubscriptions,
			migration0011PostGIS,
			migration0012Scope,
			migration0013CRDTScope,
			migration0014Blob,
			migration0015BlobCASCreatedAt,
			migration0016FsNode,
			migration0017FsNodeMount,
			migration0018FsPermission,
			migration0019FsShare,
			migration0020FsBookmark,
			migration0021BlobSource,
			migration0022DigestNode,
			migration0023DigestTokens,
			migration0024DigestContentHashIdx,
			migration0025EmbeddingsMetaGIN,
			migration0026CRDTSegments,
			migration0027APIKey,
			migration0028APIKeyExpiresAt,
			migration0029FsNodeDropPath,
			migration0030NamespaceRename,
		)
	})
	return group
}

// NewOrchestrator builds a grove migration orchestrator for the given
// driver (a *pgdriver.PgDB). The orchestrator acquires grove's migration
// lock on a dedicated connection, so concurrent `fabriq migrate` runs are
// safe.
func NewOrchestrator(drv any) (*migrate.Orchestrator, error) {
	exec, err := migrate.NewExecutorFor(drv)
	if err != nil {
		return nil, fmt.Errorf("fabriq: no migration executor for driver %T: %w", drv, err)
	}
	return migrate.NewOrchestrator(exec, Group()), nil
}

// execAll runs statements in order, failing on the first error.
func execAll(ctx context.Context, exec migrate.Executor, stmts []string) error {
	for _, stmt := range stmts {
		if _, err := exec.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migration statement failed: %w\n%s", err, stmt)
		}
	}
	return nil
}

// extensionAvailable reports whether a Postgres extension can be installed
// in this database (pg_available_extensions).
func extensionAvailable(ctx context.Context, exec migrate.Executor, name string) (bool, error) {
	rows, err := exec.Query(ctx, `SELECT count(*) FROM pg_available_extensions WHERE name = $1`, name)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return false, err
		}
	}
	return n > 0, rows.Err()
}

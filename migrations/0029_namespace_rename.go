package migrations

import (
	"context"
	"fmt"

	"github.com/xraph/grove/migrate"
)

// namespaceRenames maps the previously-unprefixed infra tables to their
// fabriq_* names (spec 2026-07-03 db-per-tenant, Phase 1): fabriq's tables
// must be coexistence-safe inside a database shared with a host application
// and other Forge extensions, which namespace their tables by convention.
//
// Postgres carries indexes, constraints, RLS enablement and policies along
// with a table rename, so isolation is untouched (pinned by the namespace
// integration tests). Index NAMES keep their historical prefixes — they are
// never referenced by code and renaming them buys nothing.
//
// Historical migrations (0009–0024) still CREATE the old names on fresh
// databases and this migration renames them at the end of the chain — one
// code path for fresh and deployed databases alike (append-only history;
// no edited DDL behind already-recorded versions).
var namespaceRenames = [][2]string{
	{"links", "fabriq_links"},
	{"blob_objects", "fabriq_blob_objects"},
	{"blob_cas", "fabriq_blob_cas"},
	{"fs_nodes", "fabriq_fs_nodes"},
	{"fs_permissions", "fabriq_fs_permissions"},
	{"fs_shares", "fabriq_fs_shares"},
	{"fs_bookmarks", "fabriq_fs_bookmarks"},
	{"blob_sources", "fabriq_blob_sources"},
	{"digest_nodes", "fabriq_digest_nodes"},
}

var migration0029NamespaceRename = &migrate.Migration{
	Name:    "namespace_rename",
	Version: "202607030029",
	Comment: "rename unprefixed infra tables to fabriq_* (host-database coexistence)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		for _, r := range namespaceRenames {
			if err := execAll(ctx, exec, []string{
				fmt.Sprintf(`ALTER TABLE IF EXISTS %s RENAME TO %s`, r[0], r[1]),
			}); err != nil {
				return err
			}
		}
		return nil
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		for i := len(namespaceRenames) - 1; i >= 0; i-- {
			r := namespaceRenames[i]
			if err := execAll(ctx, exec, []string{
				fmt.Sprintf(`ALTER TABLE IF EXISTS %s RENAME TO %s`, r[1], r[0]),
			}); err != nil {
				return err
			}
		}
		return nil
	},
}

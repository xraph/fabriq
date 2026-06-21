package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// migration0024DigestContentHashIdx indexes digest_nodes by (tenant_id,
// content_hash) so exact-source dedup can find an existing summary for an
// identical source row without a scan.
var migration0024DigestContentHashIdx = &migrate.Migration{
	Name:    "digest_content_hash_idx",
	Version: "202606210024",
	Comment: "index digest_nodes (tenant_id, content_hash) for exact-source dedup",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE INDEX IF NOT EXISTS digest_nodes_content_hash_idx ON digest_nodes (tenant_id, content_hash)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`DROP INDEX IF EXISTS digest_nodes_content_hash_idx`})
	},
}

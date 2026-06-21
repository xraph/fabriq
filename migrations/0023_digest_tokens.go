package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// migration0023DigestTokens adds digest_nodes.tokens: the cached token count of
// a node's summary, so adaptive-depth fan-in checks read it from the row instead
// of re-reading the summary from CAS (which would undo the Merkle short-circuit).
var migration0023DigestTokens = &migrate.Migration{
	Name:    "digest_tokens",
	Version: "202606210023",
	Comment: "digest_nodes.tokens (cached summary token count for adaptive depth)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`ALTER TABLE digest_nodes ADD COLUMN IF NOT EXISTS tokens BIGINT NOT NULL DEFAULT 0`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`ALTER TABLE digest_nodes DROP COLUMN IF EXISTS tokens`})
	},
}

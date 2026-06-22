package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// migration0025EmbeddingsMetaGIN indexes fabriq_embeddings.meta with a GIN index
// so metadata-containment filters (meta @> ...) on Similar and DeleteByMeta are
// efficient.
var migration0025EmbeddingsMetaGIN = &migrate.Migration{
	Name:    "embeddings_meta_gin",
	Version: "202606220025",
	Comment: "GIN index on fabriq_embeddings.meta for metadata-filtered vector search/delete",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE INDEX IF NOT EXISTS fabriq_embeddings_meta_gin ON fabriq_embeddings USING gin (meta)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{`DROP INDEX IF EXISTS fabriq_embeddings_meta_gin`})
	},
}

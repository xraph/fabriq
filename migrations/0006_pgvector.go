package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_embeddings stores one embedding per (tenant, entity, id) with an
// HNSW cosine index. Grove migration executors run statements outside an
// explicit transaction (autocommit), so index builds here are safe; a
// future CONCURRENTLY rebuild on a large table also works in this stream.
// EmbeddingDim is fixed at 768 for v1 (text-embedding-class models);
// changing it is an expand/contract migration.
var migration0006PGVector = &migrate.Migration{
	Name:    "pgvector",
	Version: "202606120006",
	Comment: "embeddings table + HNSW cosine index",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		available, err := extensionAvailable(ctx, exec, "vector")
		if err != nil {
			return err
		}
		if !available {
			return nil
		}
		return execAll(ctx, exec, []string{
			`CREATE EXTENSION IF NOT EXISTS vector`,
			`CREATE TABLE IF NOT EXISTS fabriq_embeddings (
				tenant_id TEXT NOT NULL,
				entity    TEXT NOT NULL,
				id        TEXT NOT NULL,
				embedding vector(768) NOT NULL,
				meta      JSONB NOT NULL DEFAULT '{}'::jsonb,
				PRIMARY KEY (tenant_id, entity, id)
			)`,
			`ALTER TABLE fabriq_embeddings ENABLE ROW LEVEL SECURITY`,
			`ALTER TABLE fabriq_embeddings FORCE ROW LEVEL SECURITY`,
			`DROP POLICY IF EXISTS tenant_isolation ON fabriq_embeddings`,
			`CREATE POLICY tenant_isolation ON fabriq_embeddings
				USING (tenant_id = current_setting('app.tenant_id', true))
				WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`,
			`CREATE INDEX IF NOT EXISTS fabriq_embeddings_hnsw_idx
				ON fabriq_embeddings USING hnsw (embedding vector_cosine_ops)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_embeddings`,
		})
	},
}

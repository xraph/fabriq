package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_api_key holds hosted-fabriq API keys: callers authenticate with a
// bearer key before any tenant context is established, so the key store is
// looked up by hash directly (like the outbox, one row per key, no tenant
// scoping on the lookup path itself).
//
// Deliberately NOT under RLS: this is an instance-global bookkeeping table,
// resolved before a tenant is known, mirroring fabriq_outbox's rationale.
// tenant_id is nullable — NULL means the key is multi-tenant (callable
// against any tenant), non-NULL scopes the key to one tenant.
var migration0027APIKey = &migrate.Migration{
	Name:    "api_key",
	Version: "202607010027",
	Comment: "hosted-fabriq API keys (per-tenant + multi-tenant)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE TABLE IF NOT EXISTS fabriq_api_key (
				id              TEXT PRIMARY KEY,
				prefix          TEXT NOT NULL,
				key_hash        TEXT NOT NULL UNIQUE,
				tenant_id       TEXT,
				label           TEXT NOT NULL DEFAULT '',
				can_manage_keys BOOLEAN NOT NULL DEFAULT FALSE,
				created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
				revoked_at      TIMESTAMPTZ
			)`,
			`CREATE INDEX IF NOT EXISTS fabriq_api_key_hash_idx ON fabriq_api_key (key_hash)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_api_key`,
		})
	},
}

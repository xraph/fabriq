package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_dynamic_entity persists runtime-defined (dynamic) entity
// descriptors so a hosted server can re-register them after a restart.
//
// Deliberately NOT under RLS: descriptors are instance-global schema, not
// tenant data, and the server reads them at boot before any tenant exists.
// This mirrors fabriq_api_key (0027). revision starts at 1 and moves only
// when the stored spec changes, so callers can tell a real change from a
// repeated save.
var migration0033DynamicEntity = &migrate.Migration{
	Name:    "dynamic_entity",
	Version: "202610210033",
	Comment: "persisted dynamic entity descriptors (instance-global, no RLS)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE TABLE IF NOT EXISTS fabriq_dynamic_entity (
				name       TEXT PRIMARY KEY,
				spec       JSONB NOT NULL,
				revision   BIGINT NOT NULL DEFAULT 1,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_dynamic_entity`,
		})
	},
}

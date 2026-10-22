package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_api_key.last_used_at records when a key last authenticated a
// request, so an operator can tell a live key from a forgotten one. The
// authn middleware writes it at most once a minute per key, so it is accurate
// to about a minute. NULL means the key has not been used since the column
// was added.
var migration0035APIKeyLastUsed = &migrate.Migration{
	Name:    "api_key_last_used",
	Version: "202610210035",
	Comment: "last_used_at on fabriq_api_key (key last-use reporting)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`ALTER TABLE fabriq_api_key ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`ALTER TABLE fabriq_api_key DROP COLUMN IF EXISTS last_used_at`,
		})
	},
}

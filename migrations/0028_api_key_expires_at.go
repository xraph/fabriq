package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_api_key.expires_at supports session tokens: a session is an
// expiring key row, resolved and expiry-checked by the same authn middleware
// that already handles revocation. NULL means the key never expires (the
// existing behaviour for keys issued before this column existed, and for
// long-lived API keys issued without a TTL).
var migration0028APIKeyExpiresAt = &migrate.Migration{
	Name:    "api_key_expires_at",
	Version: "202607020028",
	Comment: "expires_at on fabriq_api_key (session-token support)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`ALTER TABLE fabriq_api_key ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`ALTER TABLE fabriq_api_key DROP COLUMN IF EXISTS expires_at`,
		})
	},
}

package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_outbox is the transactional outbox: one row per domain event,
// appended in the same transaction as the aggregate write. The relay polls
// unpublished rows (FOR UPDATE SKIP LOCKED) and is woken by NOTIFY.
//
// Deliberately NOT under RLS: the relay is a worker-plane component that
// reads across tenants by design; application code never queries this
// table (the hook backstop knows fabriq_* bookkeeping tables are off
// limits to the relational port).
var migration0001Outbox = &migrate.Migration{
	Name:    "outbox",
	Version: "202606120001",
	Comment: "transactional outbox + unpublished partial index",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE TABLE IF NOT EXISTS fabriq_outbox (
				id                     TEXT PRIMARY KEY,
				tenant_id              TEXT NOT NULL,
				aggregate              TEXT NOT NULL,
				agg_id                 TEXT NOT NULL,
				version                BIGINT NOT NULL,
				type                   TEXT NOT NULL,
				at                     TIMESTAMPTZ NOT NULL DEFAULT now(),
				payload_schema_version INT NOT NULL DEFAULT 1,
				payload                JSONB NOT NULL DEFAULT '{}'::jsonb,
				traceparent            TEXT NOT NULL DEFAULT '',
				published_at           TIMESTAMPTZ,
				stream_id              TEXT NOT NULL DEFAULT '',
				CONSTRAINT fabriq_outbox_one_event_per_version UNIQUE (tenant_id, aggregate, agg_id, version)
			)`,
			// The relay's scan: unpublished rows in ULID (= commit intent) order.
			`CREATE INDEX IF NOT EXISTS fabriq_outbox_unpublished_idx
				ON fabriq_outbox (id) WHERE published_at IS NULL`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_outbox`,
		})
	},
}

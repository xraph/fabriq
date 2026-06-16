package migrations

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// fabriq_live_subscriptions is the durable registry of maintained/streamed live
// query subscriptions — the backbone of the sharded live query tier. When a
// matcher shard is (re)assigned a data partition (after a node failure or a
// rebalance), it rebuilds the subscriptions it now owns by querying this table
// and re-snapshotting them, so a client's live query survives a server failover
// transparently (it receives an OpReset and re-renders).
//
// WORKER-PLANE table like fabriq_outbox / fabriq_crdt_docs: NO RLS. The matcher
// tier scans across tenants to route and recover subscriptions; it holds no
// user content (only the query descriptor), and structural tenancy
// (tenant_id + entity, which form the partition key) is enforced by the
// routing/snapshot path, which is itself tenant-stamped.
var migration0010LiveSubscriptions = &migrate.Migration{
	Name:    "live_subscriptions",
	Version: "202606150010",
	Comment: "durable registry of live query subscriptions (sharded-tier failover backbone)",
	Up: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`CREATE TABLE IF NOT EXISTS fabriq_live_subscriptions (
				sub_id     TEXT PRIMARY KEY,
				tenant_id  TEXT NOT NULL,
				entity     TEXT NOT NULL,
				mode       INT NOT NULL DEFAULT 0,
				query      JSONB NOT NULL,
				gateway_id TEXT NOT NULL DEFAULT '',
				watermark  TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			// Partition lookup: a reassigned shard loads everything it now owns.
			`CREATE INDEX IF NOT EXISTS fabriq_live_subscriptions_partition_idx
				ON fabriq_live_subscriptions (tenant_id, entity)`,
			// Gateway recovery: a restarted gateway reclaims its connections.
			`CREATE INDEX IF NOT EXISTS fabriq_live_subscriptions_gateway_idx
				ON fabriq_live_subscriptions (gateway_id)`,
		})
	},
	Down: func(ctx context.Context, exec migrate.Executor) error {
		return execAll(ctx, exec, []string{
			`DROP TABLE IF EXISTS fabriq_live_subscriptions`,
		})
	},
}

package insights

import (
	"context"
	"time"

	"github.com/xraph/fabriq/core/registry"
)

// RollupSurface is the per-tenant Postgres rollup-maintenance capability the
// leader-elected rollup:insights maintainer job (forgeext) needs:
//
//   - EnsureRollupTable creates (idempotently) a materialized metric's rollup
//     table via the schema-owner DDL path.
//   - MaintainRollup runs one seal-aggregate-advance-watermark pass for a
//     materialized metric under the caller's (unscoped) tenant ctx.
//
// Implemented by *postgres.InsightsAdapter (a thin pass-through to the
// underlying *postgres.Adapter), resolved per tenant through the same
// shard/router seam FactSink uses (Shard.Analytics) — so a maintainer pass
// always lands on the tenant's own shard (static sharding) or tenant
// database (catalog / db-per-tenant mode), exactly like proj:insights'
// per-event writes.
type RollupSurface interface {
	EnsureRollupTable(ctx context.Context, m *registry.MetricSpec) error
	MaintainRollup(ctx context.Context, m *registry.MetricSpec, now time.Time) error
}

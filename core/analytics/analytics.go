// Package analytics is fabriq's cross-tenant analytics sink: a denormalized
// read model fed by the shared event stream so operators can run fleet-wide
// reporting without touching per-tenant databases. It is the ONE place
// tenant data is deliberately co-located; see the Sink docs and ADR.
package analytics

import (
	"context"
	"encoding/json"
	"time"
)

// Fact is the latest denormalized state of one aggregate instance. The sink
// keeps exactly one Fact per (TenantID, Aggregate, AggID), version-gated.
type Fact struct {
	TenantID  string          `json:"tenantId"`
	Aggregate string          `json:"aggregate"`
	AggID     string          `json:"aggId"`
	Version   int64           `json:"version"`
	Payload   json.RawMessage `json:"payload"` // already redacted by the applier
	At        time.Time       `json:"at"`
	Deleted   bool            `json:"deleted"`
}

// Event is one append-only history record. The sink dedupes on
// (TenantID, Aggregate, AggID, Version).
type Event struct {
	TenantID  string          `json:"tenantId"`
	Aggregate string          `json:"aggregate"`
	AggID     string          `json:"aggId"`
	Version   int64           `json:"version"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"` // already redacted by the applier
	At        time.Time       `json:"at"`
}

// Watermark records the highest applied version per aggregate instance, for
// lag metrics and resumable backfill.
type Watermark struct {
	TenantID  string `json:"tenantId"`
	Aggregate string `json:"aggregate"`
	AggID     string `json:"aggId"`
	Version   int64  `json:"version"`
}

// Sink persists analytics records. All methods are idempotent under
// at-least-once delivery: UpsertFacts version-gates (older/equal versions
// are no-ops), AppendEvents dedupes on the natural key, SetWatermark
// advances monotonically. Implemented by adapters/pganalytics and
// fabriqtest.FakeAnalyticsSink (one conformance suite gates both).
type Sink interface {
	UpsertFacts(ctx context.Context, facts []Fact) error
	AppendEvents(ctx context.Context, events []Event) error
	Watermark(ctx context.Context, tenantID, aggregate, aggID string) (int64, error)
	SetWatermark(ctx context.Context, ws []Watermark) error
	// AllWatermarks returns every applied watermark for a tenant in one read —
	// the reconciler compares these against the source's current versions to
	// find drift (facts a skipped event left missing or stale) without a
	// per-aggregate round-trip.
	AllWatermarks(ctx context.Context, tenantID string) ([]Watermark, error)
	// LagByTenant reports per-tenant read-model freshness: for each tenant with
	// at least one fact, now() - (that tenant's newest fact commit time), in
	// seconds. An empty map means the sink holds no facts. Per-tenant (rather
	// than one fleet-wide number) so a single stalled tenant cannot hide behind
	// others still flowing; the poller derives the worst-case gauge and the
	// tenants-behind count from it without emitting a per-tenant metric series.
	LagByTenant(ctx context.Context) (map[string]float64, error)
	// ReprojectTenant re-writes stored fact AND event payloads for a tenant in
	// place, applying transform to each. aggregate "" means every aggregate.
	// It returns the number of rows whose payload actually changed. Used to
	// retroactively apply a tightened redaction allow-list to already-stored
	// data (a privacy correction): the transform re-projects each stored payload
	// through the entity's current AnalyticsSpec. Idempotent — a second run with
	// the same transform rewrites nothing.
	ReprojectTenant(ctx context.Context, tenantID, aggregate string, transform func(payload json.RawMessage) (json.RawMessage, error)) (rowsRewritten int64, err error)
	// PruneEvents deletes append-only history events committed strictly before
	// olderThan, across all tenants, and returns the count removed. It is the
	// retention control for the unbounded event log; facts (latest state) are
	// never pruned — only the history. Idempotent.
	PruneEvents(ctx context.Context, olderThan time.Time) (rowsDeleted int64, err error)
	// MaintainPartitions is the partition-management hook for sinks that
	// range-partition the event log: it creates upcoming partitions and, when
	// retention > 0, drops partitions entirely older than retention (instant
	// reclaim). Sinks that do not partition return (0, 0, nil).
	MaintainPartitions(ctx context.Context, retention time.Duration) (created, dropped int, err error)
	// PurgeTenant hard-deletes ALL of one tenant's co-located data — facts,
	// events, and watermarks — and returns the number of rows removed. It is the
	// erasure primitive for tenant offboarding and right-to-be-forgotten
	// requests: the ONE deliberately destructive operation on the sink, and the
	// only supported way to remove a tenant from the shared store. Idempotent
	// (purging an absent tenant deletes nothing and returns 0). Never invoked
	// automatically — an explicit operator step, like dropping a tenant database.
	PurgeTenant(ctx context.Context, tenantID string) (rowsDeleted int64, err error)
	Close() error
}

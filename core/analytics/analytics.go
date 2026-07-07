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
	// LagSeconds reports read-model freshness as now() - (newest fact's commit
	// time), in seconds. hasData is false when the sink holds no facts yet
	// (nothing to be stale). Publishes the fabriq_analytics_lag_seconds gauge.
	LagSeconds(ctx context.Context) (seconds float64, hasData bool, err error)
	Close() error
}

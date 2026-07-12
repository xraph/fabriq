package query

import (
	"context"
	"time"
)

// AnalyticsQuerier is the per-tenant, customer-facing analytics port. Unlike
// the operator analytics sink (core/analytics), this is scoped to ONE tenant on
// ctx and RLS-enforced: it reads and writes only the tenant's own in-tenant
// analytics store. Exposed via Fabric.Analytics(); notConfiguredAnalytics{}
// degrades every method to ErrStoreNotConfigured.
type AnalyticsQuerier interface {
	// Track ingests customer-defined analytics events in bulk. Like
	// TSQuerier.BulkWrite it bypasses the command/outbox plane — fire-and-forget
	// at volume. Events land in the tenant's fabriq_insights_events table.
	Track(ctx context.Context, events []AnalyticsEvent) error

	// Query runs a structured cube aggregation (measures × dimensions ×
	// time-bucket, filtered) over the unified store and scans grouped rows into
	// into (a *[]T of a struct/map). On-demand in phase 1.
	Query(ctx context.Context, q AnalyticsQuery, into any) error

	// QueryRaw is the read-only SQL escape hatch for aggregations the cube can't
	// express. RLS-scoped and in-tenant; a data-modifying statement errors at
	// the database (read-only tx) and is rejected by a precheck.
	QueryRaw(ctx context.Context, into any, sql string, args ...any) error
}

// AnalyticsEvent is one customer-defined analytics event. Props are arbitrary
// dimensions/measures persisted as JSONB; Name and At are first-class columns.
type AnalyticsEvent struct {
	Name     string         `json:"name"`
	At       time.Time      `json:"at"`
	Props    map[string]any `json:"props,omitempty"`
	DedupKey string         `json:"dedupKey,omitempty"` // optional idempotency key
}

// MeasureKind is the aggregation a Measure computes.
type MeasureKind string

const (
	MeasureCount         MeasureKind = "count"          // COUNT(*)
	MeasureSum           MeasureKind = "sum"            // SUM(field)
	MeasureAvg           MeasureKind = "avg"            // AVG(field)
	MeasureMin           MeasureKind = "min"            // MIN(field)
	MeasureMax           MeasureKind = "max"            // MAX(field)
	MeasureCountDistinct MeasureKind = "count_distinct" // COUNT(DISTINCT field)
)

// Measure is one aggregated output column. Field is ignored for MeasureCount.
// As names the output column (defaults to Kind or Kind_Field when empty).
type Measure struct {
	Kind  MeasureKind `json:"kind"`
	Field string      `json:"field,omitempty"`
	As    string      `json:"as,omitempty"`
}

// AnalyticsQuery is an engine-neutral cube aggregation — the sibling of
// ListQuery/SearchQuery/RangeQuery. The (Measures, Dimensions, TimeBucket)
// triple is exactly what a phase-2 rollup is keyed on, so materialization needs
// no change here.
type AnalyticsQuery struct {
	// Source is the event Name (for customer events), a declared metric name,
	// or an opt-in entity name (for projected facts).
	Source string `json:"source"`
	// Measures are the aggregated output columns. At least one is required.
	Measures []Measure `json:"measures"`
	// Dimensions are group-by keys: a top-level prop key (events) or a fact
	// column. Empty means a single grand-total row.
	Dimensions []string `json:"dimensions,omitempty"`
	// TimeBucket, when > 0, groups by time_bucket(TimeBucket, at) and adds a
	// "bucket" column to the output.
	TimeBucket time.Duration `json:"timeBucket,omitempty"`
	// Filter narrows rows before aggregation, reusing the relational Where
	// vocabulary (Eq/In/Gt/Or/…). Columns are validated against known
	// dimensions/props.
	Filter Where `json:"filter,omitempty"`
	// Having filters aggregated rows by measure output (post-aggregation).
	Having Where `json:"having,omitempty"`
	// From/To bound the event time window (inclusive/exclusive). Zero = unbounded.
	From time.Time `json:"from,omitempty"`
	To   time.Time `json:"to,omitempty"`
	// OrderBy is a comma-separated "col [ASC|DESC]" over dimension or measure
	// output columns. Empty orders by the first dimension, then bucket.
	OrderBy string `json:"orderBy,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	Offset  int    `json:"offset,omitempty"`
}

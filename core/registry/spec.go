// Package registry is fabriq's declarative schema registry. Entities are
// described once as EntitySpecs — relational shape via a grove-tagged model,
// fabric-only concerns (graph mapping, search mapping, subscription scopes,
// CRDT plane) layered on top — and everything else is derived: projection
// mappings, channel names, tenant-scoped store names, conformance checks.
//
// The registry never generates DDL; grove migrations remain the schema
// authority and the registry-conformance test is the bridge.
package registry

import "time"

// ColumnType is the neutral column type set for dynamic entities; adapters map
// it to engine SQL types.
type ColumnType int

const (
	ColText ColumnType = iota
	ColInt
	ColFloat
	ColBool
	ColTime
	ColJSON
)

// DynamicColumn is one domain column of a runtime-defined entity.
type DynamicColumn struct {
	Name    string
	Type    ColumnType
	NotNull bool
	// Default is an optional SQL default EXPRESSION (e.g. "now()", "'pending'",
	// "0"). It is interpolated verbatim into DDL and is intentionally NOT
	// identifier-validated (it is an expression, not an identifier), so it must
	// be a trusted, control-plane value — never a user-supplied string. Same
	// trust level as hand-written migration SQL.
	Default string
}

// DynamicIndex is an optional secondary index on a dynamic entity.
type DynamicIndex struct {
	Name    string
	Columns []string
	Unique  bool
}

// DynamicSchema describes an entity defined at runtime instead of by a Go
// Model. Mutually exclusive with EntitySpec.Model. fabriq injects the
// structural columns (id, tenant_id, version); declare only domain columns.
type DynamicSchema struct {
	Table   string
	Columns []DynamicColumn
	Indexes []DynamicIndex

	// NoTypeCheck opts the entire entity out of application-level payload type
	// validation on the command plane. Column values are then passed through
	// unchanged and only the database column types enforce shape. Zero value
	// (false) = validate.
	NoTypeCheck bool
}

// Kind classifies how an entity is written.
type Kind int

const (
	// KindAggregate entities are written exclusively through the command
	// plane: one transactional write, one versioned outbox event.
	KindAggregate Kind = iota

	// KindDocument entities are collaborative CRDT documents: updates land
	// in the append-only document plane and are periodically materialized
	// into an ordinary versioned domain event. The plane's implementation
	// is deferred; the seam exists from phase 1.
	KindDocument
)

func (k Kind) String() string {
	switch k {
	case KindAggregate:
		return "aggregate"
	case KindDocument:
		return "document"
	default:
		return "unknown"
	}
}

// Scope names a subscription dimension. Channels are always resolved
// server-side from (tenant, scope, id) — clients never name channels.
type Scope struct {
	// Name appears in channel names: changes:{tenant}:{name}:{id}.
	Name string

	// Field is the model column whose value provides the channel id for
	// containing-scope channels (e.g. "site_id" for a by-site scope).
	// Empty for the ByID and ByTenant builtins.
	Field string
}

// ByID scopes deltas to a single aggregate: changes:{tenant}:id:{aggID}.
var ByID = Scope{Name: "id"}

// ByTenant scopes deltas to everything in the tenant.
var ByTenant = Scope{Name: "tenant"}

// ByField declares a containing scope whose channel id comes from the named
// column, e.g. ByField("site", "site_id").
func ByField(name, field string) Scope { return Scope{Name: name, Field: field} }

// EdgeSpec maps a foreign-key column to a graph relationship.
type EdgeSpec struct {
	Field  string // FK column on this entity's table
	Rel    string // relationship type, e.g. "LOCATED_AT"
	Target string // registry name of the target entity
}

// SearchSpec maps an entity into the search projection. The zero value
// (empty Index) means the entity is not indexed.
type SearchSpec struct {
	Index  string   // logical index base name; tenant routing is derived
	Fields []string // columns included in the indexed document
}

// CRDTSpec configures the document plane for KindDocument entities. The
// merge engine comes from grove's crdt packages — referenced, not
// reimplemented.
type CRDTSpec struct {
	Engine        string        // engine reference, e.g. "grove-crdt"
	SnapshotEvery int           // compact after this many updates
	QuietWindow   time.Duration // idle window before materialization

	// ArchiveHistory opts this entity's documents into offloading sealed
	// update history to the blob plane on Compact (latest state stays in the
	// DB). nil = inherit the global Config.Documents.ArchiveHistory default;
	// a non-nil pointer overrides it per entity.
	ArchiveHistory *bool
}

// GraphEdgeSpec maps a reified-edge ENTITY (rows that ARE relationships) into
// the graph. Endpoints are matched by id under their identity labels; the rel
// type comes from a column value. General: reified relationships (membership,
// grant, subscription) are a common pattern, not specific to any domain.
type GraphEdgeSpec struct {
	TypeField   string
	SourceField string
	TargetField string
	SourceLabel string
	TargetLabel string
	PropFields  []string
}

// EntitySpec declares one entity. Model must be a grove-tagged struct
// pointer such as (*domain.Asset)(nil); its table and columns are bound at
// registration.
type EntitySpec struct {
	Name      string
	Kind      Kind
	Model     any
	GraphNode string         // graph label; empty = not projected to the graph
	GraphEdge *GraphEdgeSpec // when set, the entity projects as a relationship
	Edges     []EdgeSpec
	Search    SearchSpec
	Subscribe []Scope
	CRDT      *CRDTSpec

	// Schema declares a runtime-defined ("dynamic") entity instead of Model.
	// Exactly one of Model or Schema must be set.
	Schema *DynamicSchema

	// Validate, when set, runs after structural validation on every
	// create/update/upsert with the column-keyed payload. Fabriq attaches
	// no meaning to the values; consumers enforce their own invariants
	// (enum membership, checksums, cross-field rules).
	Validate func(vals map[string]any) error

	// Live opts the entity into the maintained-result-set live query engine.
	// Nil (the zero value) means live queries are disabled for this entity.
	Live *LiveSpec

	// Cache opts the entity into the read-through row cache. Nil = not cached.
	Cache *CacheSpec

	// Embed opts the entity into vector embedding (auto-indexing). Nil = not embedded.
	Embed *EmbedSpec

	// Distill opts the entity into context distillation: each row gets an
	// L0 digest summary; declared Scopes form L1 backbone nodes. Nil = not
	// distilled. The distillation layer supplies the Summarizer/Guard.
	Distill *DistillSpec

	// Analytics opts the entity into the cross-tenant analytics sink. Nil
	// (the zero value) means the entity is NEVER co-located in the shared
	// analytics store — deny-by-default. When set, only the declared fields
	// (or the whole payload if IncludeAll) cross the trust boundary.
	Analytics *AnalyticsSpec

	// Insights opts the entity into the per-tenant customer-facing analytics
	// store (projected facts). Nil = not projected. Distinct from Analytics
	// (the cross-tenant operator sink) — Insights stays inside the tenant DB.
	Insights *InsightsSpec

	// Metrics declares named, typed cube queries for this entity's Source.
	Metrics []MetricSpec
}

// LiveSpec opts an entity into the live query engine (nil = disabled).
// Filterable/Sortable default to all columns when empty; columns are validated
// against the model at registration.
type LiveSpec struct {
	Filterable []string // columns allowed in Where (empty = all)
	Sortable   []string // columns allowed in Sort (empty = all)
	MaxWindow  int      // cap on Limit (0 = engine default)
}

// CacheSpec opts an entity into the read-through row cache (P3). Nil (the zero
// value on EntitySpec) means caching is disabled for the entity. Scoped picks
// the cache partition: true => tenant+scope, false => tenant. TTL bounds each
// cached row (0 = no expiry; per-id eviction on write still applies).
type CacheSpec struct {
	TTL    time.Duration
	Scoped bool
}

// EmbedSpec opts an entity into vector embedding. It is declarative metadata
// only — the agent layer supplies the embedding model. Fields names the columns
// whose values are concatenated into the embed text; Text, when set, overrides
// Fields and builds the text from column values.
type EmbedSpec struct {
	Fields []string
	Text   func(vals map[string]any) string
}

// DistillSpec opts an entity into context distillation. Declarative metadata
// only — the distillation layer supplies the summarization model and the guard.
// SourceFields names the columns concatenated into the L0 source text; Text,
// when set, overrides SourceFields. Scopes names the declared scope names that
// form L1 backbone digest nodes. Budget is the L0 summary token budget
// (0 = config default).
type DistillSpec struct {
	SourceFields []string
	Text         func(vals map[string]any) string
	Scopes       []string
	Budget       int
}

// AnalyticsSpec opts an aggregate into the cross-tenant analytics sink and
// declares exactly what data may cross the co-location trust boundary.
// Deny-by-default: an unmarked entity ships nothing. Field minimization:
// Include is an allow-list of payload keys; everything else is stripped
// before the record leaves the projection.
type AnalyticsSpec struct {
	// Include is the allow-list of payload field names co-located in the
	// analytics store. Ignored when IncludeAll is true.
	Include []string
	// IncludeAll ships the whole (unredacted) payload. Use with care — it
	// widens the trust boundary to every column.
	IncludeAll bool
	// Hash names payload fields whose VALUE is replaced with a stable salted
	// hash before it crosses the trust boundary — pseudonymization. The raw
	// value never lands, but equal values hash equally, so operators can still
	// count-distinct / group-by / join on the field across the fleet without
	// co-locating the sensitive value. A hashed field is implicitly included
	// (it need not also appear in Include). Requires Config.Analytics.HashSalt.
	Hash []string
}

// InsightsSpec opts a domain entity into the PER-TENANT customer-facing
// analytics store: on each change the entity is projected (version-gated) into
// the tenant's own fabriq_insights_facts table for on-demand aggregation.
// Unlike AnalyticsSpec (the cross-tenant operator sink) there is NO redaction —
// the data never leaves the tenant's own database. Nil = not projected.
type InsightsSpec struct {
	// Measures are numeric columns aggregatable via Sum/Avg/Min/Max in a cube
	// Query. Each must be a column of the entity.
	Measures []string
	// Dimensions are columns usable as group-by keys. Each must be a column.
	Dimensions []string
}

// MetricSpec is an optional named, typed cube query a tenant can invoke by
// name (query.AnalyticsQuery{Source: metric.Name}). Declaring a metric makes it
// rollup-ready in phase 2. Schemaless events remain queryable without any
// MetricSpec.
type MetricSpec struct {
	Name          string
	Source        string          // event name or entity name
	Measures      []MetricMeasure // at least one
	Dimensions    []string
	DefaultBucket time.Duration // optional default TimeBucket

	// Rollup opts this metric into materialization (phase 2b). Nil (the zero
	// value) means the metric stays live-only, computed on demand from raw
	// events/facts on every query. Rollups are event-sourced only (Source must
	// NOT name a registered entity) and, in 2b-1, additive-only: no measure may
	// be a non-additive "sketch" kind (count_distinct, percentile) — that
	// restriction is relaxed in a later phase once sketch storage exists.
	Rollup *RollupSpec
}

// RollupSpec opts a MetricSpec into materialization (phase 2b). Nil (on
// MetricSpec.Rollup) means live-only. The rollup grain is Bucket; a bucket is
// sealed (rolled up, no longer mutable by the maintainer) once it can no
// longer receive in-grace late events — SealGrace after the bucket's end time.
// RerollWindow trailing buckets are recomputed on each maintainer pass to
// absorb events that arrive within grace but after an earlier pass already
// sealed the bucket.
type RollupSpec struct {
	// Bucket is the rollup grain (e.g. time.Hour). Required, must be > 0.
	Bucket time.Duration

	// SealGrace delays sealing a bucket past its end time, so events that
	// arrive slightly late still land in the correct bucket. Zero means the
	// maintainer applies its own sane default.
	SealGrace time.Duration

	// RerollWindow is how far back (in buckets) the maintainer recomputes on
	// every pass, to absorb events that arrived after a bucket was already
	// sealed. Zero means the maintainer applies its own sane default.
	RerollWindow time.Duration
}

// MetricMeasure mirrors query.Measure at the registry layer (registry must not
// import core/query). Kind is one of "count"/"sum"/"avg"/"min"/"max"/
// "count_distinct"/"percentile".
type MetricMeasure struct {
	Kind  string
	Field string
	As    string
}

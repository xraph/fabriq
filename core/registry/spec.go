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
}

// LiveSpec opts an entity into the live query engine (nil = disabled).
// Filterable/Sortable default to all columns when empty; columns are validated
// against the model at registration.
type LiveSpec struct {
	Filterable []string // columns allowed in Where (empty = all)
	Sortable   []string // columns allowed in Sort (empty = all)
	MaxWindow  int      // cap on Limit (0 = engine default)
}

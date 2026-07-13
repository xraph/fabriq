package insights

import (
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// SourceKind distinguishes the two physical stores an AnalyticsQuery.Source
// can resolve to: raw customer events (fabriq_insights_events) or projected
// entity facts (fabriq_insights_facts).
type SourceKind int

const (
	// SourceEvent means the query reads the tenant's fabriq_insights_events
	// table, keyed by event name.
	SourceEvent SourceKind = iota
	// SourceFacts means the query reads the tenant's fabriq_insights_facts
	// table, keyed by projected entity name.
	SourceFacts
)

// Descriptor is the resolved shape of an AnalyticsQuery.Source: which table
// to read, how to key into it, and (for metric sources) the measures/
// dimensions/bucket the metric declares. Pure data — no I/O, no SQL. Both the
// postgres cube builder and the in-memory fake consume the same Descriptor so
// routing can't drift between them.
type Descriptor struct {
	// Kind is SourceEvent or SourceFacts.
	Kind SourceKind
	// Table is the physical table to read: fabriq_insights_events or
	// fabriq_insights_facts.
	Table string
	// JSONColumn is the JSONB column holding the schemaless payload: "props"
	// for events, "payload" for facts.
	JSONColumn string
	// KeyColumn is the column that scopes rows to this source: "name" for
	// events, "entity" for facts.
	KeyColumn string
	// KeyValue is the value KeyColumn is filtered to (the event name or the
	// projected entity name).
	KeyValue string
	// ExtraWhere is an additional literal SQL predicate ANDed into every query
	// against this source (e.g. "deleted = false" for facts, to hide
	// tombstoned rows). Empty for events.
	ExtraWhere string

	// FromMetric is true when the original Query.Source named a declared
	// MetricSpec rather than an event or entity directly.
	FromMetric bool
	// MetricName is the declared metric's name (set only when FromMetric).
	// Present for error messages that need to name the metric distinctly from
	// its underlying Source.
	MetricName string
	// MetricMeasures are the metric's declared measures, translated to
	// query.Measure (set only when FromMetric).
	MetricMeasures []query.Measure
	// MetricDimensions are the metric's declared dimensions (set only when
	// FromMetric).
	MetricDimensions []string
	// MetricBucket is the metric's declared default time bucket, zero if
	// none (set only when FromMetric).
	MetricBucket time.Duration

	// AllowedColumns, for SourceFacts, is the allow-list of columns (measures
	// ∪ dimensions) a query against this entity may reference — the
	// InsightsSpec column allow-list. Nil for SourceEvent (event props are
	// schemaless; any top-level key is allowed).
	AllowedColumns map[string]bool
}

// ResolveSource decides whether source is a declared metric (expand it to its
// underlying event/facts source plus its measures/dimensions), a projected
// entity (read facts), or a customer event (read events). Precedence is
// metric > entity > event. Pure; no I/O.
//
// A nil registry resolves everything to an event descriptor — the back-compat
// mode the in-memory fake supports when no registry is wired.
func ResolveSource(reg *registry.Registry, source string) (Descriptor, error) {
	if reg != nil {
		if m, ok := reg.Metric(source); ok {
			base, err := resolveBase(reg, m.Source) // must be event or entity, NOT another metric
			if err != nil {
				return Descriptor{}, fmt.Errorf("fabriq: metric %q: %w", source, err)
			}
			measures, err := toQueryMeasures(m.Measures)
			if err != nil {
				return Descriptor{}, fmt.Errorf("fabriq: metric %q: %w", source, err)
			}
			base.FromMetric = true
			base.MetricName = m.Name
			base.MetricMeasures = measures
			base.MetricDimensions = m.Dimensions
			base.MetricBucket = m.DefaultBucket
			return base, nil
		}
	}
	return resolveBase(reg, source)
}

// resolveBase resolves source as an event or a projected entity — never as a
// metric. It is used both for the top-level (non-metric) case and to resolve
// a metric's own Source, where sourcing another metric is rejected.
func resolveBase(reg *registry.Registry, source string) (Descriptor, error) {
	if reg != nil {
		if _, ok := reg.Metric(source); ok {
			return Descriptor{}, fmt.Errorf("fabriq: source %q names a metric; a metric's Source must be an event or a projected entity, not another metric", source)
		}
		if reg.EntityHasInsights(source) {
			ent, _ := reg.Get(source)
			allowed := map[string]bool{}
			for _, c := range ent.Spec.Insights.Measures {
				allowed[c] = true
			}
			for _, c := range ent.Spec.Insights.Dimensions {
				allowed[c] = true
			}
			return Descriptor{
				Kind:           SourceFacts,
				Table:          "fabriq_insights_facts",
				JSONColumn:     "payload",
				KeyColumn:      "entity",
				KeyValue:       source,
				ExtraWhere:     "deleted = false",
				AllowedColumns: allowed,
			}, nil
		}
		// A registered entity WITHOUT an InsightsSpec used as a source is an
		// error: it's a domain entity that opted out of per-tenant analytics
		// projection, so there is no facts table to read.
		if ent, ok := reg.Get(source); ok && ent.Spec.Insights == nil {
			return Descriptor{}, fmt.Errorf("fabriq: entity %q is not analytics-projected (no InsightsSpec)", source)
		}
	}
	return Descriptor{
		Kind:       SourceEvent,
		Table:      "fabriq_insights_events",
		JSONColumn: "props",
		KeyColumn:  "name",
		KeyValue:   source,
	}, nil
}

// toQueryMeasures translates registry.MetricMeasure (the registry-layer,
// import-fence-respecting mirror) to query.Measure. A "percentile" Kind is
// not supported by MetricSpec in phase 2a (query.MeasurePercentile and its
// backing registry.MetricMeasure.Percentile field don't exist yet) — it is
// rejected here rather than silently miscompiled.
func toQueryMeasures(ms []registry.MetricMeasure) ([]query.Measure, error) {
	out := make([]query.Measure, 0, len(ms))
	for _, m := range ms {
		var kind query.MeasureKind
		switch m.Kind {
		case "count":
			kind = query.MeasureCount
		case "sum":
			kind = query.MeasureSum
		case "avg":
			kind = query.MeasureAvg
		case "min":
			kind = query.MeasureMin
		case "max":
			kind = query.MeasureMax
		case "count_distinct":
			kind = query.MeasureCountDistinct
		case "percentile":
			return nil, fmt.Errorf("fabriq: measure kind %q is not supported on a MetricSpec in phase 2a", m.Kind)
		default:
			return nil, fmt.Errorf("fabriq: unknown measure kind %q", m.Kind)
		}
		out = append(out, query.Measure{Kind: kind, Field: m.Field, As: m.As})
	}
	return out, nil
}

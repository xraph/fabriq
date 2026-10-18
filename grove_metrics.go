package fabriq

import (
	"maps"
	"strings"

	gometrics "github.com/xraph/go-utils/metrics"
	"github.com/xraph/grove/observability"

	"github.com/xraph/fabriq/core/registry"
)

// collectionLabel is the db_client_operation_duration_seconds label grove
// fills with the first table a statement touches, as written: schema prefix
// kept, quotes dropped.
const collectionLabel = "db.collection.name"

// collectionOther replaces every collection label value fabriq can't vouch
// for.
const collectionOther = "other"

// systemTables are the fixed tables fabriq's migrations and adapters create.
// TestSystemTablesCoverEveryCreatedTable keeps this list in step with the
// CREATE TABLE statements in the code.
var systemTables = []string{
	"blob_cas", "blob_objects", "blob_sources", "digest_nodes", "links",
	"fs_bookmarks", "fs_nodes", "fs_permissions", "fs_shares",
	"fabriq_analytics_applied", "fabriq_analytics_events", "fabriq_analytics_events_default", "fabriq_analytics_facts",
	"fabriq_api_key",
	"fabriq_crdt_docs", "fabriq_crdt_segments", "fabriq_crdt_snapshots", "fabriq_crdt_updates",
	"fabriq_embeddings", "fabriq_geometries",
	"fabriq_insights_events", "fabriq_insights_facts", "fabriq_insights_rollup_state",
	"fabriq_live_subscriptions", "fabriq_outbox", "fabriq_projection_applied", "fabriq_projection_state",
	"fabriq_tenant_catalog",
}

// rollupTablePrefix matches adapters/postgres: a materialized metric's rollup
// lives in fabriq_insights_rollup_<metric>.
const rollupTablePrefix = "fabriq_insights_rollup_"

// openGroveMetrics is the sink Open and openCatalogMode hand to every grove
// handle they dial: the WithMetrics sink, wrapped by boundCollections. Nil
// when no WithMetrics option was given.
func openGroveMetrics(opts []Option, reg *registry.Registry) gometrics.Metrics {
	return boundCollections(groveMetricsFrom(opts), reg)
}

// boundCollections wraps the metrics sink fabriq hands to grove so the
// db.collection.name label on db_client_operation_duration_seconds can only
// take a fixed set of values: fabriq's system tables, the tables of static
// entities registered at Open, and the rollup tables of their materialized
// metrics. Anything else becomes "other". Without it the label grows with
// every schema-isolation tenant (scans are written tenant_<id>.fabriq_*),
// every runtime dynamic entity's table and any table name a tenant writes
// into raw analytics SQL, and once forge's per-metric label-set cap is hit
// every grove series in the process collapses into an overflow series.
// Schema qualifiers are stripped before matching, so a known table keeps its
// bare name. A nil m stays nil.
func boundCollections(m gometrics.Metrics, reg *registry.Registry) gometrics.Metrics {
	if m == nil {
		return nil
	}
	allowed := make(map[string]bool, len(systemTables))
	for _, t := range systemTables {
		allowed[t] = true
	}
	if reg != nil {
		for _, ent := range reg.All() {
			if ent.Binding != nil && !ent.Binding.IsDynamic() && ent.Binding.Table != "" {
				allowed[strings.ToLower(ent.Binding.Table)] = true
			}
		}
		for _, metric := range reg.MaterializedMetrics() {
			allowed[rollupTablePrefix+strings.ToLower(metric.Name)] = true
		}
	}
	return &collectionBound{Metrics: m, allowed: allowed}
}

type collectionBound struct {
	gometrics.Metrics
	allowed map[string]bool
}

// Histogram rewrites the collection label on grove's operation-duration
// histogram and passes every other histogram through untouched.
func (c *collectionBound) Histogram(name string, opts ...gometrics.MetricOption) gometrics.Histogram {
	if name != observability.OperationDurationMetric {
		return c.Metrics.Histogram(name, opts...)
	}
	var probe gometrics.MetricOptions
	for _, o := range opts {
		o(&probe)
	}
	raw, ok := probe.Labels[collectionLabel]
	if !ok {
		return c.Metrics.Histogram(name, opts...)
	}
	bounded := c.bound(raw)
	if bounded == raw {
		return c.Metrics.Histogram(name, opts...)
	}
	labels := maps.Clone(probe.Labels)
	labels[collectionLabel] = bounded
	return c.Metrics.Histogram(name, append(opts, gometrics.WithLabels(labels))...)
}

// bound maps one collection label value onto the fixed set. Empty (no table,
// a subquery, a KV command) stays empty.
func (c *collectionBound) bound(raw string) string {
	if raw == "" {
		return ""
	}
	table := raw
	if i := strings.LastIndexByte(table, '.'); i >= 0 {
		table = table[i+1:]
	}
	table = strings.ToLower(strings.Trim(table, `"`))
	if c.allowed[table] {
		return table
	}
	return collectionOther
}

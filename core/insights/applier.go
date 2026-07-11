// Package insights projects opt-in domain entities into each tenant's own
// fabriq_insights_facts table (per-tenant customer-facing analytics), version-
// gated and RLS-scoped. It mirrors core/analytics (the cross-tenant operator
// sink) but does NOT redact — the data never leaves the tenant's own database
// — and it does not write Events/Watermark rows (phase 1 is facts only).
//
// Dependency fence: this package imports only core/event, core/registry,
// core/projection, core/tenant, and internal/otel. It must NOT import
// adapters/* (the postgres adapter imports this package to implement
// FactSink — the reverse direction would be an import cycle).
package insights

import (
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
)

// Fact is one projected latest-state row for an opt-in domain entity.
type Fact struct {
	TenantID string
	Entity   string
	AggID    string
	Version  int64
	Payload  json.RawMessage // the entity payload projected to Insights columns
	At       time.Time
	Deleted  bool
}

// Applier turns one event.Envelope into a Fact using the entity's registry
// InsightsSpec. It is PURE and deterministic: the same envelope always yields
// the same bytes (so live apply and backfill agree). Deny-by-default:
// entities without an InsightsSpec produce no records.
type Applier struct{ reg *registry.Registry }

func NewApplier(reg *registry.Registry) *Applier { return &Applier{reg: reg} }

// Apply returns (fact, ok, err). ok=false means the entity is not Insights-
// projected — the caller skips it. No redaction (the tenant owns its own
// data); the payload is projected to the declared Measures+Dimensions columns
// only, keeping facts narrow. Canonical (sorted-key) JSON so live apply and
// backfill agree byte-for-byte.
func (a *Applier) Apply(env event.Envelope) (Fact, bool, error) {
	ent, found := a.reg.Get(env.Aggregate)
	if !found || ent.Spec.Insights == nil {
		return Fact{}, false, nil
	}
	spec := ent.Spec.Insights
	deleted := strings.HasSuffix(env.Type, ".deleted")

	var payload json.RawMessage
	if deleted {
		payload = json.RawMessage(`{}`)
	} else {
		p, err := project(env.Payload, spec)
		if err != nil {
			return Fact{}, false, err
		}
		payload = p
	}

	return Fact{
		TenantID: env.TenantID, Entity: env.Aggregate, AggID: env.AggID,
		Version: env.Version, Payload: payload, At: env.At, Deleted: deleted,
	}, true, nil
}

// project parses the payload and keeps only the declared Measures ∪
// Dimensions top-level keys, marshaled with recursively sorted keys so
// identical inputs always yield identical bytes. Unlike core/analytics'
// redact, project has no allow-list-vs-IncludeAll distinction and no
// hashing — Insights facts stay inside the tenant's own database, so there
// is nothing to pseudonymize.
func project(raw json.RawMessage, spec *registry.InsightsSpec) (json.RawMessage, error) {
	top := map[string]json.RawMessage{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &top); err != nil {
			return nil, err
		}
	}
	keep := make(map[string]struct{}, len(spec.Measures)+len(spec.Dimensions))
	for _, m := range spec.Measures {
		keep[m] = struct{}{}
	}
	for _, d := range spec.Dimensions {
		keep[d] = struct{}{}
	}
	out := map[string]json.RawMessage{}
	for k := range keep {
		if v, ok := top[k]; ok {
			out[k] = v
		}
	}
	return marshalCanonical(deepConvert(out)), nil
}

// deepConvert turns a parsed object into the nested map[string]any output
// shape, recursing into nested objects and keeping other values (arrays,
// scalars) as exact leaf bytes. Copied from core/analytics' deepConvert
// rather than imported — the dependency fence above forbids importing
// core/analytics from this package.
func deepConvert(top map[string]json.RawMessage) map[string]any {
	out := make(map[string]any, len(top))
	for k, v := range top {
		if child, err := parseObject(v); err == nil && looksLikeObject(v) {
			out[k] = deepConvert(child)
		} else {
			out[k] = v
		}
	}
	return out
}

// looksLikeObject reports whether raw is a JSON object (first non-space byte '{').
func looksLikeObject(raw json.RawMessage) bool {
	for _, b := range raw {
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}

// parseObject unmarshals a JSON object into a key→raw map (empty for empty input).
func parseObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	m := map[string]json.RawMessage{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// marshalCanonical marshals a nested map[string]any (leaves are
// json.RawMessage, branches are map[string]any) with recursively sorted
// keys — deterministic regardless of Go map iteration order, at every
// nesting depth, not just the top level.
func marshalCanonical(m map[string]any) json.RawMessage {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		switch v := m[k].(type) {
		case map[string]any:
			b.Write(marshalCanonical(v))
		case json.RawMessage:
			b.Write(v)
		}
	}
	b.WriteByte('}')
	return json.RawMessage(b.String())
}

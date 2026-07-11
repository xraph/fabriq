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
	return marshalCanonical(out), nil
}

// marshalCanonical marshals a flat map[string]json.RawMessage with keys in
// sorted order — deterministic regardless of Go map iteration order.
func marshalCanonical(m map[string]json.RawMessage) json.RawMessage {
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
		b.Write(m[k])
	}
	b.WriteByte('}')
	return json.RawMessage(b.String())
}

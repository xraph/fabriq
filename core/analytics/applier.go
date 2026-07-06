package analytics

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
)

// Applier turns one event.Envelope into a redacted Fact + Event using the
// aggregate's registry AnalyticsSpec. It is PURE and deterministic: the same
// envelope always yields the same records (so live apply and backfill agree).
// Deny-by-default: aggregates without an AnalyticsSpec produce no records.
type Applier struct {
	reg *registry.Registry
}

func NewApplier(reg *registry.Registry) *Applier { return &Applier{reg: reg} }

// Apply returns (fact, event, ok, err). ok=false means the aggregate is not
// analyticized — the caller skips it (cheap, no allocation of records).
func (a *Applier) Apply(env event.Envelope) (Fact, Event, bool, error) {
	ent, found := a.reg.Get(env.Aggregate)
	if !found || ent.Spec.Analytics == nil {
		return Fact{}, Event{}, false, nil
	}
	spec := ent.Spec.Analytics
	deleted := strings.HasSuffix(env.Type, ".deleted")

	var payload json.RawMessage
	if deleted {
		payload = json.RawMessage(`{}`)
	} else {
		red, err := redact(env.Payload, spec)
		if err != nil {
			return Fact{}, Event{}, false, fmt.Errorf("fabriq: analytics redact %s: %w", env.Aggregate, err)
		}
		payload = red
	}

	fact := Fact{
		TenantID: env.TenantID, Aggregate: env.Aggregate, AggID: env.AggID,
		Version: env.Version, Payload: payload, At: env.At, Deleted: deleted,
	}
	ev := Event{
		TenantID: env.TenantID, Aggregate: env.Aggregate, AggID: env.AggID,
		Version: env.Version, Type: env.Type, Payload: payload, At: env.At,
	}
	return fact, ev, true, nil
}

// redact projects the payload down to the allow-listed fields. Deterministic:
// keys are marshaled in sorted order so identical inputs yield identical bytes.
func redact(raw json.RawMessage, spec *registry.AnalyticsSpec) (json.RawMessage, error) {
	if spec.IncludeAll {
		// Re-marshal through a sorted map so output is canonical/deterministic.
		return canonical(raw)
	}
	var in map[string]json.RawMessage
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, err
		}
	}
	out := make(map[string]json.RawMessage, len(spec.Include))
	for _, f := range spec.Include {
		if v, ok := in[f]; ok {
			out[f] = v
		}
	}
	return marshalSorted(out)
}

func canonical(raw json.RawMessage) (json.RawMessage, error) {
	var in map[string]json.RawMessage
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err
	}
	return marshalSorted(in)
}

func marshalSorted(m map[string]json.RawMessage) (json.RawMessage, error) {
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
	return json.RawMessage(b.String()), nil
}

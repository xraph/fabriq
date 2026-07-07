package analytics

import (
	"crypto/sha256"
	"encoding/hex"
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
	reg  *registry.Registry
	salt string
}

// ApplierOption configures an Applier.
type ApplierOption func(*Applier)

// WithHashSalt sets the deployment salt used to pseudonymize AnalyticsSpec.Hash
// fields. It must be stable across restarts and equal everywhere (live apply,
// backfill, reproject) so the same value always hashes the same.
func WithHashSalt(salt string) ApplierOption { return func(a *Applier) { a.salt = salt } }

func NewApplier(reg *registry.Registry, opts ...ApplierOption) *Applier {
	a := &Applier{reg: reg}
	for _, o := range opts {
		o(a)
	}
	return a
}

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
		red, err := redact(env.Payload, spec, a.salt)
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

// Redact projects a payload down to an AnalyticsSpec's allow-list, exactly as
// the live applier does at ingest, applying the salt to any Hash fields.
// Exported so the Reprojector can re-apply a (typically tightened) current spec
// to already-stored payloads. Deterministic.
func Redact(raw json.RawMessage, spec *registry.AnalyticsSpec, salt string) (json.RawMessage, error) {
	return redact(raw, spec, salt)
}

// redact projects the payload down to the allow-listed (Include) plus
// pseudonymized (Hash) fields. Deterministic: keys are marshaled in sorted
// order and Hash fields become a stable salted hash, so identical inputs yield
// identical bytes.
func redact(raw json.RawMessage, spec *registry.AnalyticsSpec, salt string) (json.RawMessage, error) {
	var in map[string]json.RawMessage
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, err
		}
	}
	hashed := make(map[string]bool, len(spec.Hash))
	for _, f := range spec.Hash {
		hashed[f] = true
	}

	out := make(map[string]json.RawMessage, len(in))
	keep := func(field string, val json.RawMessage) {
		if hashed[field] {
			out[field] = hashValue(salt, val)
		} else {
			out[field] = val
		}
	}
	if spec.IncludeAll {
		for f, v := range in {
			keep(f, v)
		}
	} else {
		for _, f := range spec.Include {
			if v, ok := in[f]; ok {
				keep(f, v)
			}
		}
		for _, f := range spec.Hash {
			if v, ok := in[f]; ok {
				keep(f, v)
			}
		}
	}
	return marshalSorted(out)
}

// hashValue replaces a payload value with a stable salted hash, encoded as a
// JSON string. Equal input values hash equally (for count-distinct / joins);
// the raw value is not recoverable.
func hashValue(salt string, raw json.RawMessage) json.RawMessage {
	sum := sha256.Sum256([]byte(salt + "\x00" + string(raw)))
	b, _ := json.Marshal(hex.EncodeToString(sum[:]))
	return b
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

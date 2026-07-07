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
// pseudonymized (Hash) fields. Field names may be dot-paths ("a.b.c") that
// descend through nested JSON objects, so redaction is path-level, not just
// top-level. Leaf bytes are preserved exactly (numbers/ids keep precision);
// output keys are sorted recursively, so identical inputs yield identical bytes
// and Hash fields become a stable salted hash.
func redact(raw json.RawMessage, spec *registry.AnalyticsSpec, salt string) (json.RawMessage, error) {
	top, err := parseObject(raw)
	if err != nil {
		return nil, err
	}

	var out map[string]any
	if spec.IncludeAll {
		out = deepConvert(top)
	} else {
		out = map[string]any{}
		for _, p := range spec.Include {
			if v, ok := getPath(top, splitPath(p)); ok {
				setPath(out, splitPath(p), v)
			}
		}
	}
	// Hashing applies to both Include and IncludeAll: replace the leaf at each
	// Hash path with its salted hash (adding the path if IncludeAll omitted it
	// or it was not already Included).
	for _, p := range spec.Hash {
		segs := splitPath(p)
		if v, ok := getPath(top, segs); ok {
			setPath(out, segs, hashValue(salt, v))
		}
	}
	return marshalCanonical(out), nil
}

// hashValue replaces a payload value with a stable salted hash, encoded as a
// JSON string. Equal input values hash equally (for count-distinct / joins);
// the raw value is not recoverable.
func hashValue(salt string, raw json.RawMessage) json.RawMessage {
	sum := sha256.Sum256([]byte(salt + "\x00" + string(raw)))
	b, _ := json.Marshal(hex.EncodeToString(sum[:]))
	return b
}

// splitPath splits a dot-path into segments. A field with no dot is a single
// top-level segment (backward compatible).
func splitPath(p string) []string { return strings.Split(p, ".") }

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

// getPath walks segs through nested objects and returns the leaf's raw bytes.
// ok=false if any intermediate segment is missing or not a JSON object.
func getPath(top map[string]json.RawMessage, segs []string) (json.RawMessage, bool) {
	cur := top
	for i, s := range segs {
		v, ok := cur[s]
		if !ok {
			return nil, false
		}
		if i == len(segs)-1 {
			return v, true
		}
		next, err := parseObject(v)
		if err != nil {
			return nil, false // an intermediate segment is not an object
		}
		cur = next
	}
	return nil, false
}

// setPath sets value at segs in a nested map[string]any output, creating
// intermediate maps. A more specific path replaces a broader leaf if they clash.
func setPath(out map[string]any, segs []string, value json.RawMessage) {
	cur := out
	for i, s := range segs {
		if i == len(segs)-1 {
			cur[s] = value
			return
		}
		child, ok := cur[s].(map[string]any)
		if !ok {
			child = map[string]any{}
			cur[s] = child
		}
		cur = child
	}
}

// deepConvert turns a parsed object into the nested map[string]any output shape,
// recursing into nested objects and keeping other values (arrays, scalars) as
// exact leaf bytes. Used for IncludeAll's canonical re-marshal.
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

// marshalCanonical marshals a nested map[string]any (leaves are json.RawMessage,
// branches are map[string]any) with recursively sorted keys — deterministic.
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

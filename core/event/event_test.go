package event_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/event"
)

func validEnvelope() event.Envelope {
	return event.Envelope{
		ID:                   event.NewID(),
		TenantID:             "acme",
		Aggregate:            "asset",
		AggID:                "01HASSET",
		Version:              3,
		Type:                 "asset.updated",
		At:                   time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC),
		PayloadSchemaVersion: 1,
		Payload:              json.RawMessage(`{"id":"01HASSET","name":"Pump 7"}`),
		Traceparent:          "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
}

func TestNewID_ULIDsAreLexicallyOrdered(t *testing.T) {
	prev := event.NewID()
	for i := 0; i < 1000; i++ {
		next := event.NewID()
		if len(next) != 26 {
			t.Fatalf("ULID length = %d, want 26", len(next))
		}
		if next <= prev {
			t.Fatalf("ULIDs not monotonic: %s then %s", prev, next)
		}
		prev = next
	}
}

func TestCodec_RoundTrip(t *testing.T) {
	in := validEnvelope()
	raw, err := event.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := event.Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.ID != in.ID || out.TenantID != in.TenantID || out.Aggregate != in.Aggregate ||
		out.AggID != in.AggID || out.Version != in.Version || out.Type != in.Type ||
		!out.At.Equal(in.At) || out.PayloadSchemaVersion != in.PayloadSchemaVersion ||
		out.Traceparent != in.Traceparent || string(out.Payload) != string(in.Payload) {
		t.Fatalf("round trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestDecode_RejectsInvalidEnvelopes(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*event.Envelope)
	}{
		{"missing id", func(e *event.Envelope) { e.ID = "" }},
		{"missing tenant", func(e *event.Envelope) { e.TenantID = "" }},
		{"missing aggregate", func(e *event.Envelope) { e.Aggregate = "" }},
		{"missing agg id", func(e *event.Envelope) { e.AggID = "" }},
		{"zero version", func(e *event.Envelope) { e.Version = 0 }},
		{"missing type", func(e *event.Envelope) { e.Type = "" }},
		{"zero payload schema", func(e *event.Envelope) { e.PayloadSchemaVersion = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvelope()
			tc.mutate(&env)
			raw, err := json.Marshal(env) // bypass Encode's own validation
			if err != nil {
				t.Fatal(err)
			}
			if _, err := event.Decode(raw); err == nil {
				t.Fatal("Decode accepted invalid envelope")
			}
		})
	}
}

func TestDecode_RejectsGarbage(t *testing.T) {
	if _, err := event.Decode([]byte("{not json")); err == nil {
		t.Fatal("Decode accepted garbage")
	}
}

func TestUpcasterChain_AppliesInOrderUpToLatest(t *testing.T) {
	chain := event.NewUpcasterChain()
	// v1 -> v2: rename "nm" to "name"
	chain.MustRegister(event.Upcaster{
		Type: "asset.updated", FromVersion: 1,
		Fn: func(p json.RawMessage) (json.RawMessage, error) {
			var m map[string]any
			if err := json.Unmarshal(p, &m); err != nil {
				return nil, err
			}
			m["name"] = m["nm"]
			delete(m, "nm")
			return json.Marshal(m)
		},
	})
	// v2 -> v3: add "kind"
	chain.MustRegister(event.Upcaster{
		Type: "asset.updated", FromVersion: 2,
		Fn: func(p json.RawMessage) (json.RawMessage, error) {
			var m map[string]any
			if err := json.Unmarshal(p, &m); err != nil {
				return nil, err
			}
			m["kind"] = "asset"
			return json.Marshal(m)
		},
	})

	env := validEnvelope()
	env.PayloadSchemaVersion = 1
	env.Payload = json.RawMessage(`{"nm":"Pump 7"}`)

	out, err := chain.Apply(env)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.PayloadSchemaVersion != 3 {
		t.Fatalf("PayloadSchemaVersion = %d, want 3", out.PayloadSchemaVersion)
	}
	var m map[string]any
	if err := json.Unmarshal(out.Payload, &m); err != nil {
		t.Fatal(err)
	}
	if m["name"] != "Pump 7" || m["kind"] != "asset" {
		t.Fatalf("payload not upcast: %v", m)
	}
	if _, hasOld := m["nm"]; hasOld {
		t.Fatal("old field survived upcast")
	}
}

func TestUpcasterChain_LatestVersionPassesThrough(t *testing.T) {
	chain := event.NewUpcasterChain()
	env := validEnvelope()
	out, err := chain.Apply(env)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.PayloadSchemaVersion != env.PayloadSchemaVersion || string(out.Payload) != string(env.Payload) {
		t.Fatal("no-op chain must pass envelope through unchanged")
	}
}

func TestUpcasterChain_DuplicateRegistrationFails(t *testing.T) {
	chain := event.NewUpcasterChain()
	up := event.Upcaster{Type: "t.updated", FromVersion: 1, Fn: func(p json.RawMessage) (json.RawMessage, error) { return p, nil }}
	if err := chain.Register(up); err != nil {
		t.Fatal(err)
	}
	if err := chain.Register(up); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("want duplicate-registration error, got %v", err)
	}
}

func TestUpcasterChain_NilFnRejected(t *testing.T) {
	chain := event.NewUpcasterChain()
	if err := chain.Register(event.Upcaster{Type: "t", FromVersion: 1}); err == nil {
		t.Fatal("nil Fn must be rejected")
	}
}

func BenchmarkEncode(b *testing.B) {
	env := validEnvelope()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := event.Encode(env); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	raw, _ := event.Encode(validEnvelope())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := event.Decode(raw); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = event.NewID()
	}
}

func BenchmarkUpcastChain_ThreeSteps(b *testing.B) {
	chain := event.NewUpcasterChain()
	identity := func(p json.RawMessage) (json.RawMessage, error) { return p, nil }
	for v := 1; v <= 3; v++ {
		chain.MustRegister(event.Upcaster{Type: "asset.updated", FromVersion: v, Fn: identity})
	}
	env := validEnvelope()
	env.PayloadSchemaVersion = 1
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := chain.Apply(env); err != nil {
			b.Fatal(err)
		}
	}
}

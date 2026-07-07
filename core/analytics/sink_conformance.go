package analytics

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// RunSinkConformance is the single behavioral contract every analytics.Sink
// must satisfy. fabriqtest runs it against the fake; adapters/pganalytics runs
// it against real Postgres. Drift between fake and adapter is a failing test.
func RunSinkConformance(t *testing.T, newSink func() Sink) {
	ctx := context.Background()
	fact := func(tenant, id string, v int64, deleted bool) Fact {
		return Fact{TenantID: tenant, Aggregate: "widget", AggID: id, Version: v,
			Payload: json.RawMessage(`{"n":1}`), At: time.Unix(100, 0).UTC(), Deleted: deleted}
	}

	t.Run("UpsertLatestWins", func(t *testing.T) {
		s := newSink()
		defer s.Close()
		must(t, s.UpsertFacts(ctx, []Fact{fact("t1", "w1", 1, false)}))
		must(t, s.UpsertFacts(ctx, []Fact{fact("t1", "w1", 3, false)}))
		must(t, s.UpsertFacts(ctx, []Fact{fact("t1", "w1", 2, false)})) // stale
		must(t, s.SetWatermark(ctx, []Watermark{{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 3}}))
		v, err := s.Watermark(ctx, "t1", "widget", "w1")
		if err != nil {
			t.Fatal(err)
		}
		if v != 3 {
			t.Fatalf("stale upsert was not ignored: watermark=%d want 3", v)
		}
	})

	t.Run("ReplayIsIdempotent", func(t *testing.T) {
		s := newSink()
		defer s.Close()
		batch := []Fact{fact("t1", "w1", 5, false)}
		must(t, s.UpsertFacts(ctx, batch))
		must(t, s.UpsertFacts(ctx, batch)) // same version, no-op
	})

	t.Run("AppendDedupes", func(t *testing.T) {
		s := newSink()
		defer s.Close()
		e := Event{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 1, Type: "widget.created",
			Payload: json.RawMessage(`{}`), At: time.Unix(100, 0).UTC()}
		must(t, s.AppendEvents(ctx, []Event{e}))
		must(t, s.AppendEvents(ctx, []Event{e})) // dup, no error
	})

	t.Run("WatermarkMonotonic", func(t *testing.T) {
		s := newSink()
		defer s.Close()
		must(t, s.SetWatermark(ctx, []Watermark{{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 4}}))
		must(t, s.SetWatermark(ctx, []Watermark{{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 2}})) // lower ignored
		v, err := s.Watermark(ctx, "t1", "widget", "w1")
		if err != nil {
			t.Fatal(err)
		}
		if v != 4 {
			t.Fatalf("watermark not monotonic: got %d want 4", v)
		}
	})

	t.Run("WatermarkUnknownIsZero", func(t *testing.T) {
		s := newSink()
		defer s.Close()
		v, err := s.Watermark(ctx, "t1", "widget", "does-not-exist")
		if err != nil {
			t.Fatal(err)
		}
		if v != 0 {
			t.Fatalf("unknown watermark: got %d want 0", v)
		}
	})

	t.Run("CrossTenantRowsCoexist", func(t *testing.T) {
		s := newSink()
		defer s.Close()
		must(t, s.UpsertFacts(ctx, []Fact{fact("t1", "w1", 1, false), fact("t2", "w1", 1, false)}))
		must(t, s.SetWatermark(ctx, []Watermark{
			{TenantID: "t1", Aggregate: "widget", AggID: "w1", Version: 1},
			{TenantID: "t2", Aggregate: "widget", AggID: "w1", Version: 1},
		}))
		for _, tn := range []string{"t1", "t2"} {
			v, err := s.Watermark(ctx, tn, "widget", "w1")
			if err != nil || v != 1 {
				t.Fatalf("tenant %s isolation broken: v=%d err=%v", tn, v, err)
			}
		}
	})

	t.Run("LagByTenantReportsEach", func(t *testing.T) {
		s := newSink()
		defer s.Close()
		// Empty sink: no tenants behind.
		if m, err := s.LagByTenant(ctx); err != nil || len(m) != 0 {
			t.Fatalf("empty sink: lag=%v err=%v, want empty map", m, err)
		}
		// Two tenants, both with a fact committed in the past (At=1970).
		must(t, s.UpsertFacts(ctx, []Fact{fact("t1", "w1", 1, false), fact("t2", "w1", 1, false)}))
		m, err := s.LagByTenant(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(m) != 2 {
			t.Fatalf("lag map = %v, want an entry per tenant", m)
		}
		for _, tn := range []string{"t1", "t2"} {
			if m[tn] <= 0 {
				t.Fatalf("tenant %s lag = %v, want > 0 for a fact committed in the past", tn, m[tn])
			}
		}
	})

	t.Run("PurgeTenantErasesOnlyThatTenant", func(t *testing.T) {
		s := newSink()
		defer s.Close()
		ev := func(tenant, id string, v int64) Event {
			return Event{TenantID: tenant, Aggregate: "widget", AggID: id, Version: v,
				Type: "widget.created", Payload: json.RawMessage(`{}`), At: time.Unix(100, 0).UTC()}
		}
		for _, tn := range []string{"t1", "t2"} {
			must(t, s.UpsertFacts(ctx, []Fact{fact(tn, "w1", 1, false)}))
			must(t, s.AppendEvents(ctx, []Event{ev(tn, "w1", 1)}))
			must(t, s.SetWatermark(ctx, []Watermark{{TenantID: tn, Aggregate: "widget", AggID: "w1", Version: 1}}))
		}

		n, err := s.PurgeTenant(ctx, "t1")
		if err != nil {
			t.Fatal(err)
		}
		if n < 3 { // at least fact + event + watermark
			t.Fatalf("purge removed %d rows, want >= 3", n)
		}

		// t1 is gone: its watermark reads zero (unknown).
		if v, _ := s.Watermark(ctx, "t1", "widget", "w1"); v != 0 {
			t.Fatalf("t1 watermark after purge = %d, want 0", v)
		}
		// t2 is untouched.
		if v, _ := s.Watermark(ctx, "t2", "widget", "w1"); v != 1 {
			t.Fatalf("t2 watermark after purging t1 = %d, want 1 (must be untouched)", v)
		}

		// Idempotent: purging an absent tenant removes nothing.
		again, err := s.PurgeTenant(ctx, "t1")
		if err != nil || again != 0 {
			t.Fatalf("second purge of t1: n=%d err=%v, want 0/nil", again, err)
		}
	})

	t.Run("ReprojectTenantRewritesPayloads", func(t *testing.T) {
		s := newSink()
		defer s.Close()
		// A fact and an event whose payloads carry a field we now want stripped.
		wide := func(id string) (Fact, Event) {
			f := Fact{TenantID: "t1", Aggregate: "widget", AggID: id, Version: 1,
				Payload: json.RawMessage(`{"keep":"y","drop":"secret"}`), At: time.Unix(100, 0).UTC()}
			e := Event{TenantID: "t1", Aggregate: "widget", AggID: id, Version: 1, Type: "widget.created",
				Payload: json.RawMessage(`{"keep":"y","drop":"secret"}`), At: time.Unix(100, 0).UTC()}
			return f, e
		}
		f, e := wide("w1")
		must(t, s.UpsertFacts(ctx, []Fact{f}))
		must(t, s.AppendEvents(ctx, []Event{e}))
		// Another tenant's row with the same shape must be untouched.
		of, oe := wide("w1")
		of.TenantID, oe.TenantID = "t2", "t2"
		must(t, s.UpsertFacts(ctx, []Fact{of}))
		must(t, s.AppendEvents(ctx, []Event{oe}))

		// transform = keep only "keep" (a tightened allow-list), canonical output.
		keepOnly := func(p json.RawMessage) (json.RawMessage, error) {
			var in map[string]json.RawMessage
			if err := json.Unmarshal(p, &in); err != nil {
				return nil, err
			}
			if v, ok := in["keep"]; ok {
				return json.RawMessage(`{"keep":` + string(v) + `}`), nil
			}
			return json.RawMessage(`{}`), nil
		}

		n, err := s.ReprojectTenant(ctx, "t1", "widget", keepOnly)
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 { // one fact + one event rewritten
			t.Fatalf("reproject rewrote %d rows, want 2", n)
		}

		// Idempotent: a second run rewrites nothing (payloads already narrowed).
		again, err := s.ReprojectTenant(ctx, "t1", "widget", keepOnly)
		if err != nil || again != 0 {
			t.Fatalf("second reproject: n=%d err=%v, want 0/nil", again, err)
		}

		// t2 was not in scope: its stored payload must still contain "drop".
		leftN, err := s.ReprojectTenant(ctx, "t2", "widget", keepOnly)
		if err != nil {
			t.Fatal(err)
		}
		if leftN != 2 {
			t.Fatalf("t2 reproject rewrote %d rows, want 2 (t2 must have been untouched by the t1 reproject)", leftN)
		}
	})
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

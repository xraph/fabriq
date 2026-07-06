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
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

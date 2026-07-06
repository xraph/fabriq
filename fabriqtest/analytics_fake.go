package fabriqtest

import (
	"context"
	"strconv"
	"sync"

	"github.com/xraph/fabriq/core/analytics"
)

// FakeAnalyticsSink is an in-memory analytics.Sink with the same
// idempotency semantics as the Postgres adapter. It is the conformance twin
// used to unit-test the applier, consumer, and backfill without Docker.
type FakeAnalyticsSink struct {
	mu    sync.Mutex
	facts map[string]analytics.Fact // key: tenant|aggregate|aggID
	evs   map[string]analytics.Event
	wm    map[string]int64
}

func NewFakeAnalyticsSink() *FakeAnalyticsSink {
	return &FakeAnalyticsSink{
		facts: map[string]analytics.Fact{},
		evs:   map[string]analytics.Event{},
		wm:    map[string]int64{},
	}
}

func factKey(tenant, agg, id string) string { return tenant + "|" + agg + "|" + id }
func eventKey(tenant, agg, id string, v int64) string {
	return tenant + "|" + agg + "|" + id + "|" + itoa(v)
}

func (s *FakeAnalyticsSink) UpsertFacts(_ context.Context, facts []analytics.Fact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range facts {
		k := factKey(f.TenantID, f.Aggregate, f.AggID)
		if cur, ok := s.facts[k]; ok && cur.Version >= f.Version {
			continue // version gate
		}
		s.facts[k] = f
	}
	return nil
}

func (s *FakeAnalyticsSink) AppendEvents(_ context.Context, events []analytics.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range events {
		k := eventKey(e.TenantID, e.Aggregate, e.AggID, e.Version)
		if _, ok := s.evs[k]; ok {
			continue // dedupe
		}
		s.evs[k] = e
	}
	return nil
}

func (s *FakeAnalyticsSink) Watermark(_ context.Context, tenant, agg, id string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wm[factKey(tenant, agg, id)], nil
}

func (s *FakeAnalyticsSink) SetWatermark(_ context.Context, ws []analytics.Watermark) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range ws {
		k := factKey(w.TenantID, w.Aggregate, w.AggID)
		if w.Version > s.wm[k] {
			s.wm[k] = w.Version
		}
	}
	return nil
}

func (s *FakeAnalyticsSink) Close() error { return nil }

// Facts returns a snapshot keyed by tenant|aggregate|aggID (test inspection).
func (s *FakeAnalyticsSink) Facts() map[string]analytics.Fact {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]analytics.Fact, len(s.facts))
	for k, v := range s.facts {
		out[k] = v
	}
	return out
}

// Events returns a snapshot of all appended events (test inspection).
func (s *FakeAnalyticsSink) Events() map[string]analytics.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]analytics.Event, len(s.evs))
	for k, v := range s.evs {
		out[k] = v
	}
	return out
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

// Verify FakeAnalyticsSink implements analytics.Sink.
var _ analytics.Sink = (*FakeAnalyticsSink)(nil)

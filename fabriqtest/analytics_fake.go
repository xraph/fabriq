package fabriqtest

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

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

// AllWatermarks returns every applied watermark for a tenant.
func (s *FakeAnalyticsSink) AllWatermarks(_ context.Context, tenantID string) ([]analytics.Watermark, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []analytics.Watermark
	prefix := tenantID + "|"
	for k, v := range s.wm {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		// key is tenant|aggregate|aggID
		parts := strings.SplitN(k, "|", 3)
		if len(parts) != 3 {
			continue
		}
		out = append(out, analytics.Watermark{TenantID: parts[0], Aggregate: parts[1], AggID: parts[2], Version: v})
	}
	return out, nil
}

// LagByTenant returns now() - (that tenant's newest fact At) per tenant.
func (s *FakeAnalyticsSink) LagByTenant(_ context.Context) (map[string]float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	newest := map[string]time.Time{}
	for _, f := range s.facts {
		if f.At.After(newest[f.TenantID]) {
			newest[f.TenantID] = f.At
		}
	}
	out := make(map[string]float64, len(newest))
	for tid, at := range newest {
		out[tid] = time.Since(at).Seconds()
	}
	return out, nil
}

// ReprojectTenant re-projects stored fact and event payloads for a tenant (and
// optional aggregate) through transform, in place, returning the count changed.
func (s *FakeAnalyticsSink) ReprojectTenant(_ context.Context, tenantID, aggregate string, transform func(json.RawMessage) (json.RawMessage, error)) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for k, f := range s.facts {
		if f.TenantID != tenantID || (aggregate != "" && f.Aggregate != aggregate) {
			continue
		}
		np, err := transform(f.Payload)
		if err != nil {
			return n, err
		}
		if string(np) != string(f.Payload) {
			f.Payload = np
			s.facts[k] = f
			n++
		}
	}
	for k, e := range s.evs {
		if e.TenantID != tenantID || (aggregate != "" && e.Aggregate != aggregate) {
			continue
		}
		np, err := transform(e.Payload)
		if err != nil {
			return n, err
		}
		if string(np) != string(e.Payload) {
			e.Payload = np
			s.evs[k] = e
			n++
		}
	}
	return n, nil
}

// PruneEvents deletes history events older than olderThan across all tenants.
func (s *FakeAnalyticsSink) PruneEvents(_ context.Context, olderThan time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for k, e := range s.evs {
		if e.At.Before(olderThan) {
			delete(s.evs, k)
			n++
		}
	}
	return n, nil
}

// MaintainPartitions is a no-op for the in-memory fake (it does not partition).
func (s *FakeAnalyticsSink) MaintainPartitions(_ context.Context, _ time.Duration) (created, dropped int, err error) {
	return 0, 0, nil
}

// PurgeTenant hard-deletes every fact, event, and watermark for one tenant and
// returns the count removed. Idempotent.
func (s *FakeAnalyticsSink) PurgeTenant(_ context.Context, tenantID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	prefix := tenantID + "|"
	for k := range s.facts {
		if strings.HasPrefix(k, prefix) {
			delete(s.facts, k)
			n++
		}
	}
	for k := range s.evs {
		if strings.HasPrefix(k, prefix) {
			delete(s.evs, k)
			n++
		}
	}
	for k := range s.wm {
		if strings.HasPrefix(k, prefix) {
			delete(s.wm, k)
			n++
		}
	}
	return n, nil
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

package insights_test

import (
	"context"
	"sync"
	"testing"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/insights"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// fakeSource replays a fixed slice of envelopes then returns. Mirrors
// core/analytics/consumer_test.go's fakeSource (unexported there, so it is
// re-declared here rather than shared).
type fakeSource struct{ envs []event.Envelope }

func (f *fakeSource) EnsureGroup(context.Context, string) error { return nil }
func (f *fakeSource) Consume(_ context.Context, _, _ string, handle func(string, event.Envelope) error) error {
	for i, e := range f.envs {
		if err := handle(itoaTest(i), e); err != nil {
			return err
		}
	}
	return nil
}
func itoaTest(i int) string { return string(rune('a' + i)) }

// fakeSink captures UpsertInsightFacts calls and the tenant+scope derived
// from ctx at call time, so tests can assert the consumer stamped the right
// tenant (and, when present, the right scope).
type fakeSink struct {
	mu     sync.Mutex
	calls  int
	facts  []insights.Fact
	tids   []string
	scopes []string // "" when the ctx was left unscoped
}

func (s *fakeSink) UpsertInsightFacts(ctx context.Context, facts []insights.Fact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.facts = append(s.facts, facts...)
	tid, _ := tenant.FromContext(ctx)
	s.tids = append(s.tids, tid)
	s.scopes = append(s.scopes, tenant.ScopeOrEmpty(ctx))
	return nil
}

func TestConsumer_AppliesMarkedEntity(t *testing.T) {
	sink := &fakeSink{}
	spec := &registry.InsightsSpec{Measures: []string{"price"}, Dimensions: []string{"name"}}
	c := &insights.Consumer{
		Group:   "proj:insights",
		Source:  &fakeSource{envs: []event.Envelope{env("widget.updated", 1, `{"name":"a","price":10}`)}},
		Applier: insights.NewApplier(regWith(spec)),
		Sink:    sink,
	}
	if err := c.Run(context.Background(), "c1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sink.calls != 1 {
		t.Fatalf("expected 1 UpsertInsightFacts call, got %d", sink.calls)
	}
	if len(sink.facts) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(sink.facts))
	}
	if sink.tids[0] != "t1" {
		t.Fatalf("expected the derived tenant ctx to carry t1, got %q", sink.tids[0])
	}
}

func TestConsumer_SkipsUnmarkedEntity(t *testing.T) {
	sink := &fakeSink{}
	c := &insights.Consumer{
		Group:   "proj:insights",
		Source:  &fakeSource{envs: []event.Envelope{env("widget.updated", 1, `{}`)}},
		Applier: insights.NewApplier(regWith(nil)),
		Sink:    sink,
	}
	if err := c.Run(context.Background(), "c1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sink.calls != 0 {
		t.Fatalf("unmarked entity should produce zero writes, got %d", sink.calls)
	}
}

func TestConsumer_StampsScopeWhenPresent(t *testing.T) {
	sink := &fakeSink{}
	spec := &registry.InsightsSpec{Measures: []string{"price"}, Dimensions: []string{"name"}}
	e := env("widget.updated", 1, `{"name":"a","price":10}`)
	e.ScopeID = "s1"
	c := &insights.Consumer{
		Group:   "proj:insights",
		Source:  &fakeSource{envs: []event.Envelope{e}},
		Applier: insights.NewApplier(regWith(spec)),
		Sink:    sink,
	}
	if err := c.Run(context.Background(), "c1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sink.calls != 1 {
		t.Fatalf("expected 1 UpsertInsightFacts call, got %d", sink.calls)
	}
	if sink.tids[0] != "t1" {
		t.Fatalf("expected the derived tenant ctx to carry t1, got %q", sink.tids[0])
	}
	if sink.scopes[0] != "s1" {
		t.Fatalf("expected the derived ctx to carry scope s1, got %q", sink.scopes[0])
	}
}

func TestConsumer_LeavesScopeUnstampedWhenEnvelopeUnscoped(t *testing.T) {
	sink := &fakeSink{}
	spec := &registry.InsightsSpec{Measures: []string{"price"}, Dimensions: []string{"name"}}
	c := &insights.Consumer{
		Group:   "proj:insights",
		Source:  &fakeSource{envs: []event.Envelope{env("widget.updated", 1, `{"name":"a","price":10}`)}},
		Applier: insights.NewApplier(regWith(spec)),
		Sink:    sink,
	}
	if err := c.Run(context.Background(), "c1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sink.calls != 1 {
		t.Fatalf("expected 1 UpsertInsightFacts call, got %d", sink.calls)
	}
	if sink.scopes[0] != "" {
		t.Fatalf("expected an unscoped envelope to leave ctx unscoped, got scope %q", sink.scopes[0])
	}
}

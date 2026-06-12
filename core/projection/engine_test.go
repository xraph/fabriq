package projection_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

// fakeSource feeds queued envelopes to the consumer, redelivering on
// handler error (at-least-once), then blocks until ctx ends.
type fakeSource struct {
	mu      sync.Mutex
	groups  []string
	queue   []event.Envelope
	retries int
}

func (s *fakeSource) EnsureGroup(_ context.Context, group string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups = append(s.groups, group)
	return nil
}

func (s *fakeSource) Consume(ctx context.Context, _, _ string, handle func(string, event.Envelope) error) error {
	for i, env := range s.queue {
		for {
			if err := handle(streamID(i), env); err == nil {
				break
			}
			s.mu.Lock()
			s.retries++
			n := s.retries
			s.mu.Unlock()
			if n > 10 {
				return errors.New("fakeSource: handler keeps failing")
			}
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func streamID(i int) string {
	return time.Unix(1718000000, 0).UTC().Format("20060102") + "-" + string(rune(48+i))
}

func engineWorld(t *testing.T) (*fabriqtest.World, *registry.Registry) {
	t.Helper()
	reg := testRegistry(t)
	return fabriqtest.NewWorld(reg), reg
}

func assetCreated(t *testing.T, payload map[string]any) event.Envelope {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return event.Envelope{
		ID: event.NewID(), TenantID: "acme", Aggregate: "asset", AggID: "A1",
		Version: 1, Type: "asset.created", At: time.Now().UTC(),
		PayloadSchemaVersion: 1, Payload: raw,
	}
}

func runEngine(t *testing.T, e *projection.Engine, src *fakeSource) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, "test-consumer") }()
	// The fake source processes its queue synchronously before blocking;
	// give it a moment, then stop.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("engine did not stop on ctx cancel")
	}
	if len(src.groups) != 1 {
		t.Fatalf("EnsureGroup called %d times, want 1", len(src.groups))
	}
}

func TestEngine_AppliesEventsAndRecordsState(t *testing.T) {
	w, reg := engineWorld(t)
	src := &fakeSource{queue: []event.Envelope{
		assetCreated(t, map[string]any{"id": "A1", "tenant_id": "acme", "version": 1, "name": "Pump", "site_id": "S1"}),
	}}
	e := &projection.Engine{
		Projection: "graph", Group: "proj:graph",
		Source: src, Sink: w.Graph,
		Applier: projection.GraphApplier(reg),
		State:   w.Projections,
	}
	runEngine(t, e, src)

	// Default target "" resolves to the tenant's live graph in the fake.
	node, ok := w.Graph.Node(registry.GraphName("acme"), "Asset", "A1")
	if !ok || node.Props["name"] != "Pump" {
		t.Fatalf("node not applied: %+v ok=%v", node, ok)
	}
	if !w.Graph.HasEdge(registry.GraphName("acme"), "LOCATED_AT", "Asset", "A1", "Site", "S1") {
		t.Fatal("edge not applied")
	}
	v, err := w.Projections.AppliedVersion(context.Background(), "acme", "graph", "asset", "A1")
	if err != nil || v != 1 {
		t.Fatalf("applied version = (%d, %v), want (1, nil)", v, err)
	}
}

func TestEngine_RetriesFailedApply(t *testing.T) {
	w, reg := engineWorld(t)
	src := &fakeSource{queue: []event.Envelope{
		assetCreated(t, map[string]any{"id": "A1", "tenant_id": "acme", "version": 1, "name": "Pump"}),
	}}
	flaky := &flakySink{inner: w.Graph, failures: 2}
	e := &projection.Engine{
		Projection: "graph", Group: "proj:graph",
		Source: src, Sink: flaky,
		Applier: projection.GraphApplier(reg),
		State:   w.Projections,
	}
	runEngine(t, e, src)

	if _, ok := w.Graph.Node(registry.GraphName("acme"), "Asset", "A1"); !ok {
		t.Fatal("event lost after transient sink failures (at-least-once violated)")
	}
	if src.retries < 2 {
		t.Fatalf("expected redeliveries, got %d", src.retries)
	}
}

type flakySink struct {
	inner    projection.Sink
	failures int
}

func (f *flakySink) ApplyMutations(ctx context.Context, target string, muts []projection.Mutation) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("transient sink failure")
	}
	return f.inner.ApplyMutations(ctx, target, muts)
}

func TestEngine_UpcastsBeforeApplier(t *testing.T) {
	w, reg := engineWorld(t)
	chain := event.NewUpcasterChain()
	chain.MustRegister(event.Upcaster{
		Type: "asset.created", FromVersion: 1,
		Fn: func(p json.RawMessage) (json.RawMessage, error) {
			var m map[string]any
			if err := json.Unmarshal(p, &m); err != nil {
				return nil, err
			}
			m["name"] = m["nm"] // v1 payloads used "nm"
			delete(m, "nm")
			return json.Marshal(m)
		},
	})
	src := &fakeSource{queue: []event.Envelope{
		assetCreated(t, map[string]any{"id": "A1", "tenant_id": "acme", "version": 1, "nm": "Old Shape"}),
	}}
	e := &projection.Engine{
		Projection: "graph", Group: "proj:graph",
		Source: src, Sink: w.Graph,
		Applier:   projection.GraphApplier(reg),
		Upcasters: chain,
		State:     w.Projections,
	}
	runEngine(t, e, src)

	node, ok := w.Graph.Node(registry.GraphName("acme"), "Asset", "A1")
	if !ok || node.Props["name"] != "Old Shape" {
		t.Fatalf("upcaster did not run before applier: %+v", node.Props)
	}
}

func TestEngine_DualTargetsDuringRebuild(t *testing.T) {
	w, reg := engineWorld(t)
	src := &fakeSource{queue: []event.Envelope{
		assetCreated(t, map[string]any{"id": "A1", "tenant_id": "acme", "version": 1, "name": "Pump"}),
	}}
	building := registry.GraphNameVersioned("acme", 2)
	e := &projection.Engine{
		Projection: "graph", Group: "proj:graph",
		Source: src, Sink: w.Graph,
		Applier: projection.GraphApplier(reg),
		State:   w.Projections,
		TargetsFor: func(context.Context, string) ([]string, error) {
			return []string{"", building}, nil // live + building target
		},
	}
	runEngine(t, e, src)

	if _, ok := w.Graph.Node(registry.GraphName("acme"), "Asset", "A1"); !ok {
		t.Fatal("live target missed the event")
	}
	if _, ok := w.Graph.Node(building, "Asset", "A1"); !ok {
		t.Fatal("building target missed the event (rebuild catch-up broken)")
	}
}

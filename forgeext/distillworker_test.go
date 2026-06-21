package forgeext

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/agent"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/internal/metrics"
)

// dwNote is a small distillable grove model for the distill-worker test:
// title/body are the L0 source text and site_id is the scope.
type dwNote struct {
	grove.BaseModel `grove:"table:dwnotes"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Title    string `grove:"title,notnull"`
	Body     string `grove:"body"`
	SiteID   string `grove:"site_id"`
}

// fakeSum is a deterministic summarizer: it concatenates the raw L0 text or the
// child summaries. No model call — keeps the test hermetic.
type fakeSum struct{}

func (fakeSum) Summarize(_ context.Context, in agent.SummaryInput) (string, error) {
	if len(in.Children) > 0 {
		out := "rollup"
		for _, c := range in.Children {
			out += "|" + c.Summary
		}
		return out, nil
	}
	return "l0(" + string(in.Raw) + ")", nil
}

// distillReg registers a distillable "note" (title+body, scoped by site) plus
// the digest_node tree entity (domain.DigestNode — forgeext may import domain).
func distillReg(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "note", Kind: registry.KindAggregate, Model: (*dwNote)(nil),
		Distill: &registry.DistillSpec{SourceFields: []string{"title", "body"}, Scopes: []string{"site"}},
	})
	r.MustRegister(registry.EntitySpec{
		Name: agent.DigestEntity, Kind: registry.KindAggregate, Model: (*domain.DigestNode)(nil),
		GraphNode: "DigestNode",
	})
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestNextFireDelay(t *testing.T) {
	base := time.Unix(1000, 0)
	debounce := 100 * time.Millisecond
	cases := []struct {
		name     string
		now      time.Time
		deadline time.Time
		want     time.Duration
	}{
		{"debounce fits before deadline", base, base.Add(time.Second), debounce},
		{"deadline closer than debounce", base, base.Add(40 * time.Millisecond), 40 * time.Millisecond},
		{"deadline already passed clamps to zero", base, base.Add(-time.Second), 0},
		{"deadline exactly debounce away", base, base.Add(debounce), debounce},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := nextFireDelay(c.now, c.deadline, debounce); got != c.want {
				t.Fatalf("nextFireDelay = %v, want %v", got, c.want)
			}
		})
	}
}

func TestHasDistillableEntity(t *testing.T) {
	if !hasDistillableEntity(distillReg(t)) {
		t.Fatal("want true for a registry with a Distill entity")
	}
	plain := registry.New()
	plain.MustRegister(registry.EntitySpec{Name: "p", Kind: registry.KindAggregate, Model: (*dwNote)(nil)})
	_ = plain.Validate()
	if hasDistillableEntity(plain) {
		t.Fatal("want false for a registry with no Distill entity")
	}
}

func TestDistillSweeper_MarkSweepBuildsTree(t *testing.T) {
	reg := distillReg(t) // note (distillable) + digest_node
	w := fabriqtest.NewWorld(reg)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	d, err := agent.NewDistiller(fab, reg, stubEmb{}, fakeSum{}, nil, cas, agent.DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}
	sw := newDistillSweeper(d, 5*time.Millisecond, 50*time.Millisecond, nil)
	handle := distillHandler(context.Background(), sw)

	env := event.Envelope{TenantID: "acme", Aggregate: "note", AggID: "n1", Type: "note.created",
		Payload: json.RawMessage(`{"id":"n1","title":"Pump","body":"vibration","site_id":"s1"}`)}
	if err := handle("s-1", env); err != nil {
		t.Fatalf("handle: %v", err)
	}
	// Wait for the debounced sweep, then assert the L0 digest's VECTOR exists
	// (mirrors the embed worker test — no relational seeding needed).
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var matches []query.VectorMatch
		_ = w.Vector.Similar(ctx, query.VectorQuery{Entity: agent.DigestEntity, Embedding: []float32{1, 0, 0}, K: 50}, &matches)
		for _, m := range matches {
			if m.ID == agent.L0ID("note", "n1") {
				return // success: the debounced sweep built the L0 digest
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("debounced sweep did not build the L0 digest node")
}

// TestDistillSweeper_IncrementsMetrics asserts that a sweep driven through
// distillHandler increments fabriq_distill_nodes_total and
// fabriq_distill_summaries_total via the distillMetricsObserver.
func TestDistillSweeper_IncrementsMetrics(t *testing.T) {
	reg := distillReg(t) // note (distillable) + digest_node
	w := fabriqtest.NewWorld(reg)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	d, err := agent.NewDistiller(fab, reg, stubEmb{}, fakeSum{}, nil, cas, agent.DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}

	m, err := metrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}

	// Non-nil metrics wires the distillMetricsObserver into the Distiller.
	sw := newDistillSweeper(d, 5*time.Millisecond, 50*time.Millisecond, m)
	handle := distillHandler(context.Background(), sw)

	env := event.Envelope{
		TenantID:  "acme",
		Aggregate: "note",
		AggID:     "n1",
		Type:      "note.created",
		Payload:   json.RawMessage(`{"id":"n1","title":"Pump","body":"vibration","site_id":"s1"}`),
	}
	if err := handle("s-1", env); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// Poll for the debounced sweep (mirrors TestDistillSweeper_MarkSweepBuildsTree).
	// DistillNodesTotal > 0 proves NodeBuilt() was called, i.e. the observer fired.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(m.DistillNodesTotal) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := testutil.ToFloat64(m.DistillNodesTotal); got == 0 {
		t.Fatalf("DistillNodesTotal = 0 after sweep; observer not wired or sweep did not run")
	}
	if got := testutil.ToFloat64(m.DistillSummariesTotal); got == 0 {
		t.Fatalf("DistillSummariesTotal = 0 after sweep; Summarized() observer callback not fired")
	}

	// Verify the vector side-effect also happened (belt-and-suspenders: matches
	// the existing build test so we know it's the same sweep code path).
	ctx, _ := tenant.WithTenant(context.Background(), "acme")
	var matches []query.VectorMatch
	_ = w.Vector.Similar(ctx, query.VectorQuery{Entity: agent.DigestEntity, Embedding: []float32{1, 0, 0}, K: 50}, &matches)
	found := false
	for _, mv := range matches {
		if mv.ID == agent.L0ID("note", "n1") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("L0 digest node vector not found; sweep may have run but distillation failed silently")
	}
}

// TestDistillSweeper_ConcurrentMarksAreRaceFree exercises BOTH concurrency
// dimensions of distillSweeper under the -race detector:
//
//  1. Multi-tenant parallel: K tenants each driven by their own goroutines so
//     tenantGate.Do must let distinct tenants proceed concurrently.
//  2. Same-tenant coalescing + overlapping marks: within each tenant M events
//     are fired from multiple goroutines, mixing (a) repeated refs (latest-wins
//     coalescing) and (b) distinct refs, so MarkAndSchedule races against an
//     in-flight debounce timer on the same tenant.
//
// The test does NOT assert exact sweep counts (debounce timing is
// nondeterministic); it asserts the FINAL built state — for each tenant the
// expected L0 digest vectors must appear — and relies on -race to verify the
// worker's internal state management (mu-guarded dirty map + timers, tenantGate)
// is race-free.
func TestDistillSweeper_ConcurrentMarksAreRaceFree(t *testing.T) {
	const (
		K          = 4 // number of tenants
		goroutines = 3 // goroutines per tenant firing marks concurrently
	)

	// Distinct refs fired per tenant: n1 is repeated (coalescing target),
	// n2 and n3 are unique refs so each sweep must distill 3 L0 nodes.
	type mark struct {
		aggID   string
		payload string
	}
	// 8 events per tenant: n1 x6 (coalescing), n2 x1, n3 x1.
	tenantMarks := []mark{
		{"n1", `{"id":"n1","title":"Alpha","body":"first","site_id":"s1"}`},
		{"n2", `{"id":"n2","title":"Beta","body":"second","site_id":"s1"}`},
		{"n1", `{"id":"n1","title":"Alpha2","body":"updated","site_id":"s1"}`},
		{"n3", `{"id":"n3","title":"Gamma","body":"third","site_id":"s1"}`},
		{"n1", `{"id":"n1","title":"Alpha3","body":"again","site_id":"s1"}`},
		{"n2", `{"id":"n2","title":"Beta2","body":"second-b","site_id":"s1"}`},
		{"n1", `{"id":"n1","title":"Alpha4","body":"last","site_id":"s1"}`},
		{"n3", `{"id":"n3","title":"Gamma2","body":"third-b","site_id":"s1"}`},
	}

	reg := distillReg(t)
	w := fabriqtest.NewWorld(reg)
	fab := fabriqtest.NewFabric(w)
	cas := fabriqtest.NewFakeCAS()
	d, err := agent.NewDistiller(fab, reg, stubEmb{}, fakeSum{}, nil, cas, agent.DistillConfig{VectorDims: 3})
	if err != nil {
		t.Fatal(err)
	}

	// Small debounce so sweeps fire quickly; nil metrics (not under test here).
	sw := newDistillSweeper(d, 5*time.Millisecond, 50*time.Millisecond, nil)
	handle := distillHandler(context.Background(), sw)

	// Build tenant IDs up front.
	tenantIDs := make([]string, K)
	for i := range K {
		tenantIDs[i] = fmt.Sprintf("tenant-%d", i)
	}

	// Fire all marks: K tenants × goroutines goroutines per tenant × (len(tenantMarks)/goroutines) marks per goroutine.
	var wg sync.WaitGroup
	for _, tid := range tenantIDs {
		tid := tid
		// Distribute marks across goroutines so MarkAndSchedule calls for the
		// same tenant race against each other and against the debounce timer.
		for g := range goroutines {
			g := g
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i, mk := range tenantMarks {
					if i%goroutines != g {
						continue
					}
					env := event.Envelope{
						TenantID:  tid,
						Aggregate: "note",
						AggID:     mk.aggID,
						Type:      "note.updated",
						Payload:   json.RawMessage(mk.payload),
					}
					if err := handle("stream-"+tid, env); err != nil {
						t.Errorf("handle(%s): %v", tid, err)
					}
				}
			}()
		}
	}
	wg.Wait()

	// The expected distinct L0 IDs per tenant (n1, n2, n3).
	wantIDs := []string{
		agent.L0ID("note", "n1"),
		agent.L0ID("note", "n2"),
		agent.L0ID("note", "n3"),
	}

	// Poll until all tenants have all expected L0 digest vectors (max 3s).
	deadline := time.Now().Add(3 * time.Second)
	for _, tid := range tenantIDs {
		ctx, ctxErr := tenant.WithTenant(context.Background(), tid)
		if ctxErr != nil {
			t.Fatalf("tenant.WithTenant(%q): %v", tid, ctxErr)
		}
		for _, want := range wantIDs {
			found := false
			for !found && time.Now().Before(deadline) {
				var matches []query.VectorMatch
				_ = w.Vector.Similar(ctx, query.VectorQuery{
					Entity:    agent.DigestEntity,
					Embedding: []float32{1, 0, 0},
					K:         100,
				}, &matches)
				for _, m := range matches {
					if m.ID == want {
						found = true
						break
					}
				}
				if !found {
					time.Sleep(10 * time.Millisecond)
				}
			}
			if !found {
				t.Errorf("tenant %q: L0 digest vector %q not built within deadline", tid, want)
			}
		}
	}
}

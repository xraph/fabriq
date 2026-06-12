package query_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

type qAsset struct {
	grove.BaseModel `grove:"table:assets"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	Name     string `grove:"name,notnull"`
}

func qRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name: "asset", Kind: registry.KindAggregate, Model: (*qAsset)(nil), GraphNode: "Asset",
	})
	return r
}

// stubGraph returns canned traversal IDs.
type stubGraph struct {
	ids      []string
	lastCy   string
	lastArgs map[string]any
}

func (g *stubGraph) Query(_ context.Context, cypher string, params map[string]any, into any) error {
	g.lastCy, g.lastArgs = cypher, params
	dest, ok := into.(*[]string)
	if !ok {
		return fmt.Errorf("stub only scans *[]string, got %T", into)
	}
	*dest = append(*dest, g.ids...)
	return nil
}

func (g *stubGraph) TraverseAndHydrate(context.Context, string, map[string]any, any) error {
	return fmt.Errorf("not under test")
}

func (g *stubGraph) ApplyMutations(context.Context, string, []projection.Mutation) error {
	return fmt.Errorf("not under test")
}

// stubRelational records the batched hydration call.
type stubRelational struct {
	calls   int
	lastIDs []string
}

func (r *stubRelational) Get(context.Context, string, string, any) error { return nil }
func (r *stubRelational) List(context.Context, string, query.ListQuery, any) error {
	return nil
}
func (r *stubRelational) Query(context.Context, any, string, ...any) error { return nil }

func (r *stubRelational) GetMany(_ context.Context, _ string, ids []string, into any) error {
	r.calls++
	r.lastIDs = ids
	dest, ok := into.(*[]*qAsset)
	if !ok {
		return fmt.Errorf("unexpected hydration target %T", into)
	}
	for _, id := range ids {
		*dest = append(*dest, &qAsset{ID: id, Name: "asset-" + id})
	}
	return nil
}

func TestTraverseAndHydrate_OneBatchedQuery(t *testing.T) {
	g := &stubGraph{ids: []string{"A1", "A2", "A3"}}
	rel := &stubRelational{}

	var out []*qAsset
	err := query.TraverseAndHydrate(context.Background(), qRegistry(t), g, rel,
		"MATCH (a:Asset)-[:LOCATED_AT]->(:Site {id:$site}) RETURN a.id", map[string]any{"site": "S1"}, &out)
	if err != nil {
		t.Fatalf("TraverseAndHydrate: %v", err)
	}
	if rel.calls != 1 {
		t.Fatalf("hydration ran %d relational queries, must be exactly 1 (no N+1)", rel.calls)
	}
	if len(rel.lastIDs) != 3 || len(out) != 3 || out[0].ID != "A1" || out[2].Name != "asset-A3" {
		t.Fatalf("hydration mismatch: ids=%v out=%v", rel.lastIDs, out)
	}
}

func TestTraverseAndHydrate_EmptyTraversalSkipsHydration(t *testing.T) {
	g := &stubGraph{}
	rel := &stubRelational{}
	var out []*qAsset
	if err := query.TraverseAndHydrate(context.Background(), qRegistry(t), g, rel, "MATCH ...", nil, &out); err != nil {
		t.Fatal(err)
	}
	if rel.calls != 0 || len(out) != 0 {
		t.Fatalf("empty traversal must not hydrate: calls=%d out=%v", rel.calls, out)
	}
}

func TestTraverseAndHydrate_UnregisteredModelFails(t *testing.T) {
	type stranger struct{ ID string }
	var out []*stranger
	err := query.TraverseAndHydrate(context.Background(), qRegistry(t), &stubGraph{}, &stubRelational{}, "MATCH ...", nil, &out)
	if err == nil {
		t.Fatal("hydrating into an unregistered model must fail")
	}
}

func TestDeltaFromEnvelope(t *testing.T) {
	env := testEnvelope()
	d := query.DeltaFromEnvelope("changes:acme:id:A1", "1718000000000-0", env)
	if d.Channel != "changes:acme:id:A1" || d.StreamID != "1718000000000-0" ||
		d.TenantID != env.TenantID || d.Aggregate != env.Aggregate || d.AggID != env.AggID ||
		d.Version != env.Version || d.Type != env.Type || string(d.Payload) != string(env.Payload) {
		t.Fatalf("delta = %+v", d)
	}
}

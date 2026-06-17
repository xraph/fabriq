package query_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xraph/grove"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// repoModel is a grove-tagged struct used to register a self-referential
// entity (parent/child) for the walk tests.
type repoModel struct {
	grove.BaseModel `grove:"table:items"`

	ID       string `grove:"id,pk"`
	TenantID string `grove:"tenant_id,notnull"`
	Version  int64  `grove:"version,notnull"`
	ParentID string `grove:"parent_id"`
}

// repoRegistry returns a registry with a "item" entity that has a
// self-edge "CHILD_OF" (item → item), used to exercise walkCypher.
func repoRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	r := registry.New()
	r.MustRegister(registry.EntitySpec{
		Name:      "item",
		Kind:      registry.KindAggregate,
		Model:     (*repoModel)(nil),
		GraphNode: "Item",
		Edges: []registry.EdgeSpec{
			{Field: "parent_id", Rel: "CHILD_OF", Target: "item"},
		},
	})
	return r
}

// noopRelational is a minimal RelationalQuerier that does nothing —
// used in walkCypher tests where only the generated Cypher matters,
// not hydration.
type noopRelational struct{}

func (r *noopRelational) Get(context.Context, string, string, any) error           { return nil }
func (r *noopRelational) List(context.Context, string, query.ListQuery, any) error { return nil }
func (r *noopRelational) Query(context.Context, any, string, ...any) error         { return nil }
func (r *noopRelational) GetMany(_ context.Context, _ string, _ []string, _ any) error {
	return nil // skip hydration; we only care about the captured Cypher
}

// buildRepo wires up a Repo[repoModel] with a capturing stub graph and a
// no-op relational querier.
func buildRepo(t testing.TB, g query.GraphQuerier) *query.Repo[repoModel] {
	t.Helper()
	rel := &noopRelational{}
	reg := repoRegistry(t)
	repo, err := query.For[repoModel](reg, rel)
	if err != nil {
		t.Fatalf("query.For: %v", err)
	}
	repo = repo.WithGraph(g)
	return repo
}

// TestWalkCypher_Scoped asserts that Out/In/Reachable inject a scope
// predicate into the generated Cypher when the context carries a scope.
func TestWalkCypher_Scoped(t *testing.T) {
	ctx := tenant.MustWithTenant(context.Background(), "acme")
	ctx = tenant.MustWithScope(ctx, "proj_A")

	g := &stubGraph{}
	repo := buildRepo(t, g)

	if _, err := repo.Out(ctx, "root-id", "CHILD_OF"); err != nil {
		t.Fatalf("Out: %v", err)
	}
	cy := g.lastCy
	if !strings.Contains(cy, "scope_id") {
		t.Errorf("scoped Out: Cypher missing scope_id predicate; got: %s", cy)
	}
	if !strings.Contains(cy, "$scope") {
		t.Errorf("scoped Out: Cypher missing $scope param reference; got: %s", cy)
	}

	// Also test In direction.
	g2 := &stubGraph{}
	repo2 := buildRepo(t, g2)
	if _, err := repo2.In(ctx, "root-id", "CHILD_OF"); err != nil {
		t.Fatalf("In: %v", err)
	}
	cy2 := g2.lastCy
	if !strings.Contains(cy2, "scope_id") {
		t.Errorf("scoped In: Cypher missing scope_id predicate; got: %s", cy2)
	}
	if !strings.Contains(cy2, "$scope") {
		t.Errorf("scoped In: Cypher missing $scope param reference; got: %s", cy2)
	}

	// Variable-length (Reachable).
	g3 := &stubGraph{}
	repo3 := buildRepo(t, g3)
	if _, err := repo3.Reachable(ctx, "root-id", "CHILD_OF", 1, 3); err != nil {
		t.Fatalf("Reachable: %v", err)
	}
	cy3 := g3.lastCy
	if !strings.Contains(cy3, "scope_id") {
		t.Errorf("scoped Reachable: Cypher missing scope_id predicate; got: %s", cy3)
	}
	if !strings.Contains(cy3, "$scope") {
		t.Errorf("scoped Reachable: Cypher missing $scope param reference; got: %s", cy3)
	}
}

// TestWalkCypher_Unscoped asserts that Out/In/Reachable do NOT inject a
// scope predicate when the context carries no scope (sees all in tenant).
func TestWalkCypher_Unscoped(t *testing.T) {
	ctx := tenant.MustWithTenant(context.Background(), "acme")
	// No WithScope — unscoped context.

	g := &stubGraph{}
	repo := buildRepo(t, g)

	if _, err := repo.Out(ctx, "root-id", "CHILD_OF"); err != nil {
		t.Fatalf("Out: %v", err)
	}
	cy := g.lastCy
	if strings.Contains(cy, "scope_id") {
		t.Errorf("unscoped Out: Cypher must NOT contain scope_id predicate; got: %s", cy)
	}

	g2 := &stubGraph{}
	repo2 := buildRepo(t, g2)
	if _, err := repo2.In(ctx, "root-id", "CHILD_OF"); err != nil {
		t.Fatalf("In: %v", err)
	}
	if strings.Contains(g2.lastCy, "scope_id") {
		t.Errorf("unscoped In: Cypher must NOT contain scope_id predicate; got: %s", g2.lastCy)
	}

	g3 := &stubGraph{}
	repo3 := buildRepo(t, g3)
	if _, err := repo3.Reachable(ctx, "root-id", "CHILD_OF", 1, 3); err != nil {
		t.Fatalf("Reachable: %v", err)
	}
	if strings.Contains(g3.lastCy, "scope_id") {
		t.Errorf("unscoped Reachable: Cypher must NOT contain scope_id predicate; got: %s", g3.lastCy)
	}
}

// TestWalkCypher_ScopedCypherShape validates the full Cypher string shape
// for scoped Out and In, including WHERE placement before RETURN.
func TestWalkCypher_ScopedCypherShape(t *testing.T) {
	ctx := tenant.MustWithScope(tenant.MustWithTenant(context.Background(), "acme"), "proj_A")

	tests := []struct {
		name   string
		run    func(repo *query.Repo[repoModel]) error
		wantCy string
	}{
		{
			name: "Out",
			run: func(repo *query.Repo[repoModel]) error {
				_, err := repo.Out(ctx, "id1", "CHILD_OF")
				return err
			},
			wantCy: "MATCH (n:Item {id: $id})-[:CHILD_OF]->(m:Item) WHERE (m.scope_id IS NULL OR m.scope_id = $scope) RETURN m.id ORDER BY m.id",
		},
		{
			name: "In",
			run: func(repo *query.Repo[repoModel]) error {
				_, err := repo.In(ctx, "id1", "CHILD_OF")
				return err
			},
			wantCy: "MATCH (n:Item {id: $id})<-[:CHILD_OF]-(m:Item) WHERE (m.scope_id IS NULL OR m.scope_id = $scope) RETURN m.id ORDER BY m.id",
		},
		{
			name: "Reachable",
			run: func(repo *query.Repo[repoModel]) error {
				_, err := repo.Reachable(ctx, "id1", "CHILD_OF", 1, 5)
				return err
			},
			wantCy: "MATCH (n:Item {id: $id})-[:CHILD_OF*1..5]->(m:Item) WHERE (m.scope_id IS NULL OR m.scope_id = $scope) RETURN m.id ORDER BY m.id",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := &stubGraph{}
			repo := buildRepo(t, g)
			if err := tc.run(repo); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if g.lastCy != tc.wantCy {
				t.Errorf("%s: Cypher mismatch\n got:  %s\n want: %s", tc.name, g.lastCy, tc.wantCy)
			}
		})
	}
}

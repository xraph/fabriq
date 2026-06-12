package falkordb

import (
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/projection"
)

// The mutation translator is the ONLY place graph dialect lives. These
// tests pin the generated openCypher (common subset — no FalkorDB-specific
// functions) and the version gating predicate.

func TestCypherFor_NodeUpsert(t *testing.T) {
	cy, params, err := cypherFor(projection.NodeUpsert{
		Label: "Asset", ID: "A1",
		Props:   map[string]any{"name": "Pump", "version": int64(3)},
		Version: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"MERGE (n:Asset {id: $id})",
		"WHERE coalesce(n.version, 0) <= $version", // idempotency gate
		"SET n += $props",
	} {
		if !strings.Contains(cy, want) {
			t.Fatalf("cypher missing %q:\n%s", want, cy)
		}
	}
	if params["id"] != "A1" || params["version"] != int64(3) {
		t.Fatalf("params = %v", params)
	}
	props, ok := params["props"].(map[string]any)
	if !ok || props["name"] != "Pump" {
		t.Fatalf("props param = %v", params["props"])
	}
}

func TestCypherFor_EdgeUpsert_ReplacesPerRelSemantics(t *testing.T) {
	cy, params, err := cypherFor(projection.EdgeUpsert{
		Rel: "LOCATED_AT", FromLabel: "Asset", FromID: "A1",
		ToLabel: "Site", ToID: "S1", Version: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	// FK semantics: at most one outgoing LOCATED_AT per node — stale
	// targets are deleted before the merge.
	for _, want := range []string{
		"MATCH (from:Asset {id: $from_id})-[stale:LOCATED_AT]->(old)",
		"WHERE old.id <> $to_id",
		"DELETE stale",
		"MERGE (from:Asset {id: $from_id})",
		"MERGE (to:Site {id: $to_id})",
		"MERGE (from)-[r:LOCATED_AT]->(to)",
	} {
		if !strings.Contains(cy, want) {
			t.Fatalf("cypher missing %q:\n%s", want, cy)
		}
	}
	if params["from_id"] != "A1" || params["to_id"] != "S1" {
		t.Fatalf("params = %v", params)
	}
}

func TestCypherFor_NodeDelete_Detaches(t *testing.T) {
	cy, params, err := cypherFor(projection.NodeDelete{Label: "Asset", ID: "A1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cy, "MATCH (n:Asset {id: $id})") || !strings.Contains(cy, "DETACH DELETE n") {
		t.Fatalf("cypher = %s", cy)
	}
	if params["id"] != "A1" {
		t.Fatalf("params = %v", params)
	}
}

func TestCypherFor_EdgeDelete(t *testing.T) {
	cy, _, err := cypherFor(projection.EdgeDelete{Rel: "CHILD_OF", FromLabel: "Asset", FromID: "A1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cy, "MATCH (from:Asset {id: $from_id})-[r:CHILD_OF]->()") ||
		!strings.Contains(cy, "DELETE r") {
		t.Fatalf("cypher = %s", cy)
	}
}

func TestCypherFor_RejectsSearchMutations(t *testing.T) {
	if _, _, err := cypherFor(projection.DocIndex{}); err == nil {
		t.Fatal("search mutations must be rejected by the graph dialect")
	}
}

func TestCypherFor_RejectsInvalidIdentifiers(t *testing.T) {
	// Labels and rel types are interpolated into Cypher (parameters cannot
	// replace identifiers) — they must be strictly validated.
	_, _, err := cypherFor(projection.NodeUpsert{Label: "Asset) DETACH DELETE (m", ID: "A1", Version: 1})
	if err == nil {
		t.Fatal("malicious label must be rejected")
	}
	_, _, err = cypherFor(projection.EdgeUpsert{Rel: "X]->()<-[", FromLabel: "Asset", FromID: "A", ToLabel: "Site", ToID: "S", Version: 1})
	if err == nil {
		t.Fatal("malicious rel type must be rejected")
	}
}

func TestRouting_GraphNames(t *testing.T) {
	if g := graphForTenant("acme", 0); g != "tenant_acme" {
		t.Fatalf("live graph = %q", g)
	}
	if g := graphForTenant("acme", 5); g != "tenant_acme_v5" {
		t.Fatalf("versioned graph = %q", g)
	}
}

func BenchmarkCypherFor_NodeUpsert(b *testing.B) {
	mut := projection.NodeUpsert{
		Label: "Asset", ID: "A1",
		Props:   map[string]any{"name": "Pump", "version": int64(3), "site_id": "S1"},
		Version: 3,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := cypherFor(mut); err != nil {
			b.Fatal(err)
		}
	}
}

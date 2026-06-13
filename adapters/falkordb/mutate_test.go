package falkordb

import (
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/projection"
)

// The mutation translator is the ONLY place graph dialect lives. These
// tests pin the generated openCypher (common subset — no FalkorDB-specific
// functions) and the version gating that makes at-least-once replay safe.

func TestCypherFor_NodeUpsert(t *testing.T) {
	cy, params, err := cypherFor(projection.NodeUpsert{
		Label: "Asset", ID: "A1",
		Props:   map[string]any{"name": "Pump", "version": int64(3), "site_id": "S1"},
		Version: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"MERGE (n:Asset {id: $id})",
		"WHERE coalesce(n.version, 0) <= $version", // idempotency gate
		"SET n.name = $p_name, n.site_id = $p_site_id, n.version = $p_version",
	} {
		if !strings.Contains(cy, want) {
			t.Fatalf("cypher missing %q:\n%s", want, cy)
		}
	}
	if params["id"] != "A1" || params["version"] != int64(3) {
		t.Fatalf("params = %v", params)
	}
	if params["p_name"] != "Pump" || params["p_site_id"] != "S1" {
		t.Fatalf("prop params = %v", params)
	}
}

func TestCypherFor_EdgeUpsert_GatedAndReplacing(t *testing.T) {
	cy, params, err := cypherFor(projection.EdgeUpsert{
		Rel: "LOCATED_AT", FromLabel: "Asset", FromID: "A1",
		ToLabel: "Site", ToID: "S1", Version: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Gated on the FROM node's version so a stale replay cannot flip an
	// edge back; FK semantics: at most one outgoing edge per rel type —
	// stale targets are deleted after the merge.
	for _, want := range []string{
		"MATCH (from:Asset {id: $from_id})",
		"WHERE coalesce(from.version, 0) <= $version",
		"MERGE (to:Site {id: $to_id})",
		"MERGE (from)-[r:LOCATED_AT]->(to)",
		"SET r.version = $version",
		"OPTIONAL MATCH (from)-[stale:LOCATED_AT]->(old)",
		"WHERE old.id <> $to_id",
		"DELETE stale",
	} {
		if !strings.Contains(cy, want) {
			t.Fatalf("cypher missing %q:\n%s", want, cy)
		}
	}
	if params["from_id"] != "A1" || params["to_id"] != "S1" || params["version"] != int64(3) {
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

func TestCypherFor_EdgeDelete_Gated(t *testing.T) {
	cy, params, err := cypherFor(projection.EdgeDelete{
		Rel: "CHILD_OF", FromLabel: "Asset", FromID: "A1", Version: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"MATCH (from:Asset {id: $from_id})-[r:CHILD_OF]->()",
		"WHERE coalesce(from.version, 0) <= $version",
		"DELETE r",
	} {
		if !strings.Contains(cy, want) {
			t.Fatalf("cypher missing %q:\n%s", want, cy)
		}
	}
	if params["version"] != int64(4) {
		t.Fatalf("params = %v", params)
	}
}

func TestCypherFor_RejectsSearchMutations(t *testing.T) {
	if _, _, err := cypherFor(projection.DocIndex{}); err == nil {
		t.Fatal("search mutations must be rejected by the graph dialect")
	}
}

func TestCypherFor_RejectsInvalidIdentifiers(t *testing.T) {
	if _, _, err := cypherFor(projection.NodeUpsert{Label: "Asset) DETACH DELETE (m", ID: "A1", Version: 1}); err == nil {
		t.Fatal("malicious label must be rejected")
	}
	if _, _, err := cypherFor(projection.EdgeUpsert{Rel: "X]->()<-[", FromLabel: "Asset", FromID: "A", ToLabel: "Site", ToID: "S", Version: 1}); err == nil {
		t.Fatal("malicious rel type must be rejected")
	}
}

func TestCypherFor_SkipsInvalidPropKeys(t *testing.T) {
	cy, params, err := cypherFor(projection.NodeUpsert{
		Label: "Asset", ID: "A1",
		Props:   map[string]any{"name": "ok", "evil key": "x", "drop'); --": "y"},
		Version: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cy, "evil") || strings.Contains(cy, "drop") {
		t.Fatalf("invalid prop keys leaked into cypher:\n%s", cy)
	}
	if _, ok := params["p_name"]; !ok {
		t.Fatal("valid prop dropped")
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

// --- CYPHER param-prefix serialization --------------------------------------

func TestCypherParams_Literals(t *testing.T) {
	got, err := cypherParams(map[string]any{
		"s":    "it's a \\ test",
		"i":    int64(42),
		"f":    1.5,
		"b":    true,
		"null": nil,
		"arr":  []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`s='it\'s a \\ test'`,
		"i=42",
		"f=1.5",
		"b=true",
		"null=null",
		"arr=['a', 'b']",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("params %q missing %q", got, want)
		}
	}
	if !strings.HasPrefix(got, "CYPHER ") {
		t.Fatalf("params must start with CYPHER prefix: %q", got)
	}
}

func TestCypherParams_RejectsInvalidNames(t *testing.T) {
	if _, err := cypherParams(map[string]any{"bad name": 1}); err == nil {
		t.Fatal("invalid param name must be rejected")
	}
}

func TestCypherParams_EmptyIsEmpty(t *testing.T) {
	got, err := cypherParams(nil)
	if err != nil || got != "" {
		t.Fatalf("nil params = (%q, %v)", got, err)
	}
}

func TestCypherFor_NodeUpsertExtraLabels(t *testing.T) {
	cy, _, err := cypherFor(projection.NodeUpsert{
		Label: "Node", ExtraLabels: []string{"Dataset"}, ID: "n1",
		Props: map[string]any{"name": "x"}, Version: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cy, "MERGE (n:Node {id: $id})") {
		t.Fatalf("must MERGE on identity label only:\n%s", cy)
	}
	if !strings.Contains(cy, "n:Dataset") {
		t.Fatalf("must SET the extra label:\n%s", cy)
	}
	if strings.Index(cy, "WHERE coalesce(n.version, 0) <= $version") > strings.Index(cy, "n:Dataset") {
		t.Fatalf("extra label SET must appear after the version gate:\n%s", cy)
	}
}

func TestCypherFor_NodeUpsertRejectsBadExtraLabel(t *testing.T) {
	_, _, err := cypherFor(projection.NodeUpsert{Label: "Node", ExtraLabels: []string{"bad-label"}, ID: "n1"})
	if err == nil {
		t.Fatal("invalid extra label must error")
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

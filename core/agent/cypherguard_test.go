// core/agent/cypherguard_test.go
package agent

import "testing"

func TestReadOnlyCypher(t *testing.T) {
	ok := []string{
		"MATCH (n:Asset {id: $id}) RETURN n.id",
		"MATCH (a)-[:LOCATED_AT]->(s) WHERE a.version >= $v RETURN s.id ORDER BY s.id",
		"MATCH (n) OPTIONAL MATCH (n)-[:R]->(m) RETURN m WITH m UNWIND m AS x RETURN x",
	}
	for _, c := range ok {
		if err := readOnlyCypher(c); err != nil {
			t.Fatalf("read-only cypher rejected: %q: %v", c, err)
		}
	}
	bad := []string{
		"MATCH (n) DELETE n",
		"CREATE (n:Asset {id: 'x'})",
		"MATCH (n) SET n.x = 1",
		"MERGE (n:Asset {id: 'x'})",
		"MATCH (n) DETACH DELETE n",
		"MATCH (n) REMOVE n.x",
		"CALL db.createLabel('x')",
	}
	for _, c := range bad {
		if err := readOnlyCypher(c); err == nil {
			t.Fatalf("mutating cypher allowed: %q", c)
		}
	}
}

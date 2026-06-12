package falkordb

import (
	"fmt"
	"regexp"

	"github.com/xraph/fabriq/core/projection"
)

// This file is the ONLY place graph dialect lives. Appliers emit
// engine-neutral mutations; cypherFor translates them into the openCypher
// COMMON SUBSET (no FalkorDB-specific functions), which is what the
// adapters/graphtest conformance suite gates. Swapping in Memgraph, Neo4j
// or Kùzu means re-implementing this translation against the same suite.

// identPattern validates labels and relationship types. They are
// interpolated into Cypher text (identifiers cannot be parameterized), so
// they are restricted to safe characters; everything else is a parameter.
var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func validIdent(s string) bool { return identPattern.MatchString(s) }

// cypherFor renders one mutation as (cypher, params). Version gating is
// part of the dialect: a mutation older than the stored aggregate version
// must be a no-op.
func cypherFor(m projection.Mutation) (cypher string, params map[string]any, err error) {
	switch mut := m.(type) {
	case projection.NodeUpsert:
		if !validIdent(mut.Label) {
			return "", nil, fmt.Errorf("fabriq: invalid graph label %q", mut.Label)
		}
		cy := fmt.Sprintf(`MERGE (n:%s {id: $id})
WITH n
WHERE coalesce(n.version, 0) <= $version
SET n += $props`, mut.Label)
		return cy, map[string]any{"id": mut.ID, "version": mut.Version, "props": mut.Props}, nil

	case projection.EdgeUpsert:
		if !validIdent(mut.Rel) || !validIdent(mut.FromLabel) || !validIdent(mut.ToLabel) {
			return "", nil, fmt.Errorf("fabriq: invalid edge identifiers %q/%q/%q", mut.Rel, mut.FromLabel, mut.ToLabel)
		}
		// FK semantics: one outgoing edge per relationship type. Stale
		// targets are removed, then the current edge is merged and stamped.
		cy := fmt.Sprintf(`OPTIONAL MATCH (from:%[1]s {id: $from_id})-[stale:%[2]s]->(old)
WHERE old.id <> $to_id
DELETE stale
MERGE (from:%[1]s {id: $from_id})
MERGE (to:%[3]s {id: $to_id})
MERGE (from)-[r:%[2]s]->(to)
SET r.version = $version`, mut.FromLabel, mut.Rel, mut.ToLabel)
		return cy, map[string]any{"from_id": mut.FromID, "to_id": mut.ToID, "version": mut.Version}, nil

	case projection.NodeDelete:
		if !validIdent(mut.Label) {
			return "", nil, fmt.Errorf("fabriq: invalid graph label %q", mut.Label)
		}
		cy := fmt.Sprintf(`MATCH (n:%s {id: $id})
DETACH DELETE n`, mut.Label)
		return cy, map[string]any{"id": mut.ID}, nil

	case projection.EdgeDelete:
		if !validIdent(mut.Rel) || !validIdent(mut.FromLabel) {
			return "", nil, fmt.Errorf("fabriq: invalid edge identifiers %q/%q", mut.Rel, mut.FromLabel)
		}
		cy := fmt.Sprintf(`MATCH (from:%s {id: $from_id})-[r:%s]->()
DELETE r`, mut.FromLabel, mut.Rel)
		return cy, map[string]any{"from_id": mut.FromID}, nil

	default:
		return "", nil, fmt.Errorf("fabriq: mutation %T is not a graph mutation", m)
	}
}

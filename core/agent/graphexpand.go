// core/agent/graphexpand.go
package agent

import (
	"context"
	"fmt"
)

// expansionCypher builds the one-hop edge-expansion query. It stays within the
// confirmed openCypher common subset (property match + named relationship +
// labelled target). Implementation AND tests build the cypher through this
// helper so the FakeGraph's exact-string canning matches.
func expansionCypher(seedLabel, rel, targetLabel string) string {
	return fmt.Sprintf("MATCH (n:%s {id: $id})-[:%s]->(m:%s) RETURN m.id", seedLabel, rel, targetLabel)
}

// seedRefs collects the deduped union of vector then search refs in encounter
// order, capped at n (n <= 0 → no cap).
func seedRefs(channels map[string][]ref, n int) []ref {
	seen := map[ref]struct{}{}
	var out []ref
	for _, name := range []string{"vector", "search"} {
		for _, r := range channels[name] {
			if _, ok := seen[r]; ok {
				continue
			}
			seen[r] = struct{}{}
			out = append(out, r)
			if n > 0 && len(out) >= n {
				return out
			}
		}
	}
	return out
}

// graphChannel expands the top seed refs one hop along each seed entity's
// declared edges. The target entity name comes from EdgeSpec.Target, so
// neighbours hydrate without a label→entity reverse lookup. Only confirmed
// openCypher subset is emitted (via expansionCypher).
func (t *Toolkit) graphChannel(ctx context.Context, channels map[string][]ref, _ RecallRequest) ([]ref, []string, error) {
	seeds := seedRefs(channels, t.cfg.GraphSeeds)
	if len(seeds) == 0 {
		return nil, nil, nil
	}
	seen := map[ref]struct{}{}
	var refs []ref
	var warnings []string
	for _, seed := range seeds {
		ent, ok := t.reg.Get(seed.Entity)
		if !ok || ent.Spec.GraphNode == "" || len(ent.Spec.Edges) == 0 {
			continue
		}
		for _, edge := range ent.Spec.Edges {
			target, ok := t.reg.Get(edge.Target)
			if !ok || target.Spec.GraphNode == "" {
				continue
			}
			cypher := expansionCypher(ent.Spec.GraphNode, edge.Rel, target.Spec.GraphNode)
			var ids []string
			if err := t.fab.Graph().Query(ctx, cypher, map[string]any{"id": seed.ID}, &ids); err != nil {
				if t.cfg.Strict {
					return nil, nil, fmt.Errorf("agent: graph expand %s-[:%s]->%s: %w", ent.Spec.GraphNode, edge.Rel, target.Spec.GraphNode, err)
				}
				warnings = append(warnings, fmt.Sprintf("graph channel failed for %s-[:%s]: %v", seed.Entity, edge.Rel, err))
				continue
			}
			for _, id := range ids {
				r := ref{Entity: edge.Target, ID: id}
				if _, dup := seen[r]; dup {
					continue
				}
				seen[r] = struct{}{}
				refs = append(refs, r)
			}
		}
	}
	return refs, warnings, nil
}

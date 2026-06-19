// core/agent/graphexpand.go
package agent

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/registry"
)

// expansionCypher builds a one-or-multi-hop edge-expansion query in the
// confirmed openCypher common subset. hops<=1 → single hop; hops>1 →
// variable-length [:Rel*1..hops]. reverse → incoming edge (<-). Implementation
// AND tests build cypher through this helper so FakeGraph exact-string canning
// matches.
//
// NOTE: the combined reverse + variable-length form `<-[:R*1..N]-` (hops>1 &&
// reverse==true) is NOT yet exercised by the graph conformance suite
// (adapters/graphtest/suite.go). The suite only covers forward var-length and
// single-hop reverse separately. Add a reverse-varlen case to the suite before
// adopting a non-FalkorDB graph backend.
func expansionCypher(seedLabel, rel, targetLabel string, hops int, reverse bool) string {
	relPart := "[:" + rel + "]"
	if hops > 1 {
		relPart = fmt.Sprintf("[:%s*1..%d]", rel, hops)
	}
	if reverse {
		return fmt.Sprintf("MATCH (n:%s {id: $id})<-%s-(m:%s) RETURN m.id", seedLabel, relPart, targetLabel)
	}
	return fmt.Sprintf("MATCH (n:%s {id: $id})-%s->(m:%s) RETURN m.id", seedLabel, relPart, targetLabel)
}

// reverseEdge is an incoming edge: a Source entity points at the indexed target
// via Rel.
type reverseEdge struct {
	Source string
	Rel    string
}

// reverseEdgeIndex maps a target entity name → the edges other entities declare
// toward it.
func reverseEdgeIndex(reg *registry.Registry) map[string][]reverseEdge {
	idx := map[string][]reverseEdge{}
	for _, e := range reg.All() {
		for _, edge := range e.Spec.Edges {
			idx[edge.Target] = append(idx[edge.Target], reverseEdge{Source: e.Spec.Name, Rel: edge.Rel})
		}
	}
	return idx
}

// topSeeds returns the first n refs of an already-score-sorted fused slice
// (n<=0 → all).
func topSeeds(fused []scoredRef, n int) []ref {
	limit := len(fused)
	if n > 0 && n < limit {
		limit = n
	}
	out := make([]ref, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, fused[i].ref)
	}
	return out
}

// graphChannel expands the given seeds one hop (or Hops hops) along each seed
// entity's declared edges (forward) and, when cfg.GraphReverse is set, along
// edges that point at the seed entity (reverse). Neighbours are deduped.
func (t *Toolkit) graphChannel(ctx context.Context, seeds []ref, req RecallRequest) ([]ref, []string, error) {
	if len(seeds) == 0 {
		return nil, nil, nil
	}
	hops := req.Hops
	if hops <= 0 {
		hops = t.cfg.Hops
	}
	var revIdx map[string][]reverseEdge
	if t.cfg.GraphReverse {
		revIdx = t.revEdges
	}

	seen := map[ref]struct{}{}
	var refs []ref
	var warnings []string

	add := func(entity string, ids []string) {
		for _, id := range ids {
			nr := ref{Entity: entity, ID: id}
			if _, ok := seen[nr]; ok {
				continue
			}
			seen[nr] = struct{}{}
			refs = append(refs, nr)
		}
	}

	expand := func(seed ref, seedLabel, rel, targetLabel, targetEntity string, reverse bool) error {
		cypher := expansionCypher(seedLabel, rel, targetLabel, hops, reverse)
		var ids []string
		if err := t.fab.Graph().Query(ctx, cypher, map[string]any{"id": seed.ID}, &ids); err != nil {
			if t.cfg.Strict {
				dir := "->"
				if reverse {
					dir = "<-"
				}
				return fmt.Errorf("agent: graph expand %s-[:%s]%s%s: %w", seedLabel, rel, dir, targetLabel, err)
			}
			warnings = append(warnings, fmt.Sprintf("graph channel failed for %s-[:%s]: %v", seed.Entity, rel, err))
			return nil
		}
		add(targetEntity, ids)
		return nil
	}

	for _, seed := range seeds {
		ent, ok := t.reg.Get(seed.Entity)
		if !ok || ent.Spec.GraphNode == "" {
			continue
		}
		// forward: this entity's declared edges
		for _, edge := range ent.Spec.Edges {
			target, ok := t.reg.Get(edge.Target)
			if !ok || target.Spec.GraphNode == "" {
				continue
			}
			if err := expand(seed, ent.Spec.GraphNode, edge.Rel, target.Spec.GraphNode, edge.Target, false); err != nil {
				return nil, nil, err
			}
		}
		// reverse: edges pointing at this entity (opt-in)
		for _, re := range revIdx[seed.Entity] {
			src, ok := t.reg.Get(re.Source)
			if !ok || src.Spec.GraphNode == "" {
				continue
			}
			if err := expand(seed, ent.Spec.GraphNode, re.Rel, src.Spec.GraphNode, re.Source, true); err != nil {
				return nil, nil, err
			}
		}
	}
	return refs, warnings, nil
}

// core/agent/recall.go
package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/fabriq/core/query"
)

// RecallRequest is the input to the auto-context recall pipeline.
type RecallRequest struct {
	Query    string      `json:"query"`
	Budget   int         `json:"budget"`
	Entities []string    `json:"entities"`
	K        int         `json:"k"`
	Hops     int         `json:"hops"`
	Filters  query.Where `json:"filters,omitempty"`
}

// Recall runs the auto-context pipeline: embed → vector nearest-neighbour →
// RRF fuse → hydrate → token-budget pack. Phase 1a uses the vector channel
// only; search/graph channels arrive in Phase 1b without changing this shape.
func (t *Toolkit) Recall(ctx context.Context, req RecallRequest) (ContextPack, error) {
	if req.Query == "" {
		return ContextPack{}, fmt.Errorf("agent: recall requires a non-empty Query")
	}
	if req.Budget <= 0 {
		return ContextPack{}, fmt.Errorf("agent: recall requires a positive Budget")
	}
	if len(req.Entities) == 0 {
		return ContextPack{}, fmt.Errorf("agent: recall requires at least one entity")
	}
	k := req.K
	if k <= 0 {
		k = t.cfg.K
	}

	var warnings []string
	channels := map[string][]ref{}

	if t.emb == nil {
		warnings = append(warnings, "semantic channel skipped: no embedder configured")
	} else if vecs, err := t.emb.Embed(ctx, []string{req.Query}); err != nil {
		if t.cfg.Strict {
			return ContextPack{}, fmt.Errorf("agent: embed query: %w", err)
		}
		warnings = append(warnings, fmt.Sprintf("semantic channel failed: %v", err))
	} else if len(vecs) != 1 {
		msg := fmt.Sprintf("agent: embed returned %d vectors for 1 input", len(vecs))
		if t.cfg.Strict {
			return ContextPack{}, fmt.Errorf("%s", msg)
		}
		warnings = append(warnings, msg)
	} else {
		var vrefs []ref
		for _, ent := range req.Entities {
			var matches []query.VectorMatch
			if err := t.fab.Vector().Similar(ctx, query.VectorQuery{Entity: ent, Embedding: vecs[0], K: k}, &matches); err != nil {
				if t.cfg.Strict {
					return ContextPack{}, fmt.Errorf("agent: vector similar %q: %w", ent, err)
				}
				warnings = append(warnings, fmt.Sprintf("vector channel failed for %q: %v", ent, err))
				continue
			}
			for _, m := range matches {
				vrefs = append(vrefs, ref{Entity: ent, ID: m.ID})
			}
		}
		channels["vector"] = vrefs
	}

	fused := fuse(channels, t.cfg.ChannelWeights)

	// Batch-hydrate per entity.
	byEntity := map[string][]string{}
	for _, sr := range fused {
		byEntity[sr.Entity] = append(byEntity[sr.Entity], sr.ID)
	}
	rows := map[ref]json.RawMessage{}
	for ent, ids := range byEntity {
		hydrated, err := t.hydrate(ctx, ent, ids)
		if err != nil {
			if t.cfg.Strict {
				return ContextPack{}, fmt.Errorf("agent: hydrate %q: %w", ent, err)
			}
			warnings = append(warnings, fmt.Sprintf("hydrate failed for %q: %v", ent, err))
			continue
		}
		for id, raw := range hydrated {
			rows[ref{Entity: ent, ID: id}] = raw
		}
	}

	// Assemble in fused order, dropping rows that did not hydrate.
	items := make([]ContextItem, 0, len(fused))
	for _, sr := range fused {
		raw, ok := rows[sr.ref]
		if !ok {
			continue
		}
		items = append(items, ContextItem{
			Entity: sr.Entity,
			ID:     sr.ID,
			Row:    raw,
			Score:  sr.score,
			Source: sr.sources,
			Tokens: t.cfg.Tokenizer(raw),
		})
	}

	kept, omitted, used := pack(items, req.Budget)
	return ContextPack{Items: kept, Omitted: omitted, Tokens: used, Warnings: warnings}, nil
}

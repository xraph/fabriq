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

// Recall runs the auto-context pipeline: build per-channel ranked candidates
// (vector, search; graph added in Task 2) → RRF fuse → hydrate authoritative
// rows → token-budget pack.
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

	vrefs, vw, err := t.vectorChannel(ctx, req, k)
	if err != nil {
		return ContextPack{}, err
	}
	warnings = append(warnings, vw...)
	if vrefs != nil {
		channels["vector"] = vrefs
	}

	srefs, sw, err := t.searchChannel(ctx, req, k)
	if err != nil {
		return ContextPack{}, err
	}
	warnings = append(warnings, sw...)
	if srefs != nil {
		channels["search"] = srefs
	}

	grefs, gw, err := t.graphChannel(ctx, channels, req)
	if err != nil {
		return ContextPack{}, err
	}
	warnings = append(warnings, gw...)
	if grefs != nil {
		channels["graph"] = grefs
	}

	fused := fuse(channels, t.cfg.ChannelWeights)

	// Hydrate every fused ref from the relational source of truth, batched per entity.
	byEntity := map[string][]string{}
	for _, sr := range fused {
		byEntity[sr.Entity] = append(byEntity[sr.Entity], sr.ID)
	}
	rows := map[ref]json.RawMessage{}
	for ent, ids := range byEntity {
		hydrated, herr := t.hydrate(ctx, ent, ids)
		if herr != nil {
			if t.cfg.Strict {
				return ContextPack{}, fmt.Errorf("agent: hydrate %q: %w", ent, herr)
			}
			warnings = append(warnings, fmt.Sprintf("hydrate failed for %q: %v", ent, herr))
			continue
		}
		for id, raw := range hydrated {
			rows[ref{Entity: ent, ID: id}] = raw
		}
	}

	// Assemble in fused order, dropping refs that did not hydrate.
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

// vectorChannel embeds the query and runs nearest-neighbour per entity. Returns
// ranked refs, warnings (lenient mode), or an error (strict mode).
func (t *Toolkit) vectorChannel(ctx context.Context, req RecallRequest, k int) ([]ref, []string, error) {
	if t.emb == nil {
		return nil, []string{"semantic channel skipped: no embedder configured"}, nil
	}
	vecs, err := t.emb.Embed(ctx, []string{req.Query})
	if err != nil {
		if t.cfg.Strict {
			return nil, nil, fmt.Errorf("agent: embed query: %w", err)
		}
		return nil, []string{fmt.Sprintf("semantic channel failed: %v", err)}, nil
	}
	if len(vecs) != 1 {
		msg := fmt.Sprintf("agent: embed returned %d vectors for 1 input", len(vecs))
		if t.cfg.Strict {
			return nil, nil, fmt.Errorf("%s", msg)
		}
		return nil, []string{msg}, nil
	}
	var refs []ref
	var warnings []string
	for _, ent := range req.Entities {
		var matches []query.VectorMatch
		if err := t.fab.Vector().Similar(ctx, query.VectorQuery{Entity: ent, Embedding: vecs[0], K: k}, &matches); err != nil {
			if t.cfg.Strict {
				return nil, nil, fmt.Errorf("agent: vector similar %q: %w", ent, err)
			}
			warnings = append(warnings, fmt.Sprintf("vector channel failed for %q: %v", ent, err))
			continue
		}
		for _, m := range matches {
			refs = append(refs, ref{Entity: ent, ID: m.ID})
		}
	}
	return refs, warnings, nil
}

// searchChannel runs full-text search per searchable entity. Non-searchable
// entities (no declared search index) are skipped silently.
func (t *Toolkit) searchChannel(ctx context.Context, req RecallRequest, k int) ([]ref, []string, error) {
	var refs []ref
	var warnings []string
	for _, ent := range req.Entities {
		e, ok := t.reg.Get(ent)
		if !ok || e.Spec.Search.Index == "" {
			continue
		}
		var hits []map[string]any
		if err := t.fab.Search().Search(ctx, query.SearchQuery{Entity: ent, Query: req.Query, Limit: k}, &hits); err != nil {
			if t.cfg.Strict {
				return nil, nil, fmt.Errorf("agent: search %q: %w", ent, err)
			}
			warnings = append(warnings, fmt.Sprintf("search channel failed for %q: %v", ent, err))
			continue
		}
		for _, h := range hits {
			id, _ := h["id"].(string)
			if id == "" {
				continue
			}
			refs = append(refs, ref{Entity: ent, ID: id})
		}
	}
	return refs, warnings, nil
}

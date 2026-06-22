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
	// Altitude selects which layer of the distillation tree surfaces. AltAuto
	// (the zero value) defers to Config.Altitude, which itself defaults to
	// AltAuto: the budget then decides between entities and the tenant digest.
	Altitude Altitude `json:"altitude,omitempty"`
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

	// Pre-fuse the discovery channels to pick the highest-ranked seeds to expand.
	seeds := topSeeds(fuse(channels, t.cfg.ChannelWeights), t.cfg.GraphSeeds)

	grefs, gw, err := t.graphChannel(ctx, seeds, req)
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

	// Cluster-coverage support: if digests are present and any is a cluster, load
	// each entity item's L0-digest SemHash so a cluster digest can prune it.
	// Scope-only recalls (kind="scope") use rowHasValue, not Bucket, so they
	// must NOT trigger the per-entity L0-SemHash lookup — check the kind
	// explicitly to avoid K wasted Gets when only scope digests are present.
	if _, ok := t.reg.Get(DigestEntity); ok {
		hasCluster := false
		for _, it := range items {
			if isDigest(it.Entity) && digestKind(it.Row) == KindClusterNode {
				hasCluster = true
				break
			}
		}
		if hasCluster {
			for i := range items {
				if isDigest(items[i].Entity) {
					continue
				}
				if row, found, err := t.getDigestRow(ctx, L0ID(items[i].Entity, items[i].ID)); err == nil && found {
					items[i].Bucket = parseSemOrZero(row.SemHash)
					items[i].BucketSet = true
				}
			}
		}
	}

	// Altitude resolution: collapse the digest tree to a single layer before
	// packing so a digest and the entities it covers never both surface. AltAuto
	// lets the budget decide — descend to entities when their tokens fit, else
	// climb to the tenant digest. dedupeByAltitude is a no-op when no digest
	// items are present, so registries without digest_node are unaffected.
	alt := req.Altitude
	if alt == AltAuto {
		alt = t.cfg.Altitude
	}
	entityTokens, backboneTokens := 0, 0
	for _, it := range items {
		switch {
		case !isDigest(it.Entity):
			entityTokens += it.Tokens
		case digestLevel(it.Row) == LevelScope:
			backboneTokens += it.Tokens
		}
	}
	items = dedupeByAltitude(items, resolveAltitude(alt, entityTokens, backboneTokens, req.Budget))

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
	// Probe the caller's entities, plus digest_node when it is registered, so
	// distillation digests become recall candidates without the caller having to
	// name them. This is additive and leaves req.Entities untouched: registries
	// without digest_node behave exactly as before.
	entities := req.Entities
	if _, ok := t.reg.Get(DigestEntity); ok {
		entities = append(append([]string(nil), req.Entities...), DigestEntity)
	}

	var refs []ref
	var warnings []string
	for _, ent := range entities {
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

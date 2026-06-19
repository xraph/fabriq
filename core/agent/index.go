// core/agent/index.go
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// Indexer embeds entity rows and upserts their vectors. It is the write-side
// counterpart to recall. Construct it once and call IndexEvent from the host's
// event consumer (see Reindex for backfill).
type Indexer struct {
	fab query.Fabric
	reg *registry.Registry
	emb Embedder
}

// NewIndexer builds an Indexer. The Embedder is required (indexing without a
// model is meaningless).
func NewIndexer(fab query.Fabric, reg *registry.Registry, emb Embedder) (*Indexer, error) {
	if fab == nil || reg == nil {
		return nil, fmt.Errorf("agent: indexer requires Fabric and Registry")
	}
	if emb == nil {
		return nil, fmt.Errorf("agent: indexer requires an Embedder")
	}
	return &Indexer{fab: fab, reg: reg, emb: emb}, nil
}

func (ix *Indexer) embedSpec(entity string) *registry.EmbedSpec {
	e, ok := ix.reg.Get(entity)
	if !ok {
		return nil
	}
	return e.Spec.Embed
}

// embedTextFor builds the text to embed from a row's column values.
func embedTextFor(spec *registry.EmbedSpec, vals map[string]any) string {
	if spec.Text != nil {
		return spec.Text(vals)
	}
	parts := make([]string, 0, len(spec.Fields))
	for _, f := range spec.Fields {
		if v, ok := vals[f]; ok && v != nil {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
	}
	return strings.Join(parts, " ")
}

// IndexRow embeds the row's text (per its EmbedSpec) and upserts the vector.
// No-op for non-embeddable entities or empty text.
func (ix *Indexer) IndexRow(ctx context.Context, entity, id string, vals map[string]any) error {
	spec := ix.embedSpec(entity)
	if spec == nil {
		return nil
	}
	if id == "" {
		return fmt.Errorf("agent: index %q: empty id", entity)
	}
	text := embedTextFor(spec, vals)
	if strings.TrimSpace(text) == "" {
		return nil
	}
	vecs, err := ix.emb.Embed(ctx, []string{text})
	if err != nil {
		return fmt.Errorf("agent: embed %q/%s: %w", entity, id, err)
	}
	if len(vecs) != 1 {
		return fmt.Errorf("agent: embed %q/%s: got %d vectors for 1 input", entity, id, len(vecs))
	}
	return ix.fab.Vector().Upsert(ctx, entity, id, vecs[0], nil)
}

// IndexEvent indexes a create/update event whose aggregate is embeddable.
// Deletes (".deleted" type) and empty payloads are skipped — the vector port
// has no delete operation in v1.
func (ix *Indexer) IndexEvent(ctx context.Context, env event.Envelope) error {
	if ix.embedSpec(env.Aggregate) == nil {
		return nil
	}
	if strings.HasSuffix(env.Type, ".deleted") || len(env.Payload) == 0 {
		return nil
	}
	var vals map[string]any
	if err := json.Unmarshal(env.Payload, &vals); err != nil {
		return fmt.Errorf("agent: index event %s/%s: payload: %w", env.Aggregate, env.AggID, err)
	}
	if len(vals) == 0 {
		return nil
	}
	return ix.IndexRow(ctx, env.Aggregate, env.AggID, vals)
}

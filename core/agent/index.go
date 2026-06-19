// core/agent/index.go
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// ErrUnindexablePayload is returned by IndexEvent when the event payload cannot
// be unmarshalled into a map. Such events are structurally poison and will never
// succeed on retry; callers should ack-skip them rather than leaving them in the
// pending-entry list (PEL).
var ErrUnindexablePayload = errors.New("agent: unindexable event payload")

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

// reindexBatch is the page size used by Reindex. It is a package-level var
// (not const) so tests can reduce it to exercise the multi-page loop.
var reindexBatch = 200

// Reindex re-embeds every row of an embeddable entity (backfill). Returns the
// number of rows indexed. No-op (0) for non-embeddable entities.
//
// Reindex is tenant-scoped: it backfills only the tenant present in ctx. Call
// once per tenant for a full cluster-wide backfill.
//
// NOTE: Reindex embeds one row per Embed call (the Embedder is batch-capable;
// a batched backfill is a future optimisation for the forgeext worker). Rows
// whose "id" field is empty or absent are silently skipped.
func (ix *Indexer) Reindex(ctx context.Context, entity string) (int, error) {
	if ix.embedSpec(entity) == nil {
		return 0, nil
	}
	ent, ok := ix.reg.Get(entity)
	if !ok {
		return 0, fmt.Errorf("agent: reindex: unknown entity %q", entity)
	}
	batch := reindexBatch
	indexed := 0
	for offset := 0; ; offset += batch {
		rows, err := ix.listVals(ctx, ent, batch, offset)
		if err != nil {
			return indexed, fmt.Errorf("agent: reindex %q: %w", entity, err)
		}
		for _, vals := range rows {
			id, _ := vals["id"].(string)
			if id == "" {
				continue
			}
			if err := ix.IndexRow(ctx, entity, id, vals); err != nil {
				return indexed, err
			}
			indexed++
		}
		if len(rows) < batch {
			return indexed, nil
		}
	}
}

// listVals lists one page of an entity's rows as column-keyed maps, handling
// both typed (Go-model) and dynamic entities — the same split as hydrate.
func (ix *Indexer) listVals(ctx context.Context, ent *registry.Entity, limit, offset int) ([]map[string]any, error) {
	q := query.ListQuery{Limit: limit, Offset: offset}
	if ent.Binding.IsDynamic() {
		var maps []map[string]any
		if err := ix.fab.Relational().List(ctx, ent.Spec.Name, q, &maps); err != nil {
			return nil, err
		}
		return maps, nil
	}
	mt := ent.Binding.ModelType()
	slicePtr := reflect.New(reflect.SliceOf(mt))
	if err := ix.fab.Relational().List(ctx, ent.Spec.Name, q, slicePtr.Interface()); err != nil {
		return nil, err
	}
	slice := slicePtr.Elem()
	out := make([]map[string]any, 0, slice.Len())
	for i := 0; i < slice.Len(); i++ {
		vals, err := ent.Binding.ValuesByColumn(slice.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	return out, nil
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
		return fmt.Errorf("%w: %v", ErrUnindexablePayload, err)
	}
	if len(vals) == 0 {
		return nil
	}
	return ix.IndexRow(ctx, env.Aggregate, env.AggID, vals)
}

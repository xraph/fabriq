package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/fabriq/core/projection"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

func tenantFrom(ctx context.Context) (string, error) { return tenant.Require(ctx) }

// ApplyMutations implements the search projection write path: one _bulk
// request per batch, every op carrying the aggregate version with
// version_type=external_gte. Stale replays come back as version conflicts
// and count as success — that IS the idempotency gate working.
//
// target "" routes to the tenant's live versioned indexes (alias kept
// pointing at them); target "vN" routes to a rebuild's building indexes.
func (a *Adapter) ApplyMutations(ctx context.Context, target string, muts []projection.Mutation) error {
	if len(muts) == 0 {
		return nil
	}
	tenantID, err := tenantFrom(ctx)
	if err != nil {
		return err
	}
	version, err := targetVersion(target)
	if err != nil {
		return err
	}
	live := version == 0
	if live {
		if version, err = a.modelVersion(ctx, tenantID); err != nil {
			return err
		}
	}

	var body bytes.Buffer
	for _, m := range muts {
		switch mut := m.(type) {
		case projection.DocIndex:
			index := registry.SearchIndexVersioned(tenantID, mut.Index, version)
			if err := a.ensureIndex(ctx, index); err != nil {
				return err
			}
			if live {
				if err := a.ensureAlias(ctx, registry.SearchIndexAlias(tenantID, mut.Index), index); err != nil {
					return err
				}
			}
			meta := fmt.Sprintf(`{"index":{"_index":%q,"_id":%q,"version":%d,"version_type":"external_gte"}}`,
				index, mut.ID, mut.Version)
			doc, err := json.Marshal(mut.Doc)
			if err != nil {
				return err
			}
			body.WriteString(meta)
			body.WriteByte('\n')
			body.Write(doc)
			body.WriteByte('\n')

		case projection.DocDeindex:
			index := registry.SearchIndexVersioned(tenantID, mut.Index, version)
			meta := fmt.Sprintf(`{"delete":{"_index":%q,"_id":%q,"version":%d,"version_type":"external_gte"}}`,
				index, mut.ID, mut.Version)
			body.WriteString(meta)
			body.WriteByte('\n')

		default:
			return fmt.Errorf("fabriq: non-search mutation %T sent to the search port", m)
		}
	}

	res, err := a.es.Bulk(bytes.NewReader(body.Bytes()),
		a.es.Bulk.WithContext(ctx), a.es.Bulk.WithRefresh("true"))
	if err != nil {
		return fmt.Errorf("fabriq: bulk: %w", err)
	}
	defer drainAndClose(res.Body)
	if res.IsError() {
		return fmt.Errorf("fabriq: bulk: %s", res.String())
	}

	var parsed struct {
		Errors bool                        `json:"errors"`
		Items  []map[string]map[string]any `json:"items"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return fmt.Errorf("fabriq: bulk decode: %w", err)
	}
	if !parsed.Errors {
		return nil
	}
	for _, item := range parsed.Items {
		for _, op := range item {
			status, _ := op["status"].(float64)
			if status >= 400 && !isVersionConflict(op) && status != 404 {
				return fmt.Errorf("fabriq: bulk item failed: %v", op)
			}
		}
	}
	return nil // only gates fired (conflicts) or deletes of absent docs
}

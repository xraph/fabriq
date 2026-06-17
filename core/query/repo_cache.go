package query

import (
	"context"
	"encoding/json"

	"github.com/xraph/fabriq/core/cache"
)

// cachedHydrate caches an ordered id-list under fpKey in the repo's result-set
// keyspace, then hydrates it through GetMany (the warm, P3a-decorated path).
// Called only when r.cache != nil. The id-list — not the rows — is what's
// cached; rows come from the per-id row cache, completing the two-level design.
// Tasks 2-4 wire call sites; the helper is scaffolded here so the keyspace
// and fingerprint logic are reviewable independently of the method changes.
//
//nolint:unused // scaffolded for Tasks 2-4 which wire the call sites (List, Traverse, Search, Similar)
func (r *Repo[T]) cachedHydrate(ctx context.Context, fpKey any,
	idLoader func(context.Context) ([]string, error)) ([]*T, error) {
	fp, err := cache.Fingerprint(fpKey)
	if err != nil {
		return nil, err
	}
	raw, err := r.cache.GetOrLoad(ctx, r.qks, fp, func(ctx context.Context) ([]byte, error) {
		ids, lerr := idLoader(ctx)
		if lerr != nil {
			return nil, lerr
		}
		return json.Marshal(ids)
	})
	if err != nil {
		return nil, err
	}
	var ids []string
	if err := json.Unmarshal(raw, &ids); err != nil {
		return nil, err
	}
	return r.GetMany(ctx, ids)
}

// extractIDs pulls the json:"id" field out of each row (grove models tag id).
// Rows without an id are skipped.
func extractIDs[T any](rows []*T) ([]string, error) {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		b, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		var h struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(b, &h); err != nil {
			return nil, err
		}
		if h.ID != "" {
			ids = append(ids, h.ID)
		}
	}
	return ids, nil
}

package search

import (
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// AggregateVersions reads id -> version for one entity's documents from
// the tenant's alias, the reconciler's projection side. Capped at 10k
// documents per entity for v1 (the match_all page limit); larger tenants
// reconcile via counts first and need a scroll, which is noted in
// OPERATIONS.md.
func (a *Adapter) AggregateVersions(ctx context.Context, tenantID string, ent *registry.Entity) (map[string]int64, error) {
	if ent.Spec.Search.Index == "" {
		return nil, nil
	}
	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	_ = tctx

	alias := registry.SearchIndexAlias(tenantID, ent.Spec.Search.Index)
	body := `{"size":10000,"_source":["id","version"],"query":{"match_all":{}}}`
	res, err := a.request(ctx, "POST", "/"+alias+"/_search", "", strings.NewReader(body))
	if err != nil {
		return nil, a.transportError("scan "+alias, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, res.Body); _ = res.Body.Close() }()
	if res.StatusCode == 404 {
		return map[string]int64{}, nil // tenant never indexed this entity
	}
	if res.StatusCode >= 300 {
		return nil, a.responseError("scan "+alias, res.StatusCode, responseBody(res))
	}

	var parsed struct {
		Hits struct {
			Hits []struct {
				Source struct {
					ID      string  `json:"id"`
					Version float64 `json:"version"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(parsed.Hits.Hits))
	for _, h := range parsed.Hits.Hits {
		if h.Source.ID != "" {
			out[h.Source.ID] = int64(h.Source.Version)
		}
	}
	return out, nil
}

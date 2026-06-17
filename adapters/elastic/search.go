package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// Search implements query.SearchQuerier: a multi_match over the entity's
// DECLARED search fields against the tenant's alias, optionally narrowed by
// a structured Filter (non-scoring bool filter context), ordered by Sort
// and paginated by Offset. An absent alias (no document ever indexed for
// this tenant) is an empty result, not an error.
func (a *Adapter) Search(ctx context.Context, q query.SearchQuery, into any) error {
	tenantID, err := tenantFrom(ctx)
	if err != nil {
		return err
	}
	ent, ok := a.reg.Get(q.Entity)
	if !ok || ent.Spec.Search.Index == "" {
		return fmt.Errorf("fabriq: entity %q is not searchable", q.Entity)
	}
	dest, ok := into.(*[]map[string]any)
	if !ok {
		return fmt.Errorf("fabriq: Search scans into *[]map[string]any, got %T", into)
	}
	if verr := query.ValidateSearchQuery(q, ent.Spec.Search.Fields); verr != nil {
		return verr
	}
	size := q.Limit
	if size <= 0 {
		size = 25
	}

	// must: the full-text component (match_all when no text, so a
	// filter-only search still works); filter: the non-scoring narrowing.
	var must map[string]any
	if q.Query != "" {
		must = map[string]any{"multi_match": map[string]any{"query": q.Query, "fields": ent.Spec.Search.Fields}}
	} else {
		must = map[string]any{"match_all": map[string]any{}}
	}
	boolQuery := map[string]any{"must": []any{must}}
	var filters []any
	if len(q.Filter) > 0 {
		clauses, ferr := esFilterClauses(q.Filter)
		if ferr != nil {
			return ferr
		}
		filters = append(filters, clauses...)
	}
	// Soft secondary-scope filter: a scoped read sees docs whose scope_id
	// matches OR is missing (shared); an unscoped read sees everything in the
	// tenant (tenant isolation is enforced by the per-tenant index alias).
	if scope, ok := tenant.ScopeFromContext(ctx); ok {
		filters = append(filters, map[string]any{"bool": map[string]any{
			"should": []any{
				map[string]any{"term": map[string]any{esExactField(registry.ColumnScope, true): scope}},
				map[string]any{"bool": map[string]any{"must_not": map[string]any{"exists": map[string]any{"field": registry.ColumnScope}}}},
			},
			"minimum_should_match": 1,
		}})
	}
	if len(filters) > 0 {
		boolQuery["filter"] = filters
	}

	req := map[string]any{
		"size":  size,
		"query": map[string]any{"bool": boolQuery},
	}
	if q.Offset > 0 {
		req["from"] = q.Offset
	}
	if q.Sort != "" {
		col, desc := query.SortField(q.Sort)
		order := "asc"
		if desc {
			order = "desc"
		}
		req["sort"] = []any{map[string]any{esSortField(col): map[string]any{"order": order}}}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	alias := registry.SearchIndexAlias(tenantID, ent.Spec.Search.Index)
	res, err := a.es.Search(
		a.es.Search.WithContext(ctx),
		a.es.Search.WithIndex(alias),
		a.es.Search.WithBody(strings.NewReader(string(body))),
	)
	if err != nil {
		return fmt.Errorf("fabriq: search %s: %w", alias, err)
	}
	defer drainAndClose(res.Body)
	if res.StatusCode == 404 {
		return nil // tenant has no index yet: nothing matches
	}
	if res.IsError() {
		return fmt.Errorf("fabriq: search %s: %s", alias, res.String())
	}

	var parsed struct {
		Hits struct {
			Hits []struct {
				Source map[string]any `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return fmt.Errorf("fabriq: search decode: %w", err)
	}
	for _, h := range parsed.Hits.Hits {
		*dest = append(*dest, h.Source)
	}
	return nil
}

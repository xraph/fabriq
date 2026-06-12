package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/core/registry"
)

// Search implements query.SearchQuerier: a multi_match over the entity's
// DECLARED search fields against the tenant's alias. An absent alias (no
// document ever indexed for this tenant) is an empty result, not an error.
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
	size := q.Limit
	if size <= 0 {
		size = 25
	}

	req := map[string]any{
		"size": size,
		"query": map[string]any{
			"multi_match": map[string]any{
				"query":  q.Query,
				"fields": ent.Spec.Search.Fields,
			},
		},
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

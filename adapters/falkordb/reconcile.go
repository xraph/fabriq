package falkordb

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// AggregateVersions reads id -> version for one entity's nodes from the
// tenant's live graph — the reconciler's projection side
// (projection.ProjectedVersions).
func (a *Adapter) AggregateVersions(ctx context.Context, tenantID string, ent *registry.Entity) (map[string]int64, error) {
	if ent.Spec.GraphNode == "" {
		return nil, nil
	}
	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	cypher := fmt.Sprintf(`MATCH (n:%s) RETURN n.id AS id, n.version AS version`, ent.Spec.GraphNode)
	if !validIdent(ent.Spec.GraphNode) {
		return nil, fmt.Errorf("fabriq: invalid graph label %q", ent.Spec.GraphNode)
	}
	if err := a.Query(tctx, cypher, nil, &rows); err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, r := range rows {
		id, _ := r["id"].(string)
		if id == "" {
			continue
		}
		switch v := r["version"].(type) {
		case int64:
			out[id] = v
		case float64:
			out[id] = int64(v)
		}
	}
	return out, nil
}
